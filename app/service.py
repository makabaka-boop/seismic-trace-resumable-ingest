"""Upload protocol state machine.

Rules implemented here (see README for the full protocol):

* A session is created with an immutable ``total_bytes`` + ``whole_sha256``.
* Chunks are binary, described by ``start`` offset, declared ``length`` and
  the chunk's own SHA-256.  A new chunk must begin exactly at
  ``confirmed_offset`` and must not cross ``total_bytes``.
* Retransmission of any range already inside ``[0, confirmed_offset)`` whose
  bytes are byte-identical to the archived bytes succeeds idempotently;
  every other stale offset is answered with the current expected offset.
* When ``confirmed_offset == total_bytes`` the package is reassembled from
  persisted chunks and its whole SHA-256 is recomputed.  A match seals the
  session (immutable); a mismatch puts it into the terminal ``failed`` state
  which can never be resumed — a new session with correct metadata is needed.
"""
from __future__ import annotations

import hashlib
import uuid

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .errors import UploadError
from .models import (
    ACTIVE,
    AUDIT_COMPACTED,
    AUDIT_SEALED,
    FAILED,
    SEALED,
    AuditEvent,
    Chunk,
    UploadSession,
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


# --------------------------------------------------------------------------- #
# Session creation / lookup
# --------------------------------------------------------------------------- #
def create_session(db: Session, total_bytes: int, whole_sha256: str) -> UploadSession:
    session = UploadSession(
        id=str(uuid.uuid4()),
        total_bytes=total_bytes,
        whole_sha256=whole_sha256,
        confirmed_offset=0,
        status=ACTIVE,
    )
    db.add(session)

    # A zero-length package is complete the moment it is registered:
    # seal it when its metadata is correct, fail it terminally otherwise.
    if total_bytes == 0:
        if whole_sha256 == EMPTY_SHA256:
            session.status = SEALED
            session.confirmed_offset = 0
            session.computed_sha256 = EMPTY_SHA256
            # Born-sealed: the first audit snapshot is part of the same
            # transaction that creates the session.
            _add_audit(
                db,
                session,
                AUDIT_SEALED,
                total_bytes=0,
                whole_sha256=EMPTY_SHA256,
            )
        else:
            session.status = FAILED
            session.computed_sha256 = EMPTY_SHA256
            session.failure_reason = (
                "zero-length package metadata inconsistent: "
                "whole_sha256 does not match SHA-256 of empty payload"
            )
            db.commit()
            raise UploadError(
                422,
                "whole_digest_mismatch",
                "session failed terminally: declared whole_sha256 does not match "
                "the only possible digest of a 0-byte package; create a new session",
                digest=whole_sha256,
                expected_digest=EMPTY_SHA256,
            )

    db.commit()
    db.refresh(session)
    return session


def _get_locked_session(db: Session, session_id: str) -> UploadSession:
    """Fetch a session with a row lock, serialising chunk PUTs per session."""
    session = db.get(
        UploadSession,
        session_id,
        with_for_update={"nowait": False},
    )
    if session is None:
        raise UploadError(
            404, "session_not_found", f"unknown session id: {session_id}"
        )
    return session


# --------------------------------------------------------------------------- #
# Chunk handling
# --------------------------------------------------------------------------- #
def _chunks_in_range(
    db: Session, session_id: str, begin: int, end: int
) -> list[Chunk]:
    """Return stored chunks overlapping [begin, end), ordered by offset."""
    stmt = (
        select(Chunk)
        .where(
            Chunk.session_id == session_id,
            Chunk.start_offset < end,
            Chunk.end_offset > begin,
        )
        .order_by(Chunk.start_offset)
    )
    return list(db.execute(stmt).scalars().all())


def _archived_bytes(
    db: Session, session_id: str, begin: int, end: int
) -> bytes | None:
    """Concatenate archived chunk bytes for [begin, end).

    Returns ``None`` if the archived chunks do not cover the range without
    gaps (with our append-only layout this can only happen if the caller asks
    for bytes beyond ``confirmed_offset``).
    """
    if begin == end:
        return b""
    chunks = _chunks_in_range(db, session_id, begin, end)
    out = bytearray()
    cursor = begin
    for chunk in chunks:
        if chunk.start_offset > cursor:
            return None  # gap
        overlap_end = min(chunk.end_offset, end)
        rel_start = cursor - chunk.start_offset
        out.extend(chunk.data[rel_start : overlap_end - chunk.start_offset])
        cursor = overlap_end
        if cursor >= end:
            break
    return bytes(out) if cursor == end else None


def _stale_offset_error(session: UploadSession, start: int) -> UploadError:
    return UploadError(
        409,
        "stale_offset",
        (
            f"stale or non-contiguous chunk at offset {start}: the next chunk "
            f"must start at confirmed_offset {session.confirmed_offset}"
        ),
        offset=start,
        expected_offset=session.confirmed_offset,
    )


def _terminal_replay(
    session: UploadSession,
    start: int,
    length: int,
    chunk_digest: str,
    payload: bytes,
    db: Session,
) -> dict:
    """Idempotency rules that still apply once a session is sealed/failed."""
    end = start + length
    # Only a full retransmission of an already archived range may be echoed.
    if (
        0 <= start < end <= session.confirmed_offset
        and session.confirmed_offset == session.total_bytes
    ):
        archived = _archived_bytes(db, session.id, start, end)
        if archived is not None and archived == payload:
            return _ack(
                session, start, length, chunk_digest, idempotent_replay=True
            )
    if session.status == FAILED:
        raise UploadError(
            409,
            "session_failed_terminal",
            (
                "session is in the terminal 'failed' state because the whole "
                "package digest did not match; it cannot be resumed — create a "
                "new session with correct total_bytes/whole_sha256 metadata"
            ),
            expected_offset=session.confirmed_offset,
            expected_digest=session.whole_sha256,
            details={"computed_sha256": session.computed_sha256},
        )
    # sealed + different content / bad offset
    if start + length <= session.confirmed_offset:
        raise UploadError(
            409,
            "sealed_content_conflict",
            (
                f"sealed archive is immutable: bytes retransmitted at offset "
                f"{start} differ from the sealed content"
            ),
            offset=start,
            expected_offset=session.total_bytes,
        )
    raise UploadError(
        409,
        "session_sealed",
        f"session is already sealed at offset {session.total_bytes}",
        offset=start,
        expected_offset=session.total_bytes,
    )


# ---- HTTP-facing ack -------------------------------------------------------
def _ack(
    session: UploadSession,
    start: int,
    length: int,
    chunk_digest: str,
    *,
    idempotent_replay: bool,
) -> dict:
    return {
        "id": session.id,
        "status": session.status,
        "start_offset": start,
        "length": length,
        "chunk_sha256": chunk_digest,
        "confirmed_offset": session.confirmed_offset,
        "expected_offset": session.confirmed_offset
        if session.status == ACTIVE
        else session.total_bytes,
        "total_bytes": session.total_bytes,
        "idempotent_replay": idempotent_replay,
    }


def _next_audit_sequence(db: Session, session_id: str) -> int:
    """Next per-session ordinal; callers already hold the session row lock."""
    current = db.execute(
        select(func.coalesce(func.max(AuditEvent.sequence), 0)).where(
            AuditEvent.session_id == session_id
        )
    ).scalar_one()
    return int(current) + 1


def _add_audit(
    db: Session,
    session: UploadSession,
    event: str,
    *,
    total_bytes: int,
    whole_sha256: str,
    target_chunk_bytes: int | None = None,
    chunks_before: int | None = None,
    chunks_after: int | None = None,
) -> AuditEvent:
    """Append an audit snapshot inside the caller's open transaction.

    Nothing is committed here: when the surrounding seal/compaction
    transaction rolls back, the audit row disappears with it, so a failed
    operation can never leave an orphan record.
    """
    row = AuditEvent(
        session_id=session.id,
        sequence=_next_audit_sequence(db, session.id),
        event=event,
        total_bytes=total_bytes,
        whole_sha256=whole_sha256,
        target_chunk_bytes=target_chunk_bytes,
        chunks_before=chunks_before,
        chunks_after=chunks_after,
    )
    db.add(row)
    try:
        db.flush()
    except Exception:
        # The audit write shares the seal/compaction transaction: signal an
        # integrity failure so the caller's except clause rolls the whole
        # transaction back (layout change + audit row) atomically.
        db.rollback()
        raise UploadError(
            500,
            "audit_write_failed",
            "failed to persist the archive audit record; transaction rolled back",
        )
    return row


def _verify_and_finalize(db: Session, session: UploadSession) -> None:
    """Recompute the whole-package hash once every byte has arrived."""
    total = session.total_bytes
    chunks = list(
        db.execute(
            select(Chunk)
            .where(Chunk.session_id == session.id)
            .order_by(Chunk.start_offset)
        ).scalars().all()
    )

    hasher = hashlib.sha256()
    cursor = 0
    for chunk in chunks:
        if chunk.start_offset != cursor:
            # Persisted bytes are discontinuous: the package cannot match.
            computed = None
            break
        hasher.update(chunk.data)
        cursor = chunk.end_offset
    else:
        computed = hasher.hexdigest() if cursor == total else None

    if computed is not None and computed == session.whole_sha256:
        session.status = SEALED
        session.confirmed_offset = total
        session.computed_sha256 = computed
        session.failure_reason = None
        # First audit snapshot, in the same transaction as the sealing PUT.
        # The digest above was just recomputed from persisted bytes: reuse it
        # instead of reading the chunks a second time.
        _add_audit(
            db,
            session,
            AUDIT_SEALED,
            total_bytes=total,
            whole_sha256=computed,
        )
        return

    session.status = FAILED
    session.computed_sha256 = computed
    session.failure_reason = (
        "whole package SHA-256 mismatch after all bytes were received"
        if computed is not None
        else "stored chunks are discontinuous or incomplete at finalization"
    )


def put_chunk(
    db: Session,
    session_id: str,
    start: int,
    declared_length: int,
    chunk_digest: str,
    payload: bytes,
) -> dict:
    session = _get_locked_session(db, session_id)
    end = start + declared_length

    # ---- structural validation (errors always cite offset / digest) -------
    if start < 0:
        raise UploadError(
            422, "invalid_offset", "start offset must be >= 0", offset=start
        )
    if declared_length < 0:
        raise UploadError(
            422,
            "invalid_length",
            "declared length must be >= 0",
            offset=start,
            details={"length": declared_length},
        )
    if len(payload) != declared_length:
        raise UploadError(
            422,
            "length_mismatch",
            (
                f"declared length {declared_length} does not match the "
                f"{len(payload)} received body bytes"
            ),
            offset=start,
            details={"declared_length": declared_length, "actual_length": len(payload)},
        )
    actual_digest = hashlib.sha256(payload).hexdigest()
    if actual_digest != chunk_digest:
        raise UploadError(
            400,
            "chunk_digest_mismatch",
            (
                f"chunk at offset {start} fails its own SHA-256 check: "
                f"declared {chunk_digest}, recomputed {actual_digest}"
            ),
            offset=start,
            digest=chunk_digest,
            expected_digest=actual_digest,
        )
    if end > session.total_bytes:
        raise UploadError(
            416,
            "chunk_beyond_total",
            (
                f"chunk [{start}, {end}) crosses total_bytes "
                f"{session.total_bytes}"
            ),
            offset=start,
            expected_offset=session.confirmed_offset,
            details={"end": end, "total_bytes": session.total_bytes},
        )

    # ---- terminal states only honour byte-identical replays --------------
    if session.status in (SEALED, FAILED):
        ack = _terminal_replay(
            session, start, declared_length, chunk_digest, payload, db
        )
        db.commit()
        return ack

    # ---- active session ---------------------------------------------------
    confirmed = session.confirmed_offset

    if start == confirmed and end > confirmed:
        # The expected forward chunk.
        chunk = Chunk(
            session_id=session.id,
            start_offset=start,
            end_offset=end,
            length=declared_length,
            sha256=chunk_digest,
            data=payload,
        )
        db.add(chunk)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            # Concurrent duplicate insert: fall back to idempotent semantics.
            session = _get_locked_session(db, session_id)
            archived = _archived_bytes(db, session_id, start, end)
            if archived == payload:
                return _ack(session, start, declared_length, chunk_digest,
                            idempotent_replay=True)
            raise _stale_offset_error(session, start)

        session.confirmed_offset = end
        finalized = False
        if end == session.total_bytes:
            _verify_and_finalize(db, session)
            finalized = True
        db.commit()
        db.refresh(session)
        ack = _ack(session, start, declared_length, chunk_digest,
                   idempotent_replay=False)
        if finalized and session.status == FAILED:
            # Surface the terminal failure at the upload that caused it.
            raise UploadError(
                422,
                "whole_digest_mismatch",
                (
                    "all bytes received but recomputed whole package SHA-256 "
                    f"{session.computed_sha256} does not match declared "
                    f"{session.whole_sha256}; session is terminally failed, "
                    "create a new session with correct metadata"
                ),
                offset=session.total_bytes,
                digest=session.whole_sha256,
                expected_digest=session.computed_sha256,
            )
        return ack

    if start == confirmed and end == confirmed:
        # Zero-length chunk at the frontier: a harmless idempotent no-op
        # (empty payload already passed its digest check).
        db.commit()
        return _ack(session, start, 0, chunk_digest, idempotent_replay=True)

    # ---- stale offsets ----------------------------------------------------
    if end <= confirmed:
        # Fully inside the confirmed range: succeed only when bytes are
        # identical to what is already archived, regardless of how the
        # retransmission is re-chunked.
        archived = _archived_bytes(db, session_id, start, end)
        if archived is not None and archived == payload:
            db.commit()
            return _ack(
                session, start, declared_length, chunk_digest,
                idempotent_replay=True,
            )
        raise _stale_offset_error(session, start)

    # Overlaps the frontier but doesn't start at it, or starts ahead of it.
    raise _stale_offset_error(session, start)


def sealed_payload(db: Session, session: UploadSession) -> bytes:
    """Materialise the archived package; only used for the download route."""
    return _archived_bytes(db, session.id, 0, session.total_bytes) or b""


# --------------------------------------------------------------------------- #
# Archive audit trail
# --------------------------------------------------------------------------- #
def get_audit_trail(db: Session, session_id: str) -> list[dict]:
    """Return a session's audit snapshots ordered by event sequence.

    Read-only: it neither changes the session lifecycle nor writes anything.
    Unknown sessions raise the same ``session_not_found`` error every other
    route uses; active/failed sessions and sessions sealed before the audit
    feature existed simply have an empty trail.
    """
    session = db.get(UploadSession, session_id)
    if session is None:
        raise UploadError(
            404, "session_not_found", f"unknown session id: {session_id}"
        )
    rows = db.execute(
        select(AuditEvent)
        .where(AuditEvent.session_id == session_id)
        .order_by(AuditEvent.sequence)
    ).scalars().all()
    return [_audit_to_dict(row) for row in rows]


def _audit_to_dict(row: AuditEvent) -> dict:
    return {
        "sequence": row.sequence,
        "event": row.event,
        "occurred_at": row.occurred_at,
        "total_bytes": row.total_bytes,
        "whole_sha256": row.whole_sha256,
        "target_chunk_bytes": row.target_chunk_bytes,
        "chunks_before": row.chunks_before,
        "chunks_after": row.chunks_after,
    }


# --------------------------------------------------------------------------- #
# Sealed archive compaction
# --------------------------------------------------------------------------- #
def _planned_spans(total: int, target_chunk_bytes: int) -> list[tuple[int, int]]:
    """Deterministic target layout: contiguous spans of <= target size.

    Splitting purely on (total, target) makes repeated compaction with the
    same target byte-for-byte identical in layout and chunk digests.
    """
    return [
        (off, min(off + target_chunk_bytes, total))
        for off in range(0, total, target_chunk_bytes)
    ]


def compact_sealed(
    db: Session, session_id: str, target_chunk_bytes: int
) -> dict:
    """Rewrite a sealed package's chunks into <= target_chunk_bytes spans.

    Runs under the session row lock inside a single transaction.  Existing
    bytes are read in offset order, re-chunked and re-hashed; the total
    length and whole-package digest are re-verified before commit.  Any
    verification failure rolls the whole rewrite back.
    """
    # Reject non-integer targets outright instead of coercing them; bool is
    # an int subclass in Python, so it must be excluded explicitly (a bare
    # ``True`` would otherwise execute as a 1-byte target).
    if isinstance(target_chunk_bytes, bool) or not isinstance(
        target_chunk_bytes, int
    ):
        raise UploadError(
            422,
            "invalid_target_chunk_bytes",
            "target_chunk_bytes must be a positive integer",
            details={"target_chunk_bytes": target_chunk_bytes},
        )
    if target_chunk_bytes <= 0:
        raise UploadError(
            422,
            "invalid_target_chunk_bytes",
            "target_chunk_bytes must be a positive integer",
            details={"target_chunk_bytes": target_chunk_bytes},
        )

    session = _get_locked_session(db, session_id)
    if session.status != SEALED:
        raise UploadError(
            409,
            "compaction_state_conflict",
            (
                "only sealed sessions can be compacted "
                f"(state={session.status})"
            ),
            expected_offset=session.confirmed_offset,
            details={"status": session.status},
        )

    total = session.total_bytes
    try:
        old_chunks = list(
            db.execute(
                select(Chunk)
                .where(Chunk.session_id == session_id)
                .order_by(Chunk.start_offset)
            ).scalars().all()
        )

        # Read the archived bytes in offset order while simultaneously
        # verifying continuity, length and the whole-package digest.
        hasher = hashlib.sha256()
        buffer = bytearray()
        cursor = 0
        for chunk in old_chunks:
            if chunk.start_offset != cursor:
                raise UploadError(
                    500,
                    "compaction_integrity_error",
                    (
                        "stored chunks are discontinuous at offset "
                        f"{cursor}; refusing to compact"
                    ),
                    expected_offset=cursor,
                )
            buffer.extend(chunk.data)
            hasher.update(chunk.data)
            cursor = chunk.end_offset

        if cursor != total or len(buffer) != total:
            raise UploadError(
                500,
                "compaction_integrity_error",
                (
                    f"archived length {cursor} does not match registered "
                    f"total_bytes {total}; refusing to compact"
                ),
                offset=cursor,
                expected_offset=total,
            )
        whole_digest = hasher.hexdigest()
        if whole_digest != session.whole_sha256:
            raise UploadError(
                500,
                "compaction_integrity_error",
                (
                    "archived bytes recompute to a different whole digest; "
                    "refusing to compact"
                ),
                digest=session.whole_sha256,
                expected_digest=whole_digest,
            )

        chunks_before = len(old_chunks)
        total_bytes_before = sum(c.length for c in old_chunks)

        plan = _planned_spans(total, target_chunk_bytes)

        # Idempotent no-op: the persisted layout already equals the target
        # layout, so leave rows (and their digests) untouched.
        old_layout = [
            (c.start_offset, c.end_offset, c.length, c.sha256) for c in old_chunks
        ]
        new_layout = [
            (
                off,
                end,
                end - off,
                hashlib.sha256(bytes(buffer[off:end])).hexdigest(),
            )
            for off, end in plan
        ]
        if old_layout == new_layout:
            # Nothing to rewrite: still record that the layout was verified
            # against this target (a successful compaction), then end the
            # locked transaction cleanly.
            _add_audit(
                db,
                session,
                AUDIT_COMPACTED,
                total_bytes=total,
                whole_sha256=whole_digest,
                target_chunk_bytes=target_chunk_bytes,
                chunks_before=chunks_before,
                chunks_after=len(new_layout),
            )
            db.commit()
            return _compact_result(
                session, target_chunk_bytes, chunks_before, len(new_layout),
                total_bytes_before, total, whole_digest, new_layout,
            )

        # Single-commit rewrite: delete old rows, insert the new spans.
        for chunk in old_chunks:
            db.delete(chunk)
        db.flush()
        new_rows: list[Chunk] = []
        for off, end, length, digest in new_layout:
            row = Chunk(
                session_id=session_id,
                start_offset=off,
                end_offset=end,
                length=length,
                sha256=digest,
                data=bytes(buffer[off:end]),
            )
            db.add(row)
            new_rows.append(row)
        db.flush()

        # Re-verify the freshly persisted layout before committing.
        verify_chunks = list(
            db.execute(
                select(Chunk)
                .where(Chunk.session_id == session_id)
                .order_by(Chunk.start_offset)
            ).scalars().all()
        )
        verifier = hashlib.sha256()
        verify_cursor = 0
        total_bytes_after = 0
        for chunk in verify_chunks:
            if chunk.start_offset != verify_cursor:
                raise UploadError(
                    500,
                    "compaction_integrity_error",
                    "new layout is discontinuous; rolling back",
                    expected_offset=verify_cursor,
                )
            if chunk.length > target_chunk_bytes:
                raise UploadError(
                    500,
                    "compaction_integrity_error",
                    (
                        f"new chunk at offset {chunk.start_offset} exceeds "
                        f"target size {target_chunk_bytes}"
                    ),
                    offset=chunk.start_offset,
                    details={"length": chunk.length},
                )
            if hashlib.sha256(chunk.data).hexdigest() != chunk.sha256:
                raise UploadError(
                    500,
                    "compaction_integrity_error",
                    f"new chunk at offset {chunk.start_offset} fails its digest",
                    offset=chunk.start_offset,
                    expected_digest=chunk.sha256,
                )
            verifier.update(chunk.data)
            verify_cursor = chunk.end_offset
            total_bytes_after += chunk.length

        if verify_cursor != total:
            raise UploadError(
                500,
                "compaction_integrity_error",
                (
                    f"new layout length {verify_cursor} does not match "
                    f"total_bytes {total}"
                ),
                offset=verify_cursor,
                expected_offset=total,
            )
        recomputed_whole = verifier.hexdigest()
        if recomputed_whole != session.whole_sha256:
            raise UploadError(
                500,
                "compaction_integrity_error",
                "new layout fails whole-digest verification; rolling back",
                digest=session.whole_sha256,
                expected_digest=recomputed_whole,
            )

        # All statistics come from the verification just performed; no chunk
        # bytes are re-read to build the snapshot.
        _add_audit(
            db,
            session,
            AUDIT_COMPACTED,
            total_bytes=total,
            whole_sha256=recomputed_whole,
            target_chunk_bytes=target_chunk_bytes,
            chunks_before=chunks_before,
            chunks_after=len(verify_chunks),
        )

        db.commit()
        return _compact_result(
            session, target_chunk_bytes, chunks_before, len(verify_chunks),
            total_bytes_before, total_bytes_after, recomputed_whole,
            [
                (c.start_offset, c.end_offset, c.length, c.sha256)
                for c in verify_chunks
            ],
        )
    except UploadError:
        db.rollback()
        raise
    except Exception:
        db.rollback()
        raise


def _compact_result(
    session: UploadSession,
    target_chunk_bytes: int,
    chunks_before: int,
    chunks_after: int,
    total_bytes_before: int,
    total_bytes_after: int,
    whole_sha256: str,
    layout: list[tuple[int, int, int, str]],
) -> dict:
    return {
        "id": session.id,
        "status": SEALED,
        "target_chunk_bytes": target_chunk_bytes,
        "chunks_before": chunks_before,
        "chunks_after": chunks_after,
        "total_bytes_before": total_bytes_before,
        "total_bytes_after": total_bytes_after,
        "whole_sha256": whole_sha256,
        "chunks": [
            {
                "start_offset": off,
                "end_offset": end,
                "length": length,
                "sha256": digest,
            }
            for off, end, length, digest in layout
        ],
    }
