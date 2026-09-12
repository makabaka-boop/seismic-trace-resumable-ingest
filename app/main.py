"""HTTP API for resumable seismograph record uploads."""
from __future__ import annotations

import hashlib
import re

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from . import service
from .config import MAX_CHUNK_BYTES
from .database import get_db, init_db
from .errors import UploadError
from .models import SEALED, Chunk, UploadSession
from .schemas import ChunkAck, SessionCreate, SessionResponse

SHA256_QUERY_RE = re.compile(r"^[0-9a-fA-F]{64}$")

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Resumable Seismic Record Upload",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Register a record package (total bytes + whole SHA-256), upload it as "
        "hash-checked binary chunks over an unstable link, and obtain an "
        "immutable sealed archive only when length and digest both verify."
    ),
)


# --------------------------------------------------------------------------- #
# Error handlers — every error locates an offset and/or a digest
# --------------------------------------------------------------------------- #
@app.exception_handler(UploadError)
async def _upload_error_handler(_: Request, exc: UploadError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.to_body())


@app.exception_handler(RequestValidationError)
async def _validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    # Map each field-level failure to the offending parameter (offset/digest/
    # length/body) so callers never receive an anonymous 422.
    locations: list[dict[str, str]] = []
    for err in exc.errors():
        loc = list(err.get("loc", []))
        name = ".".join(str(p) for p in loc if p not in ("query", "body", "header"))
        if name:
            where = "offset" if name in ("start", "length") else (
                "digest" if "sha256" in name else name
            )
            locations.append({"field": where})
    body: dict = {
        "error": {
            "code": "validation_error",
            "message": "request parameters failed validation",
            "details": {
                "fields": locations,
                "validation": jsonable_encoder(exc.errors()),
            },
        }
    }
    return JSONResponse(status_code=422, content=body)


# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #
@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict:
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Sessions
# --------------------------------------------------------------------------- #
def _session_to_response(session: UploadSession) -> SessionResponse:
    return SessionResponse(
        id=session.id,
        total_bytes=session.total_bytes,
        whole_sha256=session.whole_sha256,
        confirmed_offset=session.confirmed_offset,
        status=session.status,
        computed_sha256=session.computed_sha256,
        failure_reason=session.failure_reason,
    )


@app.post("/sessions", response_model=SessionResponse, status_code=201)
def create_session(
    payload: SessionCreate, db: Session = Depends(get_db)
) -> SessionResponse:
    session = service.create_session(
        db, total_bytes=payload.total_bytes, whole_sha256=payload.whole_sha256
    )
    return _session_to_response(session)


@app.get("/sessions", response_model=list[SessionResponse])
def list_sessions(
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
) -> list[SessionResponse]:
    if status is not None and status not in ("active", "sealed", "failed"):
        raise UploadError(
            422,
            "invalid_status",
            "status filter must be one of active, sealed, failed",
        )
    stmt = select(UploadSession).order_by(UploadSession.created_at.desc())
    if status:
        stmt = stmt.where(UploadSession.status == status)
    stmt = stmt.limit(min(limit, 1000)).offset(max(offset, 0))
    rows = db.execute(stmt).scalars().all()
    return [_session_to_response(s) for s in rows]


@app.get("/sessions/{session_id}", response_model=SessionResponse)
def get_session(session_id: str, db: Session = Depends(get_db)) -> SessionResponse:
    session = db.get(UploadSession, session_id)
    if session is None:
        raise UploadError(
            404, "session_not_found", f"unknown session id: {session_id}"
        )
    return _session_to_response(session)


@app.get("/sessions/{session_id}/chunks")
def list_chunks(session_id: str, db: Session = Depends(get_db)) -> dict:
    """Inspect persisted chunk checkpoints (survives API restarts)."""
    session = db.get(UploadSession, session_id)
    if session is None:
        raise UploadError(
            404, "session_not_found", f"unknown session id: {session_id}"
        )
    rows = db.execute(
        select(Chunk)
        .where(Chunk.session_id == session_id)
        .order_by(Chunk.start_offset)
    ).scalars().all()
    return {
        "id": session_id,
        "status": session.status,
        "confirmed_offset": session.confirmed_offset,
        "total_bytes": session.total_bytes,
        "chunks": [
            {
                "start_offset": r.start_offset,
                "end_offset": r.end_offset,
                "length": r.length,
                "sha256": r.sha256,
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------- #
# Chunk upload — raw binary body, metadata in the query string
# --------------------------------------------------------------------------- #
async def _chunk_payload(request: Request, length: int) -> bytes:
    """Async dependency: stream the raw body without trusting Content-Length.

    Aborts as soon as the byte count exceeds the declared length or the
    server-wide chunk ceiling.
    """
    payload = bytearray()
    async for block in request.stream():
        payload.extend(block)
        # Only the server-wide ceiling aborts the stream here; a mismatch with
        # the *declared* length is reported downstream as length_mismatch so
        # the error can cite both numbers.
        if len(payload) > MAX_CHUNK_BYTES:
            raise UploadError(
                413,
                "chunk_too_large",
                f"received body exceeds server chunk limit {MAX_CHUNK_BYTES}",
                details={"received": len(payload), "limit": MAX_CHUNK_BYTES},
            )
    return bytes(payload)


@app.put("/sessions/{session_id}/chunks", response_model=ChunkAck)
def upload_chunk(
    session_id: str,
    start: int,
    length: int,
    sha256: str,
    payload: bytes = Depends(_chunk_payload),
    db: Session = Depends(get_db),
) -> ChunkAck:
    if not SHA256_QUERY_RE.match(sha256):
        raise UploadError(
            422,
            "invalid_digest",
            "sha256 query parameter must be 64 hexadecimal characters",
            offset=start,
            digest=sha256,
        )
    if start < 0:
        raise UploadError(
            422, "invalid_offset", "start offset must be >= 0", offset=start
        )
    if length < 0:
        raise UploadError(
            422,
            "invalid_length",
            "length must be >= 0",
            offset=start,
            details={"length": length},
        )
    if length > MAX_CHUNK_BYTES:
        raise UploadError(
            413,
            "chunk_too_large",
            f"declared length {length} exceeds server limit {MAX_CHUNK_BYTES}",
            offset=start,
        )

    # Body streaming is done by the async _chunk_payload dependency so this
    # sync endpoint can run in FastAPI's worker threadpool; the FOR UPDATE
    # row lock in the service layer then serialises concurrent writers
    # without blocking the event loop.

    ack = service.put_chunk(
        db,
        session_id=session_id,
        start=start,
        declared_length=length,
        chunk_digest=sha256.lower(),
        payload=bytes(payload),
    )
    return ChunkAck(**ack)


# --------------------------------------------------------------------------- #
# Sealed archive retrieval
# --------------------------------------------------------------------------- #
@app.get("/sessions/{session_id}/content")
def download_content(
    session_id: str, db: Session = Depends(get_db)
) -> Response:
    session = db.get(UploadSession, session_id)
    if session is None:
        raise UploadError(
            404, "session_not_found", f"unknown session id: {session_id}"
        )
    if session.status != SEALED:
        raise UploadError(
            409,
            "not_sealed",
            f"content is only retrievable for sealed sessions (state={session.status})",
            expected_offset=session.confirmed_offset,
        )
    data = service.sealed_payload(db, session)
    # Defensive: a sealed record must recompute to the registered digest.
    if hashlib.sha256(data).hexdigest() != session.whole_sha256:
        raise UploadError(
            500,
            "sealed_integrity_error",
            "stored sealed content fails whole digest verification",
            expected_digest=session.whole_sha256,
        )
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(len(data)),
            "ETag": f'"{session.whole_sha256}"',
            "X-Whole-SHA256": session.whole_sha256,
        },
    )
