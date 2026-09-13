"""Persistent storage model.

A :class:`UploadSession` registers the immutable metadata of one seismograph
record package (total size + whole-package SHA-256).  Every accepted binary
chunk is stored as a :class:`Chunk` row so that checkpoints and payload bytes
survive an API process restart.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base

# Session lifecycle states ---------------------------------------------------
ACTIVE = "active"    # chunks are being accepted
SEALED = "sealed"    # complete, whole hash verified; immutable archive
FAILED = "failed"    # whole hash mismatch; terminal, cannot be resumed

# Audit trail events ----------------------------------------------------------
AUDIT_SEALED = "sealed"        # first snapshot: verification succeeded
AUDIT_COMPACTED = "compacted"  # a successful layout rewrite (incl. no-op)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UploadSession(Base):
    __tablename__ = "upload_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    whole_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    confirmed_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=ACTIVE)

    # Populated when final verification runs, regardless of its outcome.
    computed_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        onupdate=_utcnow,
    )

    chunks: Mapped[list["Chunk"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="Chunk.start_offset",
    )

    # Append-only archive audit trail (seal + successful compactions); lives
    # and dies with the session just like the chunk rows.
    audit_events: Mapped[list["AuditEvent"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="AuditEvent.sequence",
    )


class Chunk(Base):
    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("session_id", "start_offset", name="uq_chunk_session_offset"),
        Index("ix_chunk_session_span", "session_id", "start_offset", "end_offset"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("upload_sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    start_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    length: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    session: Mapped[UploadSession] = relationship(back_populates="chunks")


class AuditEvent(Base):
    """One immutable archive-trail snapshot, written inside the same
    transaction as the state change it describes.

    The seal verification writes exactly one ``"sealed"`` event (sequence 1);
    every successful compaction appends a ``"compacted"`` event carrying the
    target block size and the chunk counts before/after it.  A rolled-back
    seal or compaction therefore leaves no orphan audit rows.
    """

    __tablename__ = "audit_events"
    __table_args__ = (
        UniqueConstraint("session_id", "sequence", name="uq_audit_session_sequence"),
        Index("ix_audit_session_sequence", "session_id", "sequence"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("upload_sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Per-session ordinal starting at 1; defines the order of the trail.
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    # Whole-package snapshot at the time of the event.
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    whole_sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    # Compaction-only fields: NULL for the initial "sealed" snapshot.
    target_chunk_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    chunks_before: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    chunks_after: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    session: Mapped[UploadSession] = relationship(back_populates="audit_events")
