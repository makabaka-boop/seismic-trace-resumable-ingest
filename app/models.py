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
