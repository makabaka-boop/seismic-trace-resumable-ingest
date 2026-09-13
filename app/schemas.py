"""Pydantic request/response schemas."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, StrictInt, field_validator

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_sha256(value: str, field_name: str) -> str:
    normalized = value.strip().lower()
    if not SHA256_RE.match(normalized):
        raise ValueError(
            f"{field_name} must be 64 lowercase hexadecimal characters (SHA-256)"
        )
    return normalized


class SessionCreate(BaseModel):
    total_bytes: int = Field(..., ge=0, description="Total package size in bytes")
    whole_sha256: str = Field(..., description="Expected SHA-256 of the full package")

    @field_validator("whole_sha256")
    @classmethod
    def _check_whole(cls, v: str) -> str:
        return _validate_sha256(v, "whole_sha256")


class SessionResponse(BaseModel):
    id: str
    total_bytes: int
    whole_sha256: str
    confirmed_offset: int
    status: Literal["active", "sealed", "failed"]
    computed_sha256: str | None = None
    failure_reason: str | None = None


class ChunkAck(BaseModel):
    """Response to every accepted/idempotently-replayed chunk."""

    id: str
    status: Literal["active", "sealed", "failed"]
    start_offset: int
    length: int
    chunk_sha256: str
    confirmed_offset: int
    expected_offset: int
    total_bytes: int
    idempotent_replay: bool = False


class CompactRequest(BaseModel):
    """Target maximum chunk size in bytes for a sealed-package compaction.

    Strictly typed: numeric strings, floats and booleans are rejected with
    a 422 parameter error instead of being silently coerced to an int.
    """

    target_chunk_bytes: StrictInt = Field(
        ..., gt=0, description="New chunks must not exceed this many bytes"
    )


class ChunkInfo(BaseModel):
    start_offset: int
    end_offset: int
    length: int
    sha256: str


class CompactResponse(BaseModel):
    """Before/after layout of a successful compaction.

    The package identity (session id, total length, whole SHA-256) is
    unchanged; only the chunk boundary layout is rewritten.
    """

    id: str
    status: Literal["sealed"]
    target_chunk_bytes: int
    chunks_before: int
    chunks_after: int
    total_bytes_before: int
    total_bytes_after: int
    whole_sha256: str
    chunks: list[ChunkInfo]


class AuditEventResponse(BaseModel):
    """One archive-trail snapshot, ordered by ``sequence``."""

    sequence: int
    event: Literal["sealed", "compacted"]
    occurred_at: datetime
    total_bytes: int
    whole_sha256: str
    # Only a "compacted" snapshot carries layout statistics; a sealed
    # snapshot leaves them null.
    target_chunk_bytes: int | None = None
    chunks_before: int | None = None
    chunks_after: int | None = None


class AuditTrailResponse(BaseModel):
    id: str
    events: list[AuditEventResponse]


class ComparisonCreate(BaseModel):
    """Submit the two sealed sessions whose archives should be compared."""

    baseline_session_id: str = Field(
        ..., min_length=1, description="Session id of the baseline archive"
    )
    candidate_session_id: str = Field(
        ..., min_length=1, description="Session id of the candidate archive"
    )


class ComparisonResponse(BaseModel):
    """Immutable comparison snapshot.

    ``first_difference_offset`` is null only for identical archives; for a
    pure length difference it is the offset at which the shorter archive
    ends (equal to ``common_prefix_bytes``).
    """

    id: str
    baseline_session_id: str
    candidate_session_id: str
    baseline_total_bytes: int
    baseline_whole_sha256: str
    candidate_total_bytes: int
    candidate_whole_sha256: str
    common_prefix_bytes: int
    first_difference_offset: int | None = None
    conclusion: Literal["identical", "content_differs", "length_differs"]
    created_at: datetime
