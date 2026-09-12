"""Pydantic request/response schemas."""
from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

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
