"""Domain error type.

Every failure is reported with an ``error.code`` and, per protocol, must point
the field engineer at the offending **offset** and/or **digest** so a bad
retry can be corrected without guessing.
"""
from __future__ import annotations

from typing import Any


class UploadError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        offset: int | None = None,
        expected_offset: int | None = None,
        digest: str | None = None,
        expected_digest: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.offset = offset
        self.expected_offset = expected_offset
        self.digest = digest
        self.expected_digest = expected_digest
        self.details = details or {}

    def to_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "error": {
                "code": self.code,
                "message": self.message,
            }
        }
        loc: dict[str, Any] = {}
        if self.offset is not None:
            loc["offset"] = self.offset
        if self.expected_offset is not None:
            loc["expected_offset"] = self.expected_offset
        if self.digest is not None:
            loc["digest"] = self.digest
        if self.expected_digest is not None:
            loc["expected_digest"] = self.expected_digest
        if loc:
            body["error"]["location"] = loc
        if self.details:
            body["error"]["details"] = self.details
        return body
