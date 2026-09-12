"""Runtime configuration.

All settings are read from environment variables so the same image can be
driven by Docker Compose, pytest or a bare ``uvicorn`` invocation.
"""
from __future__ import annotations

import os


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "seis")
    password = os.getenv("POSTGRES_PASSWORD", "seis")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "seis")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


DATABASE_URL: str = _database_url()

# Maximum number of bytes the API will buffer/read for a single chunk body.
MAX_CHUNK_BYTES: int = int(os.getenv("MAX_CHUNK_BYTES", str(256 * 1024 * 1024)))

# Seconds to wait for PostgreSQL to accept connections during startup.
DB_CONNECT_TIMEOUT: int = int(os.getenv("DB_CONNECT_TIMEOUT", "60"))
