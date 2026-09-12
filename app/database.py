"""SQLAlchemy engine, session factory and schema initialization."""
from __future__ import annotations

import logging
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import DB_CONNECT_TIMEOUT, DATABASE_URL

logger = logging.getLogger("seis_upload.db")


class Base(DeclarativeBase):
    pass


# Import models so metadata is populated before create_all() runs.
from . import models  # noqa: E402

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def wait_for_database(timeout: int = DB_CONNECT_TIMEOUT) -> None:
    """Block until PostgreSQL answers, so an API restart never hits a cold DB."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            return
        except Exception as exc:  # pragma: no cover - exercised only during boot
            last_error = exc
            logger.info("waiting for database: %s", exc)
            time.sleep(1.0)
    raise RuntimeError(f"database not reachable after {timeout}s: {last_error}")


def init_db() -> None:
    """Create tables if they do not exist (idempotent across restarts)."""
    wait_for_database()
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    """FastAPI dependency yielding a scoped session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
