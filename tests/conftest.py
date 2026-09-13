"""Acceptance-test fixtures.

Runs end-to-end over HTTP against a deployed stack (default
``http://api:8000`` inside the compose ``verify`` service; override with
``API_BASE_URL``).  Persistence tests additionally inspect PostgreSQL and,
when a docker socket is exposed, restart the real API container.
"""
from __future__ import annotations

import hashlib
import http.client
import os
import subprocess
import time
import uuid
from urllib.parse import urlsplit

import psycopg
import pytest
import requests

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000").rstrip("/")
DATABASE_DSN = os.getenv(
    "DATABASE_DSN",
    "postgresql://seis:seis@db:5432/seis",
)
RESTART_COMPOSE_SERVICE = os.getenv("RESTART_COMPOSE_SERVICE", "api")
DOCKER_SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")


# --------------------------------------------------------------------------- #
# Session-scoped readiness
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session", autouse=True)
def _wait_for_api() -> None:
    deadline = time.monotonic() + 120
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{API_BASE_URL}/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.RequestException as exc:
            last = exc
        time.sleep(1)
    raise RuntimeError(f"API not ready at {API_BASE_URL}: {last}")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def unique_payload(n: int, seed: int = 0) -> bytes:
    """Deterministic, high-entropy n-byte blob per test run."""
    out = bytearray()
    counter = seed
    while len(out) < n:
        out.extend(hashlib.sha256(f"{uuid.getnode()}-{seed}-{counter}".encode()).digest())
        counter += 1
    return bytes(out[:n])


class Client:
    def __init__(self) -> None:
        self.base = API_BASE_URL

    def create_session(self, data: bytes, total_bytes: int | None = None) -> dict:
        body = {
            "total_bytes": len(data) if total_bytes is None else total_bytes,
            "whole_sha256": sha(data),
        }
        r = requests.post(f"{self.base}/sessions", json=body, timeout=10)
        assert r.status_code == 201, r.text
        return r.json()

    def create_session_raw(self, total_bytes: int, whole_sha256: str):
        return requests.post(
            f"{self.base}/sessions",
            json={"total_bytes": total_bytes, "whole_sha256": whole_sha256},
            timeout=10,
        )

    def put_chunk(
        self,
        session_id: str,
        start: int,
        payload: bytes,
        *,
        length: int | None = None,
        digest: str | None = None,
    ) -> requests.Response:
        length = len(payload) if length is None else length
        digest = sha(payload) if digest is None else digest
        return requests.put(
            f"{self.base}/sessions/{session_id}/chunks",
            params={"start": start, "length": length, "sha256": digest},
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )

    def session(self, session_id: str) -> requests.Response:
        return requests.get(f"{self.base}/sessions/{session_id}", timeout=10)

    def chunks(self, session_id: str) -> requests.Response:
        return requests.get(
            f"{self.base}/sessions/{session_id}/chunks", timeout=10
        )

    def content(
        self, session_id: str, range_header: str | None = None
    ) -> requests.Response:
        headers = {"Range": range_header} if range_header is not None else {}
        return requests.get(
            f"{self.base}/sessions/{session_id}/content",
            headers=headers,
            timeout=30,
        )

    def content_raw_ranges(
        self, session_id: str, range_values: list[str]
    ) -> tuple[int, dict[str, str], bytes]:
        """GET /content with each Range value sent as its own header line.

        ``requests`` collapses duplicate header names into one, so a raw
        http.client connection is used to put several independent Range
        headers on the wire.  Returns (status, headers, body).
        """
        parts = urlsplit(self.base)
        conn = http.client.HTTPConnection(
            parts.hostname, parts.port, timeout=30
        )
        try:
            conn.putrequest("GET", f"/sessions/{session_id}/content")
            for value in range_values:
                conn.putheader("Range", value)
            conn.endheaders()
            resp = conn.getresponse()
            body = resp.read()
            headers = {k.lower(): v for k, v in resp.getheaders()}
            return resp.status, headers, body
        finally:
            conn.close()

    def compact(
        self, session_id: str, target_chunk_bytes: int | str
    ) -> requests.Response:
        return requests.post(
            f"{self.base}/sessions/{session_id}/compact",
            json={"target_chunk_bytes": target_chunk_bytes},
            timeout=60,
        )

    def audit(self, session_id: str) -> requests.Response:
        return requests.get(
            f"{self.base}/sessions/{session_id}/audit", timeout=10
        )

    def compare(
        self, baseline_session_id: str, candidate_session_id: str
    ) -> requests.Response:
        return requests.post(
            f"{self.base}/comparisons",
            json={
                "baseline_session_id": baseline_session_id,
                "candidate_session_id": candidate_session_id,
            },
            timeout=60,
        )

    def comparison(self, comparison_id: str) -> requests.Response:
        return requests.get(
            f"{self.base}/comparisons/{comparison_id}", timeout=10
        )


@pytest.fixture
def client() -> Client:
    return Client()


@pytest.fixture
def pg_conn():
    deadline = time.monotonic() + 60
    while True:
        try:
            conn = psycopg.connect(DATABASE_DSN, connect_timeout=3)
            break
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)
    yield conn
    conn.close()


# --------------------------------------------------------------------------- #
# Real API restart (only available with a mounted docker socket)
# --------------------------------------------------------------------------- #
def _docker_available() -> bool:
    if not os.path.exists(DOCKER_SOCKET):
        return False
    try:
        subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            timeout=10,
            check=True,
        )
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


restart_required = pytest.mark.skipif(
    not _docker_available(),
    reason="docker socket/CLI unavailable in this environment",
)


def _api_container_id() -> str:
    # Prefer the compose service label; fall back to name matching.
    label = (
        "com.docker.compose.service="
        f"{RESTART_COMPOSE_SERVICE}"
    )
    out = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"label={label}"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    ).stdout.split()
    if not out:
        out = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"name={RESTART_COMPOSE_SERVICE}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.split()
    assert out, "could not locate the running api container"
    return out[0]


@pytest.fixture
def restart_api():
    def _restart() -> None:
        cid = _api_container_id()
        subprocess.run(["docker", "restart", cid], check=True, timeout=60)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                r = requests.get(f"{API_BASE_URL}/health", timeout=3)
                if r.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise RuntimeError("API did not become healthy after restart")

    return _restart
