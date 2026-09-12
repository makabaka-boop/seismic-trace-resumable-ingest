"""Session registration and metadata validation."""
import hashlib

import requests

from conftest import API_BASE_URL, sha, unique_payload


def test_health():
    r = requests.get(f"{API_BASE_URL}/health", timeout=5)
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_create_session_registers_length_and_whole_digest(client):
    data = unique_payload(4096, seed=1)
    s = client.create_session(data)
    assert s["total_bytes"] == 4096
    assert s["whole_sha256"] == sha(data)
    assert s["confirmed_offset"] == 0
    assert s["status"] == "active"


def test_unknown_session_is_404(client):
    r = client.session("does-not-exist")
    assert r.status_code == 404
    body = r.json()["error"]
    assert body["code"] == "session_not_found"


def test_bad_digest_is_rejected_and_locates_digest(client):
    r = client.create_session_raw(100, "not-a-hex-digest")
    assert r.status_code == 422
    body = r.json()
    assert body["error"]["code"] == "validation_error"
    fields = [f["field"] for f in body["error"]["details"]["fields"]]
    assert "digest" in fields


def test_negative_total_rejected(client):
    r = client.create_session_raw(-1, "a" * 64)
    assert r.status_code == 422
    fields = [
        f["field"] for f in r.json()["error"]["details"]["fields"]
    ]
    assert "total_bytes" in fields


def test_empty_payload_with_correct_digest_is_born_sealed(client):
    empty_hash = hashlib.sha256(b"").hexdigest()
    r = client.create_session_raw(0, empty_hash)
    assert r.status_code == 201, r.text
    s = r.json()
    assert s["status"] == "sealed"
    assert s["confirmed_offset"] == 0
    # Content retrievable and genuinely empty.
    got = client.content(s["id"])
    assert got.status_code == 200
    assert got.content == b""
    assert got.headers["X-Whole-SHA256"] == empty_hash


def test_empty_payload_with_wrong_digest_fails_terminally(client):
    r = client.create_session_raw(0, "a" * 64)
    assert r.status_code == 422
    body = r.json()["error"]
    assert body["code"] == "whole_digest_mismatch"
    assert body["location"]["digest"] == "a" * 64
    assert body["location"]["expected_digest"] == hashlib.sha256(b"").hexdigest()
