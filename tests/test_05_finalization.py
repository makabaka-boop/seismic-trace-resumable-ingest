"""Whole-package digest verification and terminal states."""
from conftest import sha, unique_payload


def test_wrong_whole_digest_fails_session_terminally(client):
    data = unique_payload(3000, seed=30)
    # Metadata promises a different package than the bytes that will arrive.
    s = client.create_session_raw(len(data), "c" * 64).json()
    sid = s["id"]

    assert client.put_chunk(sid, 0, data[:1500]).status_code == 200
    final = client.put_chunk(sid, 1500, data[1500:])
    assert final.status_code == 422
    err = final.json()["error"]
    assert err["code"] == "whole_digest_mismatch"
    assert err["location"]["offset"] == 3000
    assert err["location"]["digest"] == "c" * 64
    assert err["location"]["expected_digest"] == sha(data)

    info = client.session(sid).json()
    assert info["status"] == "failed"
    assert info["computed_sha256"] == sha(data)
    assert info["failure_reason"]


def test_failed_session_cannot_be_resumed_and_expects_new_metadata(client):
    data = unique_payload(1000, seed=31)
    sid = client.create_session_raw(1000, "d" * 64).json()["id"]
    client.put_chunk(sid, 0, data)

    # A byte-identical replay of an archived sub-range stays idempotent even
    # after terminal failure...
    r = client.put_chunk(sid, 0, data[:500])
    assert r.status_code == 200
    ack = r.json()
    assert ack["status"] == "failed"
    assert ack["idempotent_replay"] is True

    # ...but any attempt to provide NEW bytes is firmly refused.
    r = client.put_chunk(sid, 500, b"Z" * 500)
    assert r.status_code in (400, 409)

    # The engineer must register a new session with correct metadata.
    good = client.create_session(data)
    ack = client.put_chunk(good["id"], 0, data).json()
    assert ack["status"] == "sealed"
    assert client.content(good["id"]).content == data


def test_sealed_session_rejects_different_content(client):
    data = unique_payload(2000, seed=32)
    s = client.create_session(data)
    sid = s["id"]
    client.put_chunk(sid, 0, data[:1000])
    client.put_chunk(sid, 1000, data[1000:])
    assert client.session(sid).json()["status"] == "sealed"

    other = b"!" + data[1:1000]
    r = client.put_chunk(sid, 0, other)  # digest of tampered bytes
    assert r.status_code in (400, 409)
    if r.status_code == 409:
        assert r.json()["error"]["code"] == "sealed_content_conflict"
    else:
        assert r.json()["error"]["code"] == "chunk_digest_mismatch"

    # Claiming the original digest for tampered bytes is also refused.
    r = client.put_chunk(sid, 0, other, digest=sha(data[:1000]))
    assert r.status_code in (400, 409)

    # Identical replay against the sealed archive stays idempotent.
    r = client.put_chunk(sid, 0, data[:1000])
    assert r.status_code == 200
    assert r.json()["idempotent_replay"] is True
    assert r.json()["status"] == "sealed"

    # Archive is byte-for-byte unchanged.
    assert client.content(sid).content == data


def test_only_digest_and_length_consistent_record_is_observable(client):
    data = unique_payload(2500, seed=33)
    good = client.create_session(data)
    client.put_chunk(good["id"], 0, data)

    bad = client.create_session_raw(len(data), "e" * 64).json()
    client.put_chunk(bad["id"], 0, data)

    good_info = client.session(good["id"]).json()
    bad_info = client.session(bad["id"]).json()
    assert good_info["status"] == "sealed"
    assert bad_info["status"] == "failed"

    got = client.content(good["id"])
    assert got.status_code == 200
    assert len(got.content) == good_info["total_bytes"]
    assert got.headers["X-Whole-SHA256"] == good_info["whole_sha256"]

    assert client.content(bad["id"]).status_code == 409
