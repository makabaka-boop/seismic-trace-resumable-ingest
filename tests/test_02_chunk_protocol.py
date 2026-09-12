"""Chunk protocol: offsets, lengths, per-chunk digests, boundaries."""
from conftest import sha, unique_payload


def test_full_upload_in_order_seals(client):
    data = unique_payload(5000, seed=2)
    s = client.create_session(data)
    sid = s["id"]

    parts = [data[0:1000], data[1000:3000], data[3000:5000]]
    offset = 0
    for i, part in enumerate(parts):
        r = client.put_chunk(sid, offset, part)
        assert r.status_code == 200, r.text
        ack = r.json()
        offset += len(part)
        assert ack["confirmed_offset"] == offset
        assert ack["status"] == ("sealed" if i == len(parts) - 1 else "active")
    assert offset == 5000

    final = client.session(sid).json()
    assert final["status"] == "sealed"
    assert final["confirmed_offset"] == 5000
    assert final["computed_sha256"] == sha(data)

    got = client.content(sid)
    assert got.status_code == 200
    assert got.content == data
    assert got.headers["X-Whole-SHA256"] == sha(data)


def test_chunk_must_start_at_confirmed_offset(client):
    data = unique_payload(3000, seed=3)
    s = client.create_session(data)
    sid = s["id"]

    # Skip offset 0 -> rejected with expected_offset 0.
    r = client.put_chunk(sid, 100, data[100:200])
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "stale_offset"
    assert err["location"]["offset"] == 100
    assert err["location"]["expected_offset"] == 0

    # Correct first chunk.
    assert client.put_chunk(sid, 0, data[:100]).status_code == 200

    # Gap at 200.
    r = client.put_chunk(sid, 200, data[200:300])
    assert r.status_code == 409
    assert r.json()["error"]["location"] == {
        "offset": 200,
        "expected_offset": 100,
    }


def test_chunk_cannot_cross_total_bytes(client):
    data = unique_payload(1000, seed=4)
    s = client.create_session(data)
    sid = s["id"]

    r = client.put_chunk(sid, 0, data + b"overflow")
    assert r.status_code == 416
    err = r.json()["error"]
    assert err["code"] == "chunk_beyond_total"
    assert err["location"]["offset"] == 0
    assert err["details"]["end"] == len(data) + len(b"overflow")
    assert err["details"]["total_bytes"] == 1000

    # Final overhang even when aligned to the frontier is rejected.
    assert client.put_chunk(sid, 0, data[:900]).status_code == 200
    r = client.put_chunk(sid, 900, data[900:1000] + b"x")
    assert r.status_code == 416


def test_declared_length_must_match_body(client):
    data = unique_payload(100, seed=5)
    s = client.create_session(data)
    sid = s["id"]

    r = client.put_chunk(sid, 0, data, length=100)
    assert r.status_code == 200

    # New session; claim 50 bytes but send 60.
    s2 = client.create_session(unique_payload(500, seed=6))
    r = client.put_chunk(s2["id"], 0, unique_payload(60, seed=61), length=50)
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "length_mismatch"
    assert err["location"]["offset"] == 0
    assert err["details"] == {"declared_length": 50, "actual_length": 60}


def test_chunk_digest_must_match_payload(client):
    data = unique_payload(200, seed=7)
    s = client.create_session(data)
    sid = s["id"]
    payload = data[:100]
    wrong_digest = sha(b"something else")
    r = client.put_chunk(sid, 0, payload, digest=wrong_digest)
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "chunk_digest_mismatch"
    loc = err["location"]
    assert loc["offset"] == 0
    assert loc["digest"] == wrong_digest
    assert loc["expected_digest"] == sha(payload)

    # Nothing got confirmed.
    assert client.session(sid).json()["confirmed_offset"] == 0


def test_malformed_digest_query_rejected(client):
    s = client.create_session(unique_payload(10, seed=8))
    r = client.put_chunk(s["id"], 0, b"0123456789", digest="zzz")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_digest"
