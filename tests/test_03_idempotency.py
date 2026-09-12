"""Idempotent retransmission and replay semantics."""
from conftest import sha, unique_payload


def test_identical_resend_of_confirmed_chunk_is_idempotent(client):
    data = unique_payload(4000, seed=10)
    s = client.create_session(data)
    sid = s["id"]
    c0, c1, c2 = data[:1000], data[1000:2500], data[2500:]

    first = client.put_chunk(sid, 0, c0).json()
    assert first["idempotent_replay"] is False
    assert first["confirmed_offset"] == 1000

    # Link drops after c1 is confirmed; device re-sends it.
    assert client.put_chunk(sid, 1000, c1).status_code == 200
    replay = client.put_chunk(sid, 1000, c1).json()
    assert replay["idempotent_replay"] is True
    # Replay must not move the checkpoint backwards or duplicate bytes.
    assert replay["confirmed_offset"] == 2500

    # Resume forward from the checkpoint.
    ack = client.put_chunk(sid, 2500, c2).json()
    assert ack["status"] == "sealed"
    assert client.content(sid).content == data


def test_resend_rechunked_but_byte_identical_succeeds(client):
    data = unique_payload(3000, seed=11)
    s = client.create_session(data)
    sid = s["id"]
    assert client.put_chunk(sid, 0, data[:1500]).status_code == 200

    # Same bytes, different chunk boundary inside the confirmed window.
    r = client.put_chunk(sid, 0, data[:500])
    assert r.status_code == 200
    assert r.json()["idempotent_replay"] is True

    r = client.put_chunk(sid, 500, data[500:1500])
    assert r.status_code == 200
    assert r.json()["confirmed_offset"] == 1500

    assert client.put_chunk(sid, 1500, data[1500:]).json()["status"] == "sealed"


def test_resend_with_different_content_is_rejected_with_expected(client):
    data = unique_payload(2000, seed=12)
    s = client.create_session(data)
    sid = s["id"]
    assert client.put_chunk(sid, 0, data[:1000]).status_code == 200

    # Same offsets, tampered payload carrying its OWN digest: per protocol
    # any non-identical old offset is answered with the expected offset.
    tampered = b"X" + data[1:500]
    r = client.put_chunk(sid, 0, tampered)
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "stale_offset"
    assert err["location"]["expected_offset"] == 1000

    # Claiming the *original* digest for tampered bytes fails the per-chunk
    # digest check, protecting archived bytes from overwrite.
    r = client.put_chunk(sid, 0, tampered, digest=sha(data[:1000]))
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "chunk_digest_mismatch"

    # Archived content is untouched and forward progress still possible.
    assert client.session(sid).json()["confirmed_offset"] == 1000
    assert client.put_chunk(sid, 1000, data[1000:]).json()["status"] == "sealed"
    assert client.content(sid).content == data


def test_overlapping_but_non_contiguous_chunk_points_to_expected(client):
    data = unique_payload(3000, seed=13)
    s = client.create_session(data)
    sid = s["id"]
    client.put_chunk(sid, 0, data[:1000])

    # Starts in the confirmed window but spills beyond it: not an exact
    # replay, not a frontier append -> stale_offset with expected=1000.
    r = client.put_chunk(sid, 900, data[900:1900])
    assert r.status_code == 409
    loc = r.json()["error"]["location"]
    assert loc == {"offset": 900, "expected_offset": 1000}
