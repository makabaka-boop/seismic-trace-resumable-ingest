"""Persistence: checkpoints and chunk bytes survive an API restart."""
import pytest

from conftest import restart_required, sha, unique_payload


def _db_state(pg_conn, sid: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT total_bytes, whole_sha256, confirmed_offset, status "
            "FROM upload_sessions WHERE id = %s",
            (sid,),
        )
        session = cur.fetchone()
        cur.execute(
            "SELECT start_offset, end_offset, length, sha256, data, "
            "octet_length(data) "
            "FROM chunks WHERE session_id = %s ORDER BY start_offset",
            (sid,),
        )
        chunks = cur.fetchall()
    return session, chunks


def test_checkpoint_and_bytes_persisted_in_postgres(client, pg_conn):
    data = unique_payload(6000, seed=20)
    s = client.create_session(data)
    sid = s["id"]
    client.put_chunk(sid, 0, data[:2500])
    client.put_chunk(sid, 2500, data[2500:4000])

    pg_conn.rollback()  # fresh read
    session, chunks = _db_state(pg_conn, sid)
    total, whole, confirmed, status = session
    assert total == 6000
    assert whole == sha(data)
    assert confirmed == 4000
    assert status == "active"
    assert [(c[0], c[1], c[2]) for c in chunks] == [
        (0, 2500, 2500),
        (2500, 4000, 1500),
    ]
    # Chunk SHA matches the stored bytes, not just the request metadata.
    assert chunks[0][3] == sha(data[:2500])
    assert sha(bytes(chunks[0][4])) == sha(data[:2500])
    assert chunks[0][5] == 2500


@restart_required
def test_resume_after_real_api_restart(client, pg_conn, restart_api):
    data = unique_payload(7000, seed=21)
    s = client.create_session(data)
    sid = s["id"]
    client.put_chunk(sid, 0, data[:3000])
    client.put_chunk(sid, 3000, data[3000:5000])

    # Kill the API process (container restart); DB keeps the state.
    restart_api()

    # The session is queryable after restart...
    after = client.session(sid).json()
    assert after["status"] == "active"
    assert after["confirmed_offset"] == 5000

    # ...the chunks index survives...
    listed = client.chunks(sid).json()
    assert [(c["start_offset"], c["end_offset"]) for c in listed["chunks"]] == [
        (0, 3000),
        (3000, 5000),
    ]

    # ...an old confirmed chunk can still be replayed idempotently...
    replay = client.put_chunk(sid, 3000, data[3000:5000])
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True

    # ...and the transfer completes without re-uploading earlier bytes.
    done = client.put_chunk(sid, 5000, data[5000:])
    assert done.status_code == 200
    assert done.json()["status"] == "sealed"

    assert client.content(sid).content == data


@restart_required
def test_failed_state_survives_restart_and_blocks_resume(
    client, pg_conn, restart_api
):
    # Declare a bogus whole digest: all correct chunks accepted, finalize fails.
    data = unique_payload(1000, seed=22)
    r = client.create_session_raw(1000, "b" * 64)
    sid = r.json()["id"]
    r = client.put_chunk(sid, 0, data)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "whole_digest_mismatch"

    restart_api()

    s = client.session(sid).json()
    assert s["status"] == "failed"
    assert s["computed_sha256"] == sha(data)

    # Byte-identical replay stays idempotent (and reports the failed state);
    # only different content/new offsets are rejected.
    r = client.put_chunk(sid, 0, data)
    assert r.status_code == 200
    assert r.json()["status"] == "failed"
    assert r.json()["idempotent_replay"] is True

    r = client.put_chunk(sid, 0, b"Q" * 1000)
    assert r.status_code in (400, 409)


def test_session_listing_observable(client):
    import requests

    from conftest import API_BASE_URL

    data = unique_payload(100, seed=23)
    s = client.create_session(data)
    client.put_chunk(s["id"], 0, data)

    assert client.session(s["id"]).json()["status"] == "sealed"
    listing = requests.get(
        f"{API_BASE_URL}/sessions", params={"status": "sealed"}, timeout=10
    ).json()
    assert any(row["id"] == s["id"] for row in listing)

    # A finished package is the only downloadable, length+digest-consistent
    # record exposed to observers.
    active = client.create_session(unique_payload(10, seed=24))
    assert requests.get(
        f"{API_BASE_URL}/sessions/{active['id']}/content", timeout=10
    ).status_code == 409
