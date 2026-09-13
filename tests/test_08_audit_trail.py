"""Archive audit trail acceptance tests.

Operations staff can trace which verified content a layout change belongs
to without affecting the upload session lifecycle:

* a successful seal verification writes the unique first audit snapshot in
  the same transaction as the sealing PUT;
* every successful compaction appends an ordered snapshot that mirrors the
  compaction response (target block size, chunks before/after, total length,
  whole digest);
* failed seals, rejected/rolled-back compactions leave no audit rows at all;
* the trail is read-only: unknown sessions reuse the standard 404 while
  active/failed sessions (and sessions sealed before auditing existed)
  return an empty trail.
"""
import hashlib

import pytest

from conftest import restart_required, sha, unique_payload


def _upload_sealed(client, data, piece=None):
    s = client.create_session(data)
    sid = s["id"]
    step = len(data) if piece is None else piece
    for off in range(0, len(data), step):
        r = client.put_chunk(sid, off, data[off : off + step])
        assert r.status_code == 200, r.text
    assert client.session(sid).json()["status"] == "sealed"
    return sid


def _events(client, sid):
    r = client.audit(sid)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == sid
    return body["events"]


def _audit_count(pg_conn, sid):
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM audit_events WHERE session_id = %s", (sid,)
        )
        return cur.fetchone()[0]


# --------------------------------------------------------------------------- #
# First snapshot: seal
# --------------------------------------------------------------------------- #
def test_successful_seal_writes_unique_first_snapshot(client, pg_conn):
    data = unique_payload(6000, seed=80)
    sid = client.create_session(data)["id"]

    # An active session has no trail yet.
    r = client.put_chunk(sid, 0, data[:2500])
    assert r.status_code == 200
    assert _events(client, sid) == []
    assert _audit_count(pg_conn, sid) == 0

    # The sealing chunk produces exactly the first snapshot, in its
    # transaction (the response already reports "sealed").
    r = client.put_chunk(sid, 2500, data[2500:])
    assert r.status_code == 200
    assert r.json()["status"] == "sealed"

    events = _events(client, sid)
    assert len(events) == 1
    first = events[0]
    assert first["sequence"] == 1
    assert first["event"] == "sealed"
    assert first["total_bytes"] == 6000
    assert first["whole_sha256"] == sha(data)
    # A seal snapshot carries no compaction statistics.
    assert first["target_chunk_bytes"] is None
    assert first["chunks_before"] is None
    assert first["chunks_after"] is None
    assert first["occurred_at"]

    # Repeated byte-identical replays against the sealed archive do not add
    # any audit rows: the lifecycle (and its trail) stays unchanged.
    for off, length in ((0, 2500), (2500, 3500), (0, 6000)):
        replay = client.put_chunk(sid, off, data[off : off + length])
        assert replay.status_code == 200
        assert replay.json()["idempotent_replay"] is True
    assert len(_events(client, sid)) == 1
    assert _audit_count(pg_conn, sid) == 1

    # The snapshot is physically attached to the session row.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT sequence, event, total_bytes, whole_sha256, "
            "target_chunk_bytes, chunks_before, chunks_after "
            "FROM audit_events WHERE session_id = %s ORDER BY sequence",
            (sid,),
        )
        rows = cur.fetchall()
    assert rows == [(1, "sealed", 6000, sha(data), None, None, None)]


def test_zero_length_sealed_package_starts_trail_at_one(client):
    empty_hash = hashlib.sha256(b"").hexdigest()
    r = client.create_session_raw(0, empty_hash)
    assert r.status_code == 201
    sid = r.json()["id"]

    events = _events(client, sid)
    assert [
        (e["sequence"], e["event"], e["total_bytes"], e["whole_sha256"])
        for e in events
    ] == [(1, "sealed", 0, empty_hash)]


def test_failed_finalization_writes_no_snapshot(client, pg_conn):
    data = unique_payload(3000, seed=81)
    sid = client.create_session_raw(len(data), "a" * 64).json()["id"]

    assert client.put_chunk(sid, 0, data[:1500]).status_code == 200
    final = client.put_chunk(sid, 1500, data[1500:])
    assert final.status_code == 422
    assert final.json()["error"]["code"] == "whole_digest_mismatch"
    assert client.session(sid).json()["status"] == "failed"

    # Failed verification is rolled back: no first snapshot exists.
    assert _events(client, sid) == []
    assert _audit_count(pg_conn, sid) == 0


# --------------------------------------------------------------------------- #
# Appended snapshots: compaction
# --------------------------------------------------------------------------- #
def test_compaction_appends_ordered_snapshots_matching_results(client, pg_conn):
    # 10000 bytes uploaded as fifty 200-byte chunks.
    data = unique_payload(10000, seed=82)
    sid = _upload_sealed(client, data, piece=200)

    first = client.compact(sid, 4096)
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert (first_body["chunks_before"], first_body["chunks_after"]) == (50, 3)

    events = _events(client, sid)
    assert len(events) == 2
    seal, snap = events
    assert seal["sequence"] == 1 and seal["event"] == "sealed"

    # The appended snapshot mirrors the compaction result.
    assert snap["sequence"] == 2
    assert snap["event"] == "compacted"
    assert snap["target_chunk_bytes"] == 4096
    assert snap["chunks_before"] == first_body["chunks_before"] == 50
    assert snap["chunks_after"] == first_body["chunks_after"] == 3
    assert snap["total_bytes"] == first_body["total_bytes_after"] == 10000
    assert snap["whole_sha256"] == first_body["whole_sha256"] == sha(data)
    assert snap["occurred_at"] >= seal["occurred_at"]

    # A second, different target appends the next snapshot in order.
    second = client.compact(sid, 2000)
    assert second.status_code == 200
    second_body = second.json()
    assert (second_body["chunks_before"], second_body["chunks_after"]) == (3, 5)

    events = _events(client, sid)
    assert [e["sequence"] for e in events] == [1, 2, 3]
    assert [e["event"] for e in events] == ["sealed", "compacted", "compacted"]
    third = events[2]
    assert third["target_chunk_bytes"] == 2000
    assert third["chunks_before"] == 3
    assert third["chunks_after"] == 5
    assert third["total_bytes"] == 10000
    assert third["whole_sha256"] == sha(data)

    # A same-target idempotent no-op is still a successful compaction, so it
    # is traceable too (no chunk rows change; counts are equal).
    noop = client.compact(sid, 2000)
    assert noop.status_code == 200
    noop_body = noop.json()
    assert noop_body["chunks_before"] == noop_body["chunks_after"] == 5
    events = _events(client, sid)
    assert [e["sequence"] for e in events] == [1, 2, 3, 4]
    fourth = events[3]
    assert (fourth["chunks_before"], fourth["chunks_after"]) == (5, 5)
    assert fourth["target_chunk_bytes"] == 2000

    # Physical rows are attached to the same session, ordered by sequence.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT sequence, event, target_chunk_bytes, chunks_before, "
            "chunks_after, total_bytes, whole_sha256 "
            "FROM audit_events WHERE session_id = %s ORDER BY sequence",
            (sid,),
        )
        rows = cur.fetchall()
    assert rows == [
        (1, "sealed", None, None, None, 10000, sha(data)),
        (2, "compacted", 4096, 50, 3, 10000, sha(data)),
        (3, "compacted", 2000, 3, 5, 10000, sha(data)),
        (4, "compacted", 2000, 5, 5, 10000, sha(data)),
    ]

    # The audit interface is read-only: querying it never moves the session.
    assert client.session(sid).json()["status"] == "sealed"
    assert client.content(sid).content == data


# --------------------------------------------------------------------------- #
# Failures and rollbacks never leave rows
# --------------------------------------------------------------------------- #
def test_rejected_and_rolled_back_compactions_add_no_snapshot(
    client, pg_conn
):
    data = unique_payload(4000, seed=83)
    sid = _upload_sealed(client, data, piece=500)
    sealed_layout = client.chunks(sid).json()["chunks"]
    assert len(_events(client, sid)) == 1

    # active/failed sessions cannot be compacted: conflict, no audit row.
    active_data = unique_payload(2000, seed=84)
    aid = client.create_session(active_data)["id"]
    client.put_chunk(aid, 0, active_data[:1000])
    assert client.session(aid).json()["status"] == "active"
    assert client.compact(aid, 1000).status_code == 409
    assert _events(client, aid) == []

    bad = client.create_session_raw(len(active_data), "b" * 64).json()["id"]
    final = client.put_chunk(bad, 0, active_data)
    assert final.status_code == 422
    assert client.compact(bad, 1000).status_code == 409
    assert _events(client, bad) == []
    assert _audit_count(pg_conn, bad) == 0

    # Integrity failure inside the single compaction transaction rolls the
    # whole rewrite back: still only the seal snapshot exists.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE chunks SET data = "
            "set_byte(data, 0, get_byte(data, 0) # 255) "
            "WHERE session_id = %s AND start_offset = 0",
            (sid,),
        )
    pg_conn.commit()

    r = client.compact(sid, 1024)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "compaction_integrity_error"

    events = _events(client, sid)
    assert len(events) == 1 and events[0]["event"] == "sealed"
    assert _audit_count(pg_conn, sid) == 1
    # The corrupting rewrite did not survive either; restore the bytes and
    # confirm the original layout is intact.
    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE chunks SET data = "
            "set_byte(data, 0, get_byte(data, 0) # 255) "
            "WHERE session_id = %s AND start_offset = 0",
            (sid,),
        )
    pg_conn.commit()
    assert client.chunks(sid).json()["chunks"] == sealed_layout

    # Failure while writing the audit row itself (target exceeds bigint)
    # must roll back the chunk rewrite too — no orphan layout, no audit row.
    before_layout = [(c["start_offset"], c["end_offset"]) for c in
                     client.chunks(sid).json()["chunks"]]
    r = client.compact(sid, 10**20)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "audit_write_failed"
    assert [(c["start_offset"], c["end_offset"]) for c in
            client.chunks(sid).json()["chunks"]] == before_layout
    events = _events(client, sid)
    assert len(events) == 1 and events[0]["event"] == "sealed"
    assert _audit_count(pg_conn, sid) == 1
    # Content identity is untouched.
    assert client.content(sid).content == data


# --------------------------------------------------------------------------- #
# Lookup semantics
# --------------------------------------------------------------------------- #
def test_audit_lookup_errors_and_empty_trails(client):
    # Unknown session reuses the existing not-found error.
    r = client.audit("does-not-exist")
    assert r.status_code == 404
    err = r.json()["error"]
    assert err["code"] == "session_not_found"
    # Same error shape as the session lookup itself.
    assert client.session("does-not-exist").json()["error"] == err

    # Active session: empty trail.
    data = unique_payload(2000, seed=85)
    aid = client.create_session(data)["id"]
    client.put_chunk(aid, 0, data[:1000])
    assert _events(client, aid) == []

    # Failed session: empty trail.
    fid = client.create_session_raw(len(data), "f" * 64).json()["id"]
    final = client.put_chunk(fid, 0, data)
    assert final.status_code == 422
    assert _events(client, fid) == []


def test_session_sealed_before_audit_feature_has_empty_trail(
    client, pg_conn
):
    # Simulate legacy data: a sealed session + verified chunks written
    # directly, without any audit_events rows.
    data = unique_payload(2500, seed=86)
    sid = "08080808-0808-0808-0808-080808080808"
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        # Idempotent across reruns against the same database.
        cur.execute("DELETE FROM upload_sessions WHERE id = %s", (sid,))
        cur.execute(
            "INSERT INTO upload_sessions "
            "(id, total_bytes, whole_sha256, confirmed_offset, status, "
            "computed_sha256, failure_reason, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, 'sealed', %s, NULL, now(), now())",
            (sid, len(data), sha(data), len(data), sha(data)),
        )
        cur.execute(
            "INSERT INTO chunks "
            "(session_id, start_offset, end_offset, length, sha256, data, "
            "created_at) VALUES (%s, 0, %s, %s, %s, %s, now())",
            (sid, len(data), len(data), sha(data), data),
        )
    pg_conn.commit()

    # Empty trail for pre-upgrade data; download still works.
    assert _events(client, sid) == []
    got = client.content(sid)
    assert got.status_code == 200
    assert got.content == data

    # Compacting it seeds the trail after the fact; the seal snapshot is
    # never back-filled.
    r = client.compact(sid, 1000)
    assert r.status_code == 200, r.text
    events = _events(client, sid)
    assert [e["sequence"] for e in events] == [1]
    assert events[0]["event"] == "compacted"
    assert events[0]["chunks_before"] == 1
    assert events[0]["chunks_after"] == 3
    assert events[0]["total_bytes"] == 2500
    assert events[0]["whole_sha256"] == sha(data)


@restart_required
def test_audit_trail_survives_api_restart(client, restart_api):
    data = unique_payload(5000, seed=87)
    sid = _upload_sealed(client, data, piece=700)

    r = client.compact(sid, 2048)
    assert r.status_code == 200
    assert (r.json()["chunks_before"], r.json()["chunks_after"]) == (8, 3)

    restart_api()

    events = _events(client, sid)
    assert [(e["sequence"], e["event"]) for e in events] == [
        (1, "sealed"),
        (2, "compacted"),
    ]
    assert events[1]["target_chunk_bytes"] == 2048
    assert (events[1]["chunks_before"], events[1]["chunks_after"]) == (8, 3)
    assert events[0]["whole_sha256"] == events[1]["whole_sha256"] == sha(data)


def test_original_endpoints_remain_compatible(client):
    # Creation, resume/idempotent replay, seal, list, chunks, content and
    # compaction contracts keep their original shapes alongside the trail.
    data = unique_payload(4500, seed=88)
    s = client.create_session(data)
    sid = s["id"]
    assert set(s) == {
        "id", "total_bytes", "whole_sha256", "confirmed_offset", "status",
        "computed_sha256", "failure_reason",
    }

    first = client.put_chunk(sid, 0, data[:3000]).json()
    assert set(first) == {
        "id", "status", "start_offset", "length", "chunk_sha256",
        "confirmed_offset", "expected_offset", "total_bytes",
        "idempotent_replay",
    }
    replay = client.put_chunk(sid, 0, data[:3000]).json()
    assert replay["idempotent_replay"] is True

    done = client.put_chunk(sid, 3000, data[3000:]).json()
    assert done["status"] == "sealed"

    listing = client.session(sid).json()
    assert listing["status"] == "sealed"
    chunks = client.chunks(sid).json()
    assert set(chunks) == {
        "id", "status", "confirmed_offset", "total_bytes", "chunks",
    }

    content = client.content(sid)
    assert content.content == data
    assert content.headers["X-Whole-SHA256"] == sha(data)

    compacted = client.compact(sid, 4096)
    assert compacted.status_code == 200
    body = compacted.json()
    assert set(body) == {
        "id", "status", "target_chunk_bytes", "chunks_before",
        "chunks_after", "total_bytes_before", "total_bytes_after",
        "whole_sha256", "chunks",
    }
    assert body["chunks_after"] == 2
    assert client.content(sid).content == data
    # ...and the new trail endpoint is the only addition.
    assert len(_events(client, sid)) == 2


@pytest.mark.parametrize("piece", [1, 333])
def test_unique_first_snapshot_under_different_chunkings(client, piece):
    data = unique_payload(1000, seed=89)
    sid = _upload_sealed(client, data, piece=piece)
    events = _events(client, sid)
    seals = [e for e in events if e["event"] == "sealed"]
    assert len(seals) == 1
    assert seals[0]["sequence"] == 1
