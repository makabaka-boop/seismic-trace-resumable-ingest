"""Sealed-archive comparison records.

Before two sealed record packages of the same survey line are delivered,
operations can ask the service whether their contents are identical and
where the first differing byte lives — without downloading either package:

* ``POST /comparisons`` re-verifies both archives (chunk continuity,
  per-chunk digests, whole-package digests), walks both byte streams in
  offset order and persists an immutable record: per-side length/digest
  snapshots, common prefix length, first difference offset and the
  conclusion (``identical`` / ``content_differs`` / ``length_differs``);
* the record is returned with 201 and can be re-fetched by its identifier
  via ``GET /comparisons/{id}`` — surviving API restarts and unaffected by
  later compaction of either archive;
* both sessions must be sealed: unknown ids reuse the standard 404, while
  active/failed sessions get a 409 that names the side and its state, and
  no comparison row is ever written for rejected or failed requests.
"""
import hashlib

import pytest

from conftest import restart_required, sha, unique_payload


def _upload_sealed(client, data, piece):
    s = client.create_session(data)
    sid = s["id"]
    for off in range(0, len(data), piece):
        r = client.put_chunk(sid, off, data[off : off + piece])
        assert r.status_code == 200, r.text
    assert client.session(sid).json()["status"] == "sealed"
    return sid


def _comparison_count(pg_conn):
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM comparisons")
        return cur.fetchone()[0]


def _get(client, comparison_id):
    r = client.comparison(comparison_id)
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------------------------------- #
# Conclusions and offsets
# --------------------------------------------------------------------------- #
def test_identical_content_with_different_chunk_boundaries(client, pg_conn):
    data = unique_payload(9000, seed=100)
    # Same bytes, deliberately different chunk layouts on each side.
    baseline = _upload_sealed(client, data, 4096)   # 3 chunks
    candidate = _upload_sealed(client, data, 250)   # 36 chunks
    assert len(client.chunks(baseline).json()["chunks"]) == 3
    assert len(client.chunks(candidate).json()["chunks"]) == 36

    r = client.compare(baseline, candidate)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["id"]
    assert body["baseline_session_id"] == baseline
    assert body["candidate_session_id"] == candidate
    assert body["conclusion"] == "identical"
    assert body["common_prefix_bytes"] == len(data)
    assert body["first_difference_offset"] is None
    # Per-side snapshots mirror the registered package metadata.
    assert body["baseline_total_bytes"] == len(data)
    assert body["candidate_total_bytes"] == len(data)
    assert body["baseline_whole_sha256"] == sha(data)
    assert body["candidate_whole_sha256"] == sha(data)
    assert body["created_at"]

    # The record is physically persisted and re-fetchable by identifier.
    assert _get(client, body["id"]) == body
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT baseline_session_id, candidate_session_id, "
            "baseline_total_bytes, candidate_total_bytes, "
            "baseline_whole_sha256, candidate_whole_sha256, "
            "common_prefix_bytes, first_difference_offset, conclusion "
            "FROM comparisons WHERE id = %s",
            (body["id"],),
        )
        rows = cur.fetchall()
    assert rows == [
        (
            baseline, candidate, len(data), len(data),
            sha(data), sha(data), len(data), None, "identical",
        )
    ]


def test_single_byte_difference_after_common_prefix(client):
    data = bytearray(unique_payload(6000, seed=101))
    baseline = _upload_sealed(client, bytes(data), 1000)

    flipped = bytearray(data)
    flipped[4321] ^= 0xFF  # one byte, deep inside the common span
    candidate = _upload_sealed(client, bytes(flipped), 777)

    r = client.compare(baseline, candidate)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["conclusion"] == "content_differs"
    assert body["common_prefix_bytes"] == 4321
    assert body["first_difference_offset"] == 4321
    assert body["baseline_total_bytes"] == 6000
    assert body["candidate_total_bytes"] == 6000
    assert body["baseline_whole_sha256"] == sha(bytes(data))
    assert body["candidate_whole_sha256"] == sha(bytes(flipped))

    # The same record comes back unchanged from the query endpoint.
    assert _get(client, body["id"]) == body


def test_difference_inside_first_chunk_of_each_side(client):
    baseline = _upload_sealed(client, unique_payload(2000, seed=102), 500)
    other = bytearray(unique_payload(2000, seed=102))
    other[0] ^= 0x01  # very first byte differs
    candidate = _upload_sealed(client, bytes(other), 500)

    body = client.compare(baseline, candidate).json()
    assert body["conclusion"] == "content_differs"
    assert body["common_prefix_bytes"] == 0
    assert body["first_difference_offset"] == 0


@pytest.mark.parametrize("longer_side", ["baseline", "candidate"])
def test_same_prefix_but_different_lengths(client, longer_side):
    prefix = unique_payload(5000, seed=103)
    extra = unique_payload(1500, seed=104)
    short = _upload_sealed(client, prefix, 800)
    long = _upload_sealed(client, prefix + extra, 4096)

    if longer_side == "baseline":
        r = client.compare(long, short)
    else:
        r = client.compare(short, long)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["conclusion"] == "length_differs"
    # The first difference is where the shorter archive ends.
    assert body["common_prefix_bytes"] == len(prefix)
    assert body["first_difference_offset"] == len(prefix)
    assert {body["baseline_total_bytes"], body["candidate_total_bytes"]} == {
        len(prefix), len(prefix) + len(extra)
    }
    assert _get(client, body["id"]) == body


def test_zero_length_packages_compare_identical(client):
    empty_hash = hashlib.sha256(b"").hexdigest()
    baseline = client.create_session_raw(0, empty_hash).json()["id"]
    candidate = client.create_session_raw(0, empty_hash).json()["id"]

    r = client.compare(baseline, candidate)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["conclusion"] == "identical"
    assert body["common_prefix_bytes"] == 0
    assert body["first_difference_offset"] is None


def test_session_compared_with_itself_is_identical(client):
    data = unique_payload(3000, seed=105)
    sid = _upload_sealed(client, data, 700)
    body = client.compare(sid, sid).json()
    assert body["conclusion"] == "identical"
    assert body["common_prefix_bytes"] == len(data)
    assert body["first_difference_offset"] is None


# --------------------------------------------------------------------------- #
# Preconditions: sealed only, existing not-found error, no side effects
# --------------------------------------------------------------------------- #
def test_unknown_sessions_reuse_session_not_found(client):
    data = unique_payload(1000, seed=106)
    sid = _upload_sealed(client, data, 500)

    r = client.compare("does-not-exist", sid)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    # Same error shape as the existing session lookup.
    assert r.json()["error"] == client.session("does-not-exist").json()["error"]

    r = client.compare(sid, "does-not-exist")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_non_sealed_sessions_conflict_and_write_nothing(client, pg_conn):
    sealed_data = unique_payload(2000, seed=107)
    sealed = _upload_sealed(client, sealed_data, 1000)
    before = _comparison_count(pg_conn)

    # Active candidate: the conflict names the side and its state.
    active_data = unique_payload(2000, seed=108)
    active = client.create_session(active_data)["id"]
    client.put_chunk(active, 0, active_data[:500])
    assert client.session(active).json()["status"] == "active"

    r = client.compare(sealed, active)
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "comparison_state_conflict"
    assert err["details"]["side"] == "candidate"
    assert err["details"]["session_id"] == active
    assert err["details"]["status"] == "active"

    # Failed baseline: same contract, other side.
    failed_data = unique_payload(2000, seed=109)
    failed = client.create_session_raw(len(failed_data), "e" * 64).json()["id"]
    final = client.put_chunk(failed, 0, failed_data)
    assert final.status_code == 422
    assert client.session(failed).json()["status"] == "failed"

    r = client.compare(failed, sealed)
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "comparison_state_conflict"
    assert err["details"]["side"] == "baseline"
    assert err["details"]["session_id"] == failed
    assert err["details"]["status"] == "failed"

    # Neither rejected request left a comparison record behind.
    assert _comparison_count(pg_conn) == before


def test_unknown_comparison_id_is_not_found(client):
    r = client.comparison("does-not-exist")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "comparison_not_found"


# --------------------------------------------------------------------------- #
# Archive integrity failures never leave a record
# --------------------------------------------------------------------------- #
def test_archive_integrity_failure_leaves_no_record(client, pg_conn):
    data = unique_payload(4000, seed=110)
    baseline = _upload_sealed(client, data, 500)
    candidate = _upload_sealed(client, data, 1000)
    before = _comparison_count(pg_conn)

    # Corrupt one persisted byte of the candidate archive out-of-band.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE chunks SET data = "
            "set_byte(data, 0, get_byte(data, 0) # 255) "
            "WHERE session_id = %s AND start_offset = 0",
            (candidate,),
        )
    pg_conn.commit()
    try:
        r = client.compare(baseline, candidate)
        assert r.status_code == 500
        err = r.json()["error"]
        assert err["code"] == "comparison_integrity_error"
        assert err["details"]["side"] == "candidate"
        assert err["details"]["session_id"] == candidate
        # The failed verification left no comparison row.
        assert _comparison_count(pg_conn) == before
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE chunks SET data = "
                "set_byte(data, 0, get_byte(data, 0) # 255) "
                "WHERE session_id = %s AND start_offset = 0",
                (candidate,),
            )
        pg_conn.commit()

    # Restored archive compares cleanly again.
    r = client.compare(baseline, candidate)
    assert r.status_code == 201
    assert r.json()["conclusion"] == "identical"


# --------------------------------------------------------------------------- #
# Immutability: compaction and restarts cannot change a saved record
# --------------------------------------------------------------------------- #
def test_record_survives_later_compaction(client, pg_conn):
    data = bytearray(unique_payload(8000, seed=111))
    baseline = _upload_sealed(client, bytes(data), 300)
    flipped = bytearray(data)
    flipped[5555] ^= 0xFF
    candidate = _upload_sealed(client, bytes(flipped), 2048)

    created = client.compare(baseline, candidate)
    assert created.status_code == 201
    body = created.json()
    assert body["conclusion"] == "content_differs"
    assert body["first_difference_offset"] == 5555

    # Compacting both archives afterwards rewrites chunk layouts but must
    # not touch the saved snapshot.
    assert client.compact(baseline, 1024).status_code == 200
    assert client.compact(candidate, 999).status_code == 200
    assert len(client.chunks(baseline).json()["chunks"]) == 8
    assert len(client.chunks(candidate).json()["chunks"]) == 9

    assert _get(client, body["id"]) == body

    # The snapshot is still physically intact in PostgreSQL.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT conclusion, common_prefix_bytes, first_difference_offset, "
            "baseline_whole_sha256, candidate_whole_sha256 "
            "FROM comparisons WHERE id = %s",
            (body["id"],),
        )
        row = cur.fetchone()
    assert row == (
        "content_differs", 5555, 5555, sha(bytes(data)), sha(bytes(flipped))
    )


@restart_required
def test_record_survives_api_restart(client, restart_api):
    data = unique_payload(5000, seed=112)
    baseline = _upload_sealed(client, data, 4096)
    candidate = _upload_sealed(client, data + unique_payload(250, seed=113), 1000)

    created = client.compare(baseline, candidate)
    assert created.status_code == 201
    body = created.json()
    assert body["conclusion"] == "length_differs"
    assert body["first_difference_offset"] == len(data)

    restart_api()

    assert _get(client, body["id"]) == body


# --------------------------------------------------------------------------- #
# Existing contracts stay compatible
# --------------------------------------------------------------------------- #
def test_comparison_has_no_side_effects_on_sessions(client):
    data = unique_payload(4000, seed=114)
    baseline = _upload_sealed(client, data, 500)
    candidate = _upload_sealed(client, data, 2000)

    r = client.compare(baseline, candidate)
    assert r.status_code == 201

    # Sessions are untouched: still sealed, still downloadable, and the
    # comparison wrote no audit events (each side keeps only its seal row).
    for sid in (baseline, candidate):
        assert client.session(sid).json()["status"] == "sealed"
        assert client.content(sid).content == data
        events = client.audit(sid).json()["events"]
        assert [e["event"] for e in events] == ["sealed"]

    # A fresh comparison of the same pair creates a new record rather than
    # mutating the previous one.
    again = client.compare(baseline, candidate)
    assert again.status_code == 201
    assert again.json()["id"] != r.json()["id"]
    assert _get(client, r.json()["id"]) == r.json()
