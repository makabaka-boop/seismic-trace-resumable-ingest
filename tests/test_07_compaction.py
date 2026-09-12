"""Sealed-archive compaction acceptance tests.

A sealed package originally uploaded as many small chunks is repacked into
fewer, contiguous chunks without changing its identity: the session id,
total byte count and whole SHA-256 stay the same and downloaded bytes are
byte-identical.  Repeating a compaction with the same target must reproduce
the very same layout, and non-sealed requests must have no side effects.
"""
import threading

from conftest import sha, unique_payload


def _upload_sealed(client, data, piece):
    s = client.create_session(data)
    sid = s["id"]
    for off in range(0, len(data), piece):
        r = client.put_chunk(sid, off, data[off : off + piece])
        assert r.status_code == 200, r.text
    assert client.session(sid).json()["status"] == "sealed"
    return sid


def _layout(chunks_payload):
    return [
        (c["start_offset"], c["end_offset"], c["length"], c["sha256"])
        for c in chunks_payload["chunks"]
    ]


def test_compaction_reduces_rows_and_preserves_bytes(client, pg_conn):
    # 10000 bytes sealed as fifty 200-byte chunks.
    data = unique_payload(10000, seed=50)
    sid = _upload_sealed(client, data, 200)

    before = client.chunks(sid).json()
    assert len(before["chunks"]) == 50
    assert [(c["start_offset"], c["end_offset"]) for c in before["chunks"]] == [
        (i * 200, min((i + 1) * 200, 10000)) for i in range(50)
    ]

    r = client.compact(sid, 4096)
    assert r.status_code == 200, r.text
    body = r.json()

    # Before/after counters and identity fields.
    assert body["id"] == sid
    assert body["status"] == "sealed"
    assert body["target_chunk_bytes"] == 4096
    assert body["chunks_before"] == 50
    assert body["chunks_after"] == 3
    assert body["chunks_after"] < body["chunks_before"]
    assert body["total_bytes_before"] == 10000
    assert body["total_bytes_after"] == 10000
    assert body["whole_sha256"] == sha(data)

    # New layout is contiguous and respects the target ceiling.
    spans = [(c["start_offset"], c["end_offset"], c["length"]) for c in body["chunks"]]
    assert spans == [(0, 4096, 4096), (4096, 8192, 4096), (8192, 10000, 1808)]
    cursor = 0
    for start, end, length in spans:
        assert start == cursor
        assert length == end - start
        assert length <= 4096
        cursor = end
    assert cursor == 10000

    # Reported chunk digests really describe the new spans.
    for c in body["chunks"]:
        assert c["sha256"] == sha(data[c["start_offset"] : c["end_offset"]])

    # The chunks endpoint exposes exactly the same new layout.
    assert _layout(client.chunks(sid).json()) == _layout(body)

    # Fewer physical rows in PostgreSQL.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM chunks WHERE session_id = %s", (sid,))
        assert cur.fetchone()[0] == 3
        cur.execute(
            "SELECT octet_length(data), sha256 FROM chunks "
            "WHERE session_id = %s ORDER BY start_offset",
            (sid,),
        )
        rows = cur.fetchall()
    assert [row[0] for row in rows] == [4096, 4096, 1808]
    assert [row[1] for row in rows] == [c[3] for c in _layout(body)]

    # Identity untouched, downloaded bytes and digest unchanged.
    info = client.session(sid).json()
    assert info["status"] == "sealed"
    assert info["total_bytes"] == 10000
    assert info["confirmed_offset"] == 10000
    assert info["whole_sha256"] == sha(data)

    got = client.content(sid)
    assert got.status_code == 200
    assert got.content == data
    assert got.headers["X-Whole-SHA256"] == sha(data)
    assert got.headers["ETag"] == f'"{sha(data)}"'


def test_repeated_compaction_with_same_target_is_identical(client):
    data = unique_payload(7000, seed=51)
    sid = _upload_sealed(client, data, 137)
    original_rows = len(client.chunks(sid).json()["chunks"])

    first = client.compact(sid, 1000)
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["chunks_before"] == original_rows
    first_layout = _layout(first_body)

    # Same target again: same layout and same per-chunk digests.
    second = client.compact(sid, 1000)
    assert second.status_code == 200, second.text
    second_body = second.json()
    assert second_body["chunks_before"] == second_body["chunks_after"] == len(
        first_layout
    )
    assert _layout(second_body) == first_layout
    assert second_body["whole_sha256"] == first_body["whole_sha256"] == sha(data)

    # And again: the operation is a stable idempotent projection.
    third = client.compact(sid, 1000)
    assert third.status_code == 200
    assert _layout(third.json()) == first_layout

    # A different target deterministically re-splits the same bytes.
    other = client.compact(sid, 3000)
    assert other.status_code == 200
    other_body = other.json()
    assert [(c["start_offset"], c["end_offset"]) for c in other_body["chunks"]] == [
        (0, 3000),
        (3000, 6000),
        (6000, 7000),
    ]
    # The original target is still reproducible after the intermediate split.
    again = client.compact(sid, 1000)
    assert _layout(again.json()) == first_layout

    assert client.content(sid).content == data


def test_non_sealed_requests_conflict_without_side_effects(client, pg_conn):
    # An active session: conflict and no rows/layout changes.
    data = unique_payload(3000, seed=52)
    active = client.create_session(data)
    aid = active["id"]
    client.put_chunk(aid, 0, data[:1200])
    active_layout_before = _layout(client.chunks(aid).json())

    r = client.compact(aid, 1000)
    assert r.status_code == 409, r.text
    err = r.json()["error"]
    assert err["code"] == "compaction_state_conflict"
    assert err["location"]["expected_offset"] == 1200
    assert err["details"]["status"] == "active"

    assert client.session(aid).json()["status"] == "active"
    assert _layout(client.chunks(aid).json()) == active_layout_before

    # A failed session (wrong whole digest) likewise.
    bad = client.create_session_raw(len(data), "f" * 64).json()
    bid = bad["id"]
    fr = client.put_chunk(bid, 0, data)
    assert fr.status_code == 422
    failed_layout_before = _layout(client.chunks(bid).json())

    r = client.compact(bid, 1000)
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "compaction_state_conflict"
    assert err["details"]["status"] == "failed"
    assert client.session(bid).json()["status"] == "failed"
    assert _layout(client.chunks(bid).json()) == failed_layout_before

    # Neither session's chunk rows were touched in PostgreSQL.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM chunks WHERE session_id = %s", (aid,))
        assert cur.fetchone()[0] == len(active_layout_before)
        cur.execute("SELECT count(*) FROM chunks WHERE session_id = %s", (bid,))
        assert cur.fetchone()[0] == len(failed_layout_before)
        cur.execute(
            "SELECT status, confirmed_offset FROM upload_sessions WHERE id = %s",
            (aid,),
        )
        assert cur.fetchone() == ("active", 1200)

    # Invalid targets are parameter errors and also side-effect free.
    sealed_sid = _upload_sealed(client, unique_payload(500, seed=53), 50)
    layout_before = _layout(client.chunks(sealed_sid).json())
    for bad_target in (0, -1):
        rr = client.compact(sealed_sid, bad_target)
        assert rr.status_code == 422, rr.text
        body = rr.json()
        assert body["error"]["code"] == "validation_error"
        fields = [f["field"] for f in body["error"]["details"]["fields"]]
        assert "target_chunk_bytes" in fields
    # Non-integer target must not be silently accepted.
    rr = client.compact(sealed_sid, "not-an-int")
    assert rr.status_code == 422
    assert _layout(client.chunks(sealed_sid).json()) == layout_before
    assert client.content(sealed_sid).status_code == 200

    # Unknown session.
    assert client.compact("unknown-session-id", 1000).status_code == 404


def test_empty_sealed_package_compacts_to_zero_chunks(client):
    import hashlib

    empty_hash = hashlib.sha256(b"").hexdigest()
    sid = client.create_session_raw(0, empty_hash).json()["id"]

    r = client.compact(sid, 4096)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "sealed"
    assert body["chunks_before"] == 0
    assert body["chunks_after"] == 0
    assert body["total_bytes_before"] == 0
    assert body["total_bytes_after"] == 0
    assert body["whole_sha256"] == empty_hash
    assert body["chunks"] == []

    # Download contract still serves the empty package.
    got = client.content(sid)
    assert got.status_code == 200
    assert got.content == b""


def test_concurrent_readers_see_only_complete_layouts(client):
    # While compactions alternate between two targets, every chunks/content
    # read must observe a fully consistent pre- or post-compaction layout.
    data = unique_payload(20000, seed=54)
    sid = _upload_sealed(client, data, 64)

    stop = threading.Event()
    violations: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            listing = client.chunks(sid).json()
            chunks = listing["chunks"]
            cursor = 0
            for c in chunks:
                if c["start_offset"] != cursor:
                    violations.append(f"gap at {cursor}")
                    return
                if c["length"] != c["end_offset"] - c["start_offset"]:
                    violations.append("inconsistent length")
                    return
                cursor = c["end_offset"]
            if cursor != 20000:
                violations.append(f"short layout ending at {cursor}")
                return
            got = client.content(sid)
            if got.status_code != 200 or got.content != data:
                violations.append("content read inconsistent")
                return

    worker = threading.Thread(target=reader)
    worker.start()
    try:
        for i in range(12):
            target = 7000 if i % 2 == 0 else 2048
            rr = client.compact(sid, target)
            assert rr.status_code == 200, rr.text
            for c in rr.json()["chunks"]:
                assert c["length"] <= target
    finally:
        stop.set()
        worker.join()

    assert violations == []
    assert client.content(sid).content == data
