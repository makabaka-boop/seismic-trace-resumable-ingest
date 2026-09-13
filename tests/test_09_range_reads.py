"""Single-range HTTP reads of sealed archives.

Field engineers verifying a large sealed record can fetch exactly the byte
window they need from the existing content URL instead of downloading the
whole package:

* ``Range: bytes=a-b`` (closed), ``bytes=a-`` (open-ended) and ``bytes=-n``
  (suffix) are resolved against the sealed package length and answered with
  206 carrying ``Content-Range``/``Content-Length``/``Accept-Ranges`` plus
  the original ``ETag`` and whole-digest headers, extracted across
  persisted chunk boundaries;
* every read re-verifies chunk continuity, per-chunk lengths and digests
  and the whole-package digest before releasing the fragment, so different
  chunk layouts (e.g. before/after compaction) answer the same range
  byte-identically;
* malformed or multi-range headers get a 400 locating the Range; ranges
  beyond the package — and any range on a zero-byte package — get 416 with
  ``Content-Range: bytes */<total>``; no read writes data or audit rows;
* requests without a Range header keep the original 200 full-download
  contract, and active/failed sessions keep their 409 state conflict.
"""
import hashlib

from conftest import sha, unique_payload


def _upload_sealed(client, data, piece):
    s = client.create_session(data)
    sid = s["id"]
    for off in range(0, len(data), piece):
        r = client.put_chunk(sid, off, data[off : off + piece])
        assert r.status_code == 200, r.text
    assert client.session(sid).json()["status"] == "sealed"
    return sid


def _assert_range_headers(resp, begin, end_inclusive, total, whole):
    assert resp.headers["Content-Range"] == (
        f"bytes {begin}-{end_inclusive}/{total}"
    )
    assert resp.headers["Content-Length"] == str(end_inclusive - begin + 1)
    assert resp.headers["Accept-Ranges"] == "bytes"
    assert resp.headers["ETag"] == f'"{whole}"'
    assert resp.headers["X-Whole-SHA256"] == whole


# --------------------------------------------------------------------------- #
# Closed / open-ended / suffix ranges across chunk boundaries
# --------------------------------------------------------------------------- #
def test_closed_range_extracts_bytes_across_chunk_boundaries(client):
    data = unique_payload(10000, seed=60)
    sid = _upload_sealed(client, data, 300)  # 34 persisted chunks
    whole = sha(data)

    # A window crossing several 300-byte chunk boundaries.
    r = client.content(sid, "bytes=250-929")
    assert r.status_code == 206, r.text
    assert r.content == data[250:930]
    _assert_range_headers(r, 250, 929, 10000, whole)

    # Single-byte and chunk-aligned windows.
    r = client.content(sid, "bytes=0-0")
    assert r.status_code == 206
    assert r.content == data[0:1]
    _assert_range_headers(r, 0, 0, 10000, whole)

    r = client.content(sid, "bytes=300-599")  # exactly one stored chunk
    assert r.status_code == 206
    assert r.content == data[300:600]
    _assert_range_headers(r, 300, 599, 10000, whole)

    r = client.content(sid, "bytes=9999-9999")
    assert r.status_code == 206
    assert r.content == data[9999:]
    _assert_range_headers(r, 9999, 9999, 10000, whole)

    # An end beyond the package is clamped to the last byte.
    r = client.content(sid, "bytes=9990-99999")
    assert r.status_code == 206
    assert r.content == data[9990:]
    _assert_range_headers(r, 9990, 9999, 10000, whole)


def test_open_ended_and_suffix_ranges(client):
    data = unique_payload(8000, seed=61)
    sid = _upload_sealed(client, data, 1000)
    whole = sha(data)

    r = client.content(sid, "bytes=7500-")
    assert r.status_code == 206, r.text
    assert r.content == data[7500:]
    _assert_range_headers(r, 7500, 7999, 8000, whole)

    r = client.content(sid, "bytes=-500")
    assert r.status_code == 206
    assert r.content == data[-500:]
    _assert_range_headers(r, 7500, 7999, 8000, whole)

    # A suffix longer than the package yields the whole package as 206.
    r = client.content(sid, "bytes=-99999")
    assert r.status_code == 206
    assert r.content == data
    _assert_range_headers(r, 0, 7999, 8000, whole)

    # An explicit whole-package range behaves the same way.
    r = client.content(sid, "bytes=0-")
    assert r.status_code == 206
    assert r.content == data
    _assert_range_headers(r, 0, 7999, 8000, whole)


def test_ranges_are_identical_before_and_after_compaction(client):
    data = unique_payload(12000, seed=62)
    sid = _upload_sealed(client, data, 137)  # irregular chunk boundaries

    ranges = [
        "bytes=0-4999",
        "bytes=4096-8191",
        "bytes=5000-",
        "bytes=-777",
        "bytes=11999-11999",
    ]

    def read_all():
        out = []
        for rv in ranges:
            r = client.content(sid, rv)
            assert r.status_code == 206, r.text
            out.append(
                (
                    r.content,
                    r.headers["Content-Range"],
                    r.headers["ETag"],
                    r.headers["X-Whole-SHA256"],
                )
            )
        return out

    before = read_all()
    r = client.compact(sid, 4096)
    assert r.status_code == 200, r.text
    after = read_all()

    # Different chunk boundaries must not change any range response.
    assert after == before
    assert before[0][0] == data[0:5000]
    assert before[1][0] == data[4096:8192]
    assert before[2][0] == data[5000:]
    assert before[3][0] == data[-777:]
    assert before[4][0] == data[-1:]

    # The full download is equally unaffected by the relayout.
    assert client.content(sid).content == data


# --------------------------------------------------------------------------- #
# Malformed / multi-range headers → 400 locating the Range
# --------------------------------------------------------------------------- #
def test_malformed_and_multi_range_headers_return_400(client):
    data = unique_payload(1000, seed=63)
    sid = _upload_sealed(client, data, 256)

    bad_headers = [
        "bananas",
        "items=0-10",
        "bytes=",
        "bytes=-",
        "bytes=abc-def",
        "bytes=1-2-3",
        "bytes=10-5",        # start after end
        "bytes=0-10,20-30",  # multi-range is not supported
        "bytes=0-10,",
    ]
    for bad in bad_headers:
        r = client.content(sid, bad)
        assert r.status_code == 400, (bad, r.status_code, r.text)
        err = r.json()["error"]
        assert err["code"] == "invalid_range"
        assert err["details"]["range"] == bad


# --------------------------------------------------------------------------- #
# Unsatisfiable ranges → 416 with Content-Range: bytes */<total>
# --------------------------------------------------------------------------- #
def test_unsatisfiable_ranges_return_416_with_total_length(client):
    data = unique_payload(1000, seed=64)
    sid = _upload_sealed(client, data, 256)

    for rv in ("bytes=1000-", "bytes=1000-2000", "bytes=5000-6000", "bytes=-0"):
        r = client.content(sid, rv)
        assert r.status_code == 416, (rv, r.status_code, r.text)
        assert r.headers["Content-Range"] == "bytes */1000"
        err = r.json()["error"]
        assert err["code"] == "range_not_satisfiable"
        assert err["details"]["range"] == rv
        assert err["details"]["total_bytes"] == 1000


def test_zero_byte_package_rejects_every_range_with_416(client):
    empty_hash = hashlib.sha256(b"").hexdigest()
    sid = client.create_session_raw(0, empty_hash).json()["id"]
    assert client.session(sid).json()["status"] == "sealed"

    for rv in ("bytes=0-0", "bytes=0-", "bytes=-1"):
        r = client.content(sid, rv)
        assert r.status_code == 416, (rv, r.status_code, r.text)
        assert r.headers["Content-Range"] == "bytes */0"
        assert r.json()["error"]["code"] == "range_not_satisfiable"

    # The zero-byte package itself still downloads normally.
    r = client.content(sid)
    assert r.status_code == 200
    assert r.content == b""
    assert r.headers["Content-Length"] == "0"


# --------------------------------------------------------------------------- #
# Reads are side-effect free: no audit rows, no data writes
# --------------------------------------------------------------------------- #
def test_range_reads_leave_no_audit_or_data_writes(client, pg_conn):
    data = unique_payload(5000, seed=65)
    sid = _upload_sealed(client, data, 400)
    layout_before = client.chunks(sid).json()["chunks"]
    events_before = client.audit(sid).json()["events"]
    assert [e["event"] for e in events_before] == ["sealed"]

    # Exercise every response class: 206, 200, 400 and 416.
    assert client.content(sid, "bytes=100-200").status_code == 206
    assert client.content(sid).status_code == 200
    assert client.content(sid, "bytes=oops").status_code == 400
    assert client.content(sid, "bytes=99999-").status_code == 416

    assert client.chunks(sid).json()["chunks"] == layout_before
    assert client.audit(sid).json()["events"] == events_before
    info = client.session(sid).json()
    assert info["status"] == "sealed"
    assert info["confirmed_offset"] == len(data)

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM audit_events WHERE session_id = %s", (sid,)
        )
        assert cur.fetchone()[0] == 1  # only the seal snapshot


# --------------------------------------------------------------------------- #
# Lifecycle states are untouched by the new header
# --------------------------------------------------------------------------- #
def test_active_and_failed_sessions_keep_state_conflict(client):
    data = unique_payload(2000, seed=66)

    active = client.create_session(data)
    aid = active["id"]
    assert client.put_chunk(aid, 0, data[:500]).status_code == 200
    r = client.content(aid, "bytes=0-100")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "not_sealed"

    bad = client.create_session_raw(len(data), "f" * 64).json()
    assert client.put_chunk(bad["id"], 0, data).status_code == 422
    assert client.session(bad["id"]).json()["status"] == "failed"
    r = client.content(bad["id"], "bytes=0-100")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "not_sealed"

    # Unknown sessions keep the standard 404, Range header or not.
    assert client.content("no-such-session", "bytes=0-1").status_code == 404


def test_full_download_contract_is_unchanged(client):
    data = unique_payload(4096, seed=67)
    sid = _upload_sealed(client, data, 1024)

    r = client.content(sid)  # no Range header
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["Content-Length"] == str(len(data))
    assert r.headers["ETag"] == f'"{sha(data)}"'
    assert r.headers["X-Whole-SHA256"] == sha(data)
    assert "Content-Range" not in r.headers


# --------------------------------------------------------------------------- #
# A fragment is only released once the whole archive verifies
# --------------------------------------------------------------------------- #
def test_range_read_reverifies_archive_integrity(client, pg_conn):
    data = unique_payload(6000, seed=68)
    sid = _upload_sealed(client, data, 1000)

    # Corrupt the first stored chunk in PostgreSQL, keeping its length.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT data FROM chunks WHERE session_id = %s AND start_offset = 0",
            (sid,),
        )
        original = bytes(cur.fetchone()[0])
        tampered = bytes([original[0] ^ 0xFF]) + original[1:]
        cur.execute(
            "UPDATE chunks SET data = %s "
            "WHERE session_id = %s AND start_offset = 0",
            (tampered, sid),
        )
    pg_conn.commit()

    try:
        # The requested window does not overlap the corrupted chunk, yet the
        # read must refuse: the archive as a whole no longer verifies.
        r = client.content(sid, "bytes=5000-5999")
        assert r.status_code == 500
        assert r.json()["error"]["code"] == "sealed_integrity_error"
        r = client.content(sid)
        assert r.status_code == 500
        assert r.json()["error"]["code"] == "sealed_integrity_error"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE chunks SET data = %s "
                "WHERE session_id = %s AND start_offset = 0",
                (original, sid),
            )
        pg_conn.commit()

    # Restored archive serves the same range again.
    r = client.content(sid, "bytes=5000-5999")
    assert r.status_code == 206
    assert r.content == data[5000:6000]


def test_range_read_detects_discontinuous_archive(client, pg_conn):
    data = unique_payload(3000, seed=69)
    sid = _upload_sealed(client, data, 1000)

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM chunks WHERE session_id = %s AND start_offset = 2000",
            (sid,),
        )
    pg_conn.commit()

    r = client.content(sid, "bytes=0-10")
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "sealed_integrity_error"
