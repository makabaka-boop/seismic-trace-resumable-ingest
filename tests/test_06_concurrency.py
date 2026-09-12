"""Concurrent chunk delivery must not corrupt the confirmed frontier."""
import concurrent.futures

import pytest

from conftest import unique_payload


@pytest.mark.parametrize("workers", [8])
def test_parallel_in_order_chunks_seal_exact_bytes(client, workers):
    data = unique_payload(20000, seed=40)
    s = client.create_session(data)
    sid = s["id"]

    # 100-byte pieces submitted in order but dispatched to a thread pool;
    # the server must serialize them per session.
    pieces = [data[i : i + 100] for i in range(0, len(data), 100)]

    accepted, rejected = 0, 0
    offsets = [sum(len(p) for p in pieces[:i]) for i in range(len(pieces))]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(client.put_chunk, sid, off, piece)
            for off, piece in zip(offsets, pieces)
        ]
        # Collect raced pieces, then re-deliver strictly in offset order:
        # each one is either the new frontier append or, after sealing, a
        # byte-identical idempotent replay.
        raced: list[tuple[int, bytes]] = []
        for fut, off, piece in zip(futures, offsets, pieces):
            r = fut.result()
            if r.status_code == 200:
                accepted += 1
            elif r.status_code == 409:
                assert r.json()["error"]["code"] == "stale_offset"
                rejected += 1
                raced.append((off, piece))
            else:
                pytest.fail(f"unexpected status {r.status_code}: {r.text}")

        for off, piece in sorted(raced):
            rr = client.put_chunk(sid, off, piece)
            assert rr.status_code == 200, rr.text

    info = client.session(sid).json()
    assert info["confirmed_offset"] == len(data)
    assert info["status"] == "sealed", info
    assert accepted + rejected == len(pieces)
    assert client.content(sid).content == data
