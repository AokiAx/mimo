"""Tests specific to the SQLite-backed reasoning cache.

Memory-only behavior (hits / misses / evictions / stats) is covered in
``test_anthropic_passthrough.py``. This file focuses on persistence:
durability across resets, TTL pruning, queue backpressure, and the
memory-then-SQLite read path.

Each test gets an isolated ``MIMO_REASONING_CACHE_DB`` via ``conftest.py``,
and the autouse fixture there calls ``reset_for_tests`` before/after.
"""
from __future__ import annotations

import os
import sqlite3
import time

import pytest

from gateway.reasoning_cache import (
    flush,
    get_cache_stats,
    lookup_reasoning,
    remember_reasoning,
    reset_for_tests,
)


# ───────── helpers ─────────


def _wait_for_flush(timeout: float = 3.0) -> None:
    """Block until the writer thread has drained the queue."""
    flush(timeout)


def _db_path() -> str:
    """Return the env-configured DB path (set by conftest)."""
    return os.environ["MIMO_REASONING_CACHE_DB"]


def _sqlite_count() -> int:
    """How many rows live in the on-disk table right now."""
    if not os.path.exists(_db_path()):
        return 0
    conn = sqlite3.connect(_db_path())
    try:
        return conn.execute("SELECT COUNT(*) FROM reasoning_cache").fetchone()[0]
    finally:
        conn.close()


def _sqlite_fetch(tool_ids: list[str]) -> tuple[str, float] | None:
    conn = sqlite3.connect(_db_path())
    try:
        row = conn.execute(
            "SELECT reasoning, expires_at FROM reasoning_cache WHERE tool_ids = ?",
            ("|".join(sorted(tool_ids)),),
        ).fetchone()
        return row
    finally:
        conn.close()


# ───────── persistence durability ─────────


def test_write_lands_in_sqlite_after_flush():
    remember_reasoning("persisted reasoning", ["toolu_a", "toolu_b"])
    _wait_for_flush()
    row = _sqlite_fetch(["toolu_a", "toolu_b"])
    assert row is not None
    assert row[0] == "persisted reasoning"
    assert row[1] > time.time()  # expires_at in the future


def test_lookup_after_memory_evict_falls_back_to_sqlite():
    """Simulate the eviction case: store → drop from memory → look up.

    Production trigger for this code path is the LRU dropping cold entries
    when newer reasoning crowds them out. We force it by calling
    ``reset_for_tests`` (which wipes memory but the SQLite file remains
    because ``conftest`` keeps the env var pointing at the same path).
    """
    remember_reasoning("disk-only after restart", ["toolu_durable"])
    _wait_for_flush()
    assert _sqlite_count() == 1

    # Wipe memory + writer thread, keep DB file.
    reset_for_tests()

    # First lookup must miss memory and hit SQLite.
    result = lookup_reasoning(["toolu_durable"])
    assert result == "disk-only after restart"

    stats = get_cache_stats()
    assert stats["sqlite_hits"] == 1
    assert stats["memory_hits"] == 0


def test_sqlite_hit_rehydrates_memory():
    """After a SQLite-hit lookup the entry should live in memory too, so a
    second lookup is the fast path."""
    remember_reasoning("hot data", ["toolu_x"])
    _wait_for_flush()
    reset_for_tests()

    # SQLite hit (cold).
    assert lookup_reasoning(["toolu_x"]) == "hot data"
    s = get_cache_stats()
    assert s["sqlite_hits"] == 1
    assert s["size"] == 1   # rehydrated

    # Memory hit (hot).
    assert lookup_reasoning(["toolu_x"]) == "hot data"
    s = get_cache_stats()
    assert s["memory_hits"] == 1


def test_simulated_gateway_restart_reloads_top_entries(monkeypatch):
    """The init code should pull surviving entries back into the memory LRU
    so a fresh process is immediately warm (modulo cap)."""
    # Write a few entries.
    for i in range(5):
        remember_reasoning(f"r{i}", [f"toolu_{i}"])
    _wait_for_flush()
    assert _sqlite_count() == 5

    # Simulate process death + restart.
    reset_for_tests()
    # Triggering the next remember_reasoning runs _ensure_initialized,
    # which is where the reload happens. Use lookup so we don't also add a
    # 6th entry.
    lookup_reasoning(["toolu_0"])
    stats = get_cache_stats()
    # After init reload, memory should hold all 5 entries.
    assert stats["size"] == 5


# ───────── TTL ─────────


def test_expired_sqlite_entry_treated_as_miss():
    """A row whose expires_at is in the past should miss + bump the
    expired counter. Doesn't go through reset_for_tests because that would
    trigger init-time purge and we want to exercise the per-lookup TTL
    check on the SQLite read path."""
    import gateway.reasoning_cache as rc

    remember_reasoning("about to expire", ["toolu_dead"])
    _wait_for_flush()

    # Drop from memory ONLY (don't reset writer thread / re-init DB —
    # that would also purge the row server-side).
    with rc._lock:
        rc._by_tool_ids.clear()

    # Backdate the SQLite row directly.
    conn = sqlite3.connect(_db_path())
    conn.execute("UPDATE reasoning_cache SET expires_at = ? WHERE tool_ids = ?",
                 (time.time() - 60, "toolu_dead"))
    conn.commit()
    conn.close()

    # Now lookup: memory miss → SQLite read returns row → row is expired →
    # bumps expired + misses, returns None.
    assert lookup_reasoning(["toolu_dead"]) is None
    stats = get_cache_stats()
    assert stats["expired"] >= 1
    assert stats["misses"] >= 1


def test_init_purges_expired_rows():
    """On startup, the table should have its expired rows wiped — keeps the
    DB file from accumulating dead state over time."""
    remember_reasoning("live", ["toolu_live"])
    remember_reasoning("stale", ["toolu_stale"])
    _wait_for_flush()

    conn = sqlite3.connect(_db_path())
    conn.execute("UPDATE reasoning_cache SET expires_at = ? WHERE tool_ids = ?",
                 (time.time() - 1, "toolu_stale"))
    conn.commit()
    conn.close()

    # Init runs purge during _ensure_initialized → reset + re-init.
    reset_for_tests()
    # Force re-init via any call.
    lookup_reasoning(["toolu_live"])

    assert _sqlite_count() == 1
    assert _sqlite_fetch(["toolu_live"]) is not None
    assert _sqlite_fetch(["toolu_stale"]) is None


# ───────── memory cache still works for the in-process round-trip ─────────


def test_remember_lookup_round_trip_without_touching_sqlite():
    """The hot path: write then immediately read inside the same process.
    Should be memory hit, no SQLite read involved."""
    remember_reasoning("fast path", ["toolu_z"])

    # Look up BEFORE the writer thread necessarily flushed — memory layer
    # is updated synchronously inside remember_reasoning.
    assert lookup_reasoning(["toolu_z"]) == "fast path"
    stats = get_cache_stats()
    assert stats["memory_hits"] == 1
    assert stats["sqlite_hits"] == 0


def test_lookup_with_no_tool_ids_is_miss():
    """Empty / all-invalid id iterables short-circuit to miss."""
    assert lookup_reasoning([]) is None
    assert lookup_reasoning([None]) is None
    assert lookup_reasoning(["", "   "]) is None
    stats = get_cache_stats()
    assert stats["misses"] >= 1


def test_remember_with_empty_reasoning_is_skip():
    """We deliberately don't persist empty strings — they would 400 if
    rehydrated. Track as store_skip."""
    remember_reasoning("", ["toolu_e"])
    remember_reasoning(None, ["toolu_e"])  # type: ignore[arg-type]

    stats = get_cache_stats()
    assert stats["stores"] == 0
    assert stats["store_skips"] >= 2
    assert _sqlite_count() == 0


def test_text_fallback_keys_text_only_thinking_responses():
    """Text-only thinking responses have no tool ids. The ``text=`` kwarg
    lets us key on a hash of the assistant text so the same conversation
    can rehydrate reasoning_content on the next turn."""
    remember_reasoning(
        "deep thought here",
        [],                                  # no tool calls
        text="The answer is 42.",
    )
    # Same text → cache hit.
    assert lookup_reasoning([], text="The answer is 42.") == "deep thought here"


def test_text_fallback_misses_on_text_change():
    remember_reasoning("orig", [], text="hello world")
    assert lookup_reasoning([], text="hello earth") is None


def test_text_fallback_does_not_collide_with_tool_id_keys():
    """Sentinel-prefixed text keys must not collide with single-tool-id keys."""
    remember_reasoning("via tool id", ["toolu_x"])
    remember_reasoning("via text hash", [], text="some response")
    assert lookup_reasoning(["toolu_x"]) == "via tool id"
    assert lookup_reasoning([], text="some response") == "via text hash"


def test_remember_prefers_tool_id_key_when_both_provided():
    """When both tool ids and text are supplied, tool-id keying wins —
    that's the primary, hash is the fallback."""
    remember_reasoning("real reasoning", ["toolu_y"], text="ignored text")
    # Lookup by tool id hits.
    assert lookup_reasoning(["toolu_y"]) == "real reasoning"
    # Lookup by text alone misses — we didn't write under the text key.
    assert lookup_reasoning([], text="ignored text") is None


# ───────── writer thread behavior ─────────


def test_concurrent_writes_serialize_through_queue():
    """Stress the writer queue with many concurrent stores. None should be
    lost as long as we stay within the queue size limit (1024 default)."""
    import threading
    barrier = threading.Barrier(8)

    def writer(i: int) -> None:
        barrier.wait()
        for j in range(20):
            remember_reasoning(f"writer{i}-job{j}", [f"toolu_{i}_{j}"])

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    _wait_for_flush(timeout=5.0)
    assert _sqlite_count() == 8 * 20


def test_overflow_falls_back_to_memory_only(monkeypatch):
    """If the write queue is saturated, the entry still lives in memory; we
    just record the drop in stats. We force the condition by shrinking the
    queue and never starting the worker."""
    # Simulate: queue is full → put_nowait raises. To trigger, monkeypatch
    # the queue to a 0-size one after init.
    import gateway.reasoning_cache as rc

    # Force init to set up the queue.
    remember_reasoning("seed", ["toolu_seed"])

    # Replace the queue with a "full" one — its put_nowait will always
    # raise queue.Full.
    import queue as _q
    full_queue: _q.Queue = _q.Queue(maxsize=1)
    full_queue.put_nowait(("filler", "", 0))
    monkeypatch.setattr(rc, "_write_queue", full_queue)

    remember_reasoning("dropped from disk", ["toolu_overflow"])

    # Memory still has it.
    assert lookup_reasoning(["toolu_overflow"]) == "dropped from disk"
    # write_drops should bump.
    assert get_cache_stats()["write_drops"] >= 1


# ───────── PR #25 review regressions ─────────


def test_init_reload_does_not_clobber_fresh_memory_write():
    """PR #25 Codex P1 #1.

    First remember after restart: SQLite has an older value for the same
    tool_ids, then remember_reasoning writes a fresher value to memory and
    triggers _ensure_initialized → reload. The reload must NOT overwrite
    the freshly-written value with the stale on-disk one.

    Repro: seed SQLite with an old row, reset memory (keep DB), call
    remember_reasoning with a new value, then lookup — should return new.
    """
    # Phase 1: write old value, flush to SQLite, simulate process restart
    # by clearing memory + the writer thread (DB file survives).
    remember_reasoning("OLD reasoning", ["toolu_clobber"])
    _wait_for_flush()
    reset_for_tests()

    # Phase 2: fresh process writes a new value for the same key.
    remember_reasoning("NEW reasoning", ["toolu_clobber"])

    # Phase 3: lookup — must see the new value, not the old one reloaded
    # from disk during init.
    assert lookup_reasoning(["toolu_clobber"]) == "NEW reasoning"


def test_init_reload_still_brings_in_unrelated_rows():
    """The setdefault fix must not break the legitimate reload case:
    rows whose keys aren't currently in memory should still come back."""
    remember_reasoning("disk-only row A", ["toolu_disk_a"])
    remember_reasoning("disk-only row B", ["toolu_disk_b"])
    _wait_for_flush()
    reset_for_tests()

    # Write a third, fresh value — its init reload should NOT touch this,
    # but SHOULD still load A and B.
    remember_reasoning("fresh row C", ["toolu_fresh_c"])

    assert lookup_reasoning(["toolu_disk_a"]) == "disk-only row A"
    assert lookup_reasoning(["toolu_disk_b"]) == "disk-only row B"
    assert lookup_reasoning(["toolu_fresh_c"]) == "fresh row C"


def test_remember_swallows_sqlite_init_corruption(monkeypatch, tmp_path):
    """PR #25 Codex P1 #2.

    A corrupted DB file makes sqlite3.connect / execute raise
    DatabaseError, not OSError. remember_reasoning must catch it and
    continue memory-only, not propagate the exception out into the request
    pipeline.
    """
    # Write garbage to where the cache DB is expected.
    db = tmp_path / "corrupt_cache.db"
    db.write_bytes(b"this is not a valid sqlite database file at all")
    monkeypatch.setenv("MIMO_REASONING_CACHE_DB", str(db))
    reset_for_tests()

    # Must not raise.
    remember_reasoning("survives corruption", ["toolu_corrupt"])

    # Memory layer still served the write — lookup hits memory.
    assert lookup_reasoning(["toolu_corrupt"]) == "survives corruption"

    # Stats reflect the failed persistence attempt.
    stats = get_cache_stats()
    assert stats["write_drops"] >= 1


def test_lookup_swallows_sqlite_init_corruption(monkeypatch, tmp_path):
    """Same fail-open semantics for the read path: a corrupt DB must
    cause a miss, not raise."""
    db = tmp_path / "corrupt_cache.db"
    db.write_bytes(b"\x00garbage data\xff")
    monkeypatch.setenv("MIMO_REASONING_CACHE_DB", str(db))
    reset_for_tests()

    # Must not raise; returns None (miss).
    assert lookup_reasoning(["toolu_anything"]) is None
    assert get_cache_stats()["misses"] >= 1


def test_clear_wipes_disk_even_on_cold_start():
    """PR #25 Codex P2 #3.

    Operator scenario: fresh process, never called remember/lookup, has
    rows persisted on disk from previous run. Calling clear_reasoning_cache
    must delete those disk rows (previously short-circuited and returned
    immediately because _db_initialized was False).
    """
    # Phase 1: seed DB and persist.
    remember_reasoning("survive me not", ["toolu_clear_test"])
    _wait_for_flush()
    assert _sqlite_count() == 1

    # Phase 2: full restart — db file survives, in-memory state is wiped,
    # _db_initialized is False.
    reset_for_tests()
    import gateway.reasoning_cache as rc
    assert rc._db_initialized is False

    # Phase 3: call clear before any other cache call.
    from gateway.reasoning_cache import clear_reasoning_cache
    clear_reasoning_cache()

    # Disk row gone.
    assert _sqlite_count() == 0

    # Belt-and-braces: subsequent lookup must also miss (no zombie reload).
    assert lookup_reasoning(["toolu_clear_test"]) is None


def test_clear_on_missing_db_does_not_create_empty_file(tmp_path, monkeypatch):
    """Side check: clear shouldn't create the DB file just to delete from
    it. A fresh deployment with no cache history should leave the
    filesystem untouched."""
    db = tmp_path / "never_used.db"
    monkeypatch.setenv("MIMO_REASONING_CACHE_DB", str(db))
    reset_for_tests()
    assert not db.exists()

    from gateway.reasoning_cache import clear_reasoning_cache
    clear_reasoning_cache()

    assert not db.exists(), "clear() should not create an empty DB file"
