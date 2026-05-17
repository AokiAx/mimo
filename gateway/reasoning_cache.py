"""SQLite-backed reasoning cache with in-memory LRU fast path.

The gateway needs MiMo's hidden ``reasoning_content`` / Anthropic ``thinking``
text to survive across processes — without persistence, every gateway
restart leaves all in-flight conversations subject to upstream 400s because
the cache entry that would have re-hydrated the missing field is gone.

Design:

* **Two-layer**. Memory ``OrderedDict`` LRU (4096 entries) services hot
  lookups in ~1 µs. On miss it falls through to SQLite at
  ``data/reasoning_cache.db`` and rehydrates the entry into memory.
* **Async writes**. ``remember_reasoning`` returns immediately after
  updating memory; the SQLite write goes through a bounded
  ``queue.Queue`` drained by a single daemon thread that does batched
  ``INSERT OR REPLACE``. Request hot path never touches disk.
* **TTL**. 7 days (cf. previous 6 hours). Long-lived multi-day Claude Code
  sessions don't survive shorter windows. Expired rows are pruned on
  startup and opportunistically by the writer thread.
* **Backward compatible API**. ``remember_reasoning`` / ``lookup_reasoning``
  signatures unchanged so existing callers in ``openai_chat.py`` and
  ``anthropic_passthrough.py`` don't need to change.

Path is overridable via the ``MIMO_REASONING_CACHE_DB`` env var (used by
tests). ``reset_for_tests()`` tears down the writer thread + in-memory state
so test isolation works.
"""
from __future__ import annotations

import logging
import os
import queue
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable
from pathlib import Path

logger = logging.getLogger(__name__)

# ────────────── tunables ──────────────

_DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "reasoning_cache.db"
_MAX_ENTRIES = 4096           # memory LRU cap (preserved name for monkeypatch in tests)
_TTL_S = 7 * 24 * 3600        # 7 days
_WRITE_QUEUE_MAX = 1024
_WORKER_DRAIN_TIMEOUT_S = 1.0  # how long the worker waits for the first item

# ────────────── module state ──────────────

_lock = threading.Lock()
_by_tool_ids: OrderedDict[tuple[str, ...], tuple[str, float]] = OrderedDict()

_stats: dict[str, int] = {
    "stores": 0,
    "store_skips": 0,
    "hits": 0,           # total hits (memory + sqlite)
    "memory_hits": 0,
    "sqlite_hits": 0,
    "misses": 0,
    "expired": 0,        # cache had key but TTL lapsed (counted on read)
    "evictions": 0,      # LRU eviction count (memory size cap)
    "write_drops": 0,    # times we couldn't enqueue a persistent write
}

_writer_lock = threading.Lock()
_write_queue: queue.Queue | None = None  # type: ignore[type-arg]
_writer_thread: threading.Thread | None = None
_writer_stop = threading.Event()
_db_initialized = False


def _db_path() -> Path:
    raw = os.environ.get("MIMO_REASONING_CACHE_DB")
    return Path(raw) if raw else _DEFAULT_DB_PATH


# ────────────── init / shutdown ──────────────


def _ensure_initialized() -> None:
    """Idempotent: create DB, reload top-N entries, start writer thread."""
    global _db_initialized, _write_queue, _writer_thread
    if _db_initialized:
        return
    with _writer_lock:
        if _db_initialized:
            return

        path = _db_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS reasoning_cache ("
                "  tool_ids TEXT PRIMARY KEY,"
                "  reasoning TEXT NOT NULL,"
                "  expires_at REAL NOT NULL"
                ")"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reasoning_expire "
                "ON reasoning_cache(expires_at)"
            )
            now = time.time()
            conn.execute(
                "DELETE FROM reasoning_cache WHERE expires_at < ?", (now,)
            )
            conn.commit()

            # Reload most-recently-expiring rows into the memory LRU so hot
            # data is fast right after restart. SELECT ordering is by
            # remaining TTL — fresher rows fill the LRU first.
            cur = conn.execute(
                "SELECT tool_ids, reasoning, expires_at "
                "FROM reasoning_cache ORDER BY expires_at DESC LIMIT ?",
                (_MAX_ENTRIES,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        with _lock:
            for tool_ids_str, reasoning, expires_at in rows:
                key = tuple(tool_ids_str.split("|"))
                _by_tool_ids[key] = (reasoning, expires_at)
            # Rows were ordered by expires_at DESC; reverse the OrderedDict
            # iteration so the most-recently-expiring entries become the most
            # recently used (= last in LRU order). Tiny detail but it means
            # the first eviction is the row closest to expiry, not freshest.
            for key in list(_by_tool_ids.keys())[::-1]:
                _by_tool_ids.move_to_end(key)

        _writer_stop.clear()
        _write_queue = queue.Queue(maxsize=_WRITE_QUEUE_MAX)
        _writer_thread = threading.Thread(
            target=_writer_loop, name="mimo-reasoning-cache-writer",
            daemon=True,
        )
        _writer_thread.start()
        _db_initialized = True


def reset_for_tests() -> None:
    """Tear everything down so the next call starts fresh.

    Tests use this in fixtures between cases. Production never calls it.
    """
    global _db_initialized, _write_queue, _writer_thread
    with _writer_lock:
        if _writer_thread is not None and _writer_thread.is_alive():
            _writer_stop.set()
            try:
                if _write_queue is not None:
                    _write_queue.put_nowait(None)
            except queue.Full:
                pass
            _writer_thread.join(timeout=2.0)
        _db_initialized = False
        _write_queue = None
        _writer_thread = None
        _writer_stop.clear()
    with _lock:
        _by_tool_ids.clear()
        for k in _stats:
            _stats[k] = 0


# ────────────── writer thread ──────────────


def _writer_loop() -> None:
    """Background worker: pull entries off the queue and batch-write."""
    path = _db_path()
    conn = sqlite3.connect(str(path), check_same_thread=False)
    last_purge = time.time()
    try:
        while True:
            try:
                first = _write_queue.get(timeout=_WORKER_DRAIN_TIMEOUT_S)  # type: ignore[union-attr]
            except queue.Empty:
                if _writer_stop.is_set():
                    return
                # Opportunistic purge of expired rows.
                if time.time() - last_purge > 300:
                    _purge_expired(conn)
                    last_purge = time.time()
                continue
            if first is None:
                return

            batch = [first]
            # Drain any other pending items so we commit them in one
            # transaction — turns N requests into 1 fsync.
            while True:
                try:
                    nxt = _write_queue.get_nowait()  # type: ignore[union-attr]
                except queue.Empty:
                    break
                if nxt is None:
                    # Shutdown signal received mid-drain. Flush what we have
                    # before returning.
                    try:
                        conn.executemany(
                            "INSERT OR REPLACE INTO reasoning_cache "
                            "(tool_ids, reasoning, expires_at) "
                            "VALUES (?, ?, ?)",
                            batch,
                        )
                        conn.commit()
                    except sqlite3.DatabaseError as e:
                        logger.warning("reasoning_cache flush on shutdown failed: %s", e)
                    return
                batch.append(nxt)

            try:
                conn.executemany(
                    "INSERT OR REPLACE INTO reasoning_cache "
                    "(tool_ids, reasoning, expires_at) VALUES (?, ?, ?)",
                    batch,
                )
                conn.commit()
            except sqlite3.DatabaseError as e:
                # Persistence is best-effort — memory cache continues to
                # serve. We don't want to crash the worker on transient
                # disk issues.
                logger.warning("reasoning_cache write failed (%d items): %s",
                               len(batch), e)
    finally:
        conn.close()


def _purge_expired(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            "DELETE FROM reasoning_cache WHERE expires_at < ?", (time.time(),)
        )
        conn.commit()
    except sqlite3.DatabaseError:
        pass


# ────────────── key shape ──────────────


def _key(tool_call_ids: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(sorted(t for t in tool_call_ids if isinstance(t, str) and t))


def _key_str(key: tuple[str, ...]) -> str:
    return "|".join(key)


# ────────────── public API ──────────────


def remember_reasoning(
    reasoning_content: str | None,
    tool_call_ids: Iterable[str | None],
) -> None:
    """Remember reasoning keyed by the message's sorted tool-call ids.

    Empty/missing reasoning or missing tool ids are no-ops because they
    can't help any future lookup.
    """
    if not isinstance(reasoning_content, str) or not reasoning_content:
        with _lock:
            _stats["store_skips"] += 1
        return
    key = _key(tool_call_ids)
    if not key:
        with _lock:
            _stats["store_skips"] += 1
        return

    expires_at = time.time() + _TTL_S
    with _lock:
        _by_tool_ids[key] = (reasoning_content, expires_at)
        _by_tool_ids.move_to_end(key)
        while len(_by_tool_ids) > _MAX_ENTRIES:
            _by_tool_ids.popitem(last=False)
            _stats["evictions"] += 1
        _stats["stores"] += 1

    # Best-effort async persistence.
    try:
        _ensure_initialized()
    except OSError as e:
        # DB init failed (permissions, missing parent dir we can't create,
        # etc). Stay memory-only; surface in stats.
        logger.warning("reasoning_cache DB init failed: %s", e)
        with _lock:
            _stats["write_drops"] += 1
        return

    if _write_queue is None:
        return
    try:
        _write_queue.put_nowait((_key_str(key), reasoning_content, expires_at))
    except queue.Full:
        # Queue is full = writer thread can't keep up. The memory cache is
        # still updated; we just lose persistence for this one entry.
        with _lock:
            _stats["write_drops"] += 1


def lookup_reasoning(tool_call_ids: Iterable[str | None]) -> str | None:
    """Return cached reasoning if any (memory first, SQLite second)."""
    key = _key(tool_call_ids)
    if not key:
        with _lock:
            _stats["misses"] += 1
        return None
    now = time.time()

    # Memory layer.
    with _lock:
        item = _by_tool_ids.get(key)
        if item is not None:
            reasoning, expires_at = item
            if expires_at < now:
                _by_tool_ids.pop(key, None)
                _stats["expired"] += 1
                # Fall through to SQLite — could have a fresher row there
                # in theory, but our writer is the only source, so memory
                # and SQLite agree. Bail with miss.
                _stats["misses"] += 1
                return None
            _by_tool_ids.move_to_end(key)
            _stats["hits"] += 1
            _stats["memory_hits"] += 1
            return reasoning

    # SQLite fallback. Note: we open a fresh connection on the read path
    # rather than sharing one — sqlite3 connections aren't safe to use
    # concurrently from multiple threads even with check_same_thread=False
    # (the cursor's row buffer races). A short-lived read connection costs
    # microseconds and avoids the locking headache.
    try:
        _ensure_initialized()
    except OSError:
        with _lock:
            _stats["misses"] += 1
        return None

    try:
        conn = sqlite3.connect(str(_db_path()), check_same_thread=False)
        try:
            cur = conn.execute(
                "SELECT reasoning, expires_at FROM reasoning_cache "
                "WHERE tool_ids = ?",
                (_key_str(key),),
            )
            row = cur.fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError as e:
        logger.warning("reasoning_cache SQLite read failed: %s", e)
        with _lock:
            _stats["misses"] += 1
        return None

    if row is None:
        with _lock:
            _stats["misses"] += 1
        return None

    reasoning, expires_at = row
    if expires_at < now:
        with _lock:
            _stats["expired"] += 1
            _stats["misses"] += 1
        return None

    # Rehydrate the memory layer so the next lookup is fast.
    with _lock:
        _by_tool_ids[key] = (reasoning, expires_at)
        _by_tool_ids.move_to_end(key)
        while len(_by_tool_ids) > _MAX_ENTRIES:
            _by_tool_ids.popitem(last=False)
            _stats["evictions"] += 1
        _stats["hits"] += 1
        _stats["sqlite_hits"] += 1
    return reasoning


def get_cache_stats() -> dict[str, int]:
    """Return a snapshot of cache counters + current memory size."""
    with _lock:
        snapshot = dict(_stats)
        snapshot["size"] = len(_by_tool_ids)
    return snapshot


def clear_reasoning_cache() -> None:
    """Clear both memory and SQLite. Used by tests and operational escape hatch."""
    with _lock:
        _by_tool_ids.clear()
        for k in _stats:
            _stats[k] = 0
    if not _db_initialized:
        return
    try:
        conn = sqlite3.connect(str(_db_path()), check_same_thread=False)
        try:
            conn.execute("DELETE FROM reasoning_cache")
            conn.commit()
        finally:
            conn.close()
    except sqlite3.DatabaseError as e:
        logger.warning("reasoning_cache clear failed: %s", e)


def flush(timeout_s: float = 2.0) -> None:
    """Wait for the writer queue to drain. Used by tests and graceful shutdown."""
    if _write_queue is None:
        return
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and not _write_queue.empty():
        time.sleep(0.01)
