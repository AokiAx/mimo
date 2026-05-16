"""Best-effort in-memory cache for MiMo ``reasoning_content`` round-trips.

Some OpenAI-compatible clients do not persist unknown assistant fields. MiMo now
requires assistant ``reasoning_content`` to be returned in later tool-call turns
when thinking mode is enabled. This cache lets the gateway rehydrate missing
reasoning for follow-up requests that pass through the same process.

Tracks lifetime hit/miss/store counts so operators can observe whether the
cache is actually doing useful work — exposed via :func:`get_cache_stats`.
"""
from __future__ import annotations

import time
import threading
from collections import OrderedDict
from collections.abc import Iterable

_MAX_ENTRIES = 4096
_TTL_S = 6 * 3600

_lock = threading.Lock()
# key: sorted tuple of tool call ids, value: (reasoning_content, expires_at)
_by_tool_ids: OrderedDict[tuple[str, ...], tuple[str, float]] = OrderedDict()

# Lifetime counters. Reset only by clear_reasoning_cache().
_stats = {
    "stores": 0,         # remember_reasoning calls that wrote something
    "store_skips": 0,    # remember_reasoning calls dropped (empty/no ids)
    "hits": 0,
    "misses": 0,
    "expired": 0,        # cache had the key but TTL already lapsed
}


def _key(tool_call_ids: Iterable[str | None]) -> tuple[str, ...]:
    return tuple(sorted(t for t in tool_call_ids if isinstance(t, str) and t))


def remember_reasoning(reasoning_content: str | None, tool_call_ids: Iterable[str | None]) -> None:
    """Remember reasoning for a set of tool-call ids.

    Empty/missing reasoning or missing tool ids are ignored because they cannot
    improve future requests.
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
        _stats["stores"] += 1


def lookup_reasoning(tool_call_ids: Iterable[str | None]) -> str | None:
    key = _key(tool_call_ids)
    if not key:
        with _lock:
            _stats["misses"] += 1
        return None
    now = time.time()
    with _lock:
        item = _by_tool_ids.get(key)
        if item is None:
            _stats["misses"] += 1
            return None
        reasoning, expires_at = item
        if expires_at < now:
            _by_tool_ids.pop(key, None)
            _stats["expired"] += 1
            return None
        _by_tool_ids.move_to_end(key)
        _stats["hits"] += 1
        return reasoning


def get_cache_stats() -> dict[str, int]:
    """Return a snapshot of cache counters and current size."""
    with _lock:
        snapshot = dict(_stats)
        snapshot["size"] = len(_by_tool_ids)
        return snapshot


def clear_reasoning_cache() -> None:
    """Test helper / operational escape hatch. Also resets counters."""
    with _lock:
        _by_tool_ids.clear()
        for k in _stats:
            _stats[k] = 0
