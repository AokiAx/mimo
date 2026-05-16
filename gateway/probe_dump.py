"""Temporary diagnostic: dump gateway inbound / outbound bodies to a JSONL file.

Enable by setting ``MIMO_PROBE_DUMP=<path>`` in the gateway environment. When
unset, every entrypoint is a no-op (zero overhead). When set, each gateway
``/v1/*`` request appends one line per direction to the path:

  {"ts": ..., "dir": "in",  "adapter": "openai_chat", "body": {...}}
  {"ts": ..., "dir": "out", "adapter": "openai_chat", "body": "<bytes-as-utf8-or-base64>"}
  {"ts": ..., "dir": "out-stream", "adapter": "anthropic", "chunk": "..."}   # one per chunk

This is intended for one-off NewAPI passthrough audits (does NewAPI strip
``reasoning_content`` / ``thinking`` blocks, does it rewrite ``tool_call_id``?).
Pair it with tests/probe_reasoning_passthrough.py: that script captures what
the client (NewAPI-side) sees; this module captures what the gateway sees.
Diffing the two reveals what NewAPI is doing in the middle.

Remove the call sites and this file once the audit is done. Not meant for
production use — single global file lock, append-only, no rotation.
"""
from __future__ import annotations

import base64
import json
import os
import threading
import time
from collections.abc import AsyncIterator
from typing import Any

_lock = threading.Lock()


def _path() -> str | None:
    p = os.environ.get("MIMO_PROBE_DUMP", "")
    return p.strip() or None


def _write(record: dict[str, Any]) -> None:
    path = _path()
    if not path:
        return
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _lock:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
        except OSError:
            # Diagnostic shouldn't break the request path.
            pass


def dump_inbound(adapter_name: str, body: dict[str, Any]) -> None:
    """Record the parsed inbound JSON body before handler.handle runs."""
    if not _path():
        return
    _write({
        "ts": time.time(),
        "dir": "in",
        "adapter": adapter_name,
        "body": body,
    })


def dump_outbound(adapter_name: str, body_bytes: bytes) -> None:
    """Record the fully-serialized non-stream response body."""
    if not _path():
        return
    try:
        decoded: Any = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded = base64.b64encode(body_bytes).decode("ascii")
    _write({
        "ts": time.time(),
        "dir": "out",
        "adapter": adapter_name,
        "body": decoded,
    })


def tee_stream(
    adapter_name: str, stream: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    """Wrap an outbound stream to record every chunk as it passes through.

    No-op (returns the original iterator) when MIMO_PROBE_DUMP is unset, so
    the streaming hot path stays untouched in normal operation.
    """
    if not _path():
        return stream

    async def _wrapped() -> AsyncIterator[bytes]:
        async for chunk in stream:
            try:
                text = chunk.decode("utf-8")
            except UnicodeDecodeError:
                text = base64.b64encode(chunk).decode("ascii")
            _write({
                "ts": time.time(),
                "dir": "out-stream",
                "adapter": adapter_name,
                "chunk": text,
            })
            yield chunk

    return _wrapped()
