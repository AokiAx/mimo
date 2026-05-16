"""Anthropic-native passthrough: patch & cache helpers.

The gateway forwards ``/v1/messages`` straight to MiMo's
``/anthropic/v1/messages`` so ``thinking`` content blocks and their
``signature`` round-trip natively. But MiMo requires that assistant messages
in history which contain ``tool_use`` blocks also carry the original
``thinking`` block — many Anthropic-style agent clients drop it when storing
the conversation, which then triggers a 400 on the next turn.

This module is the compatibility layer that fixes that:

  * On the way in (request body), :func:`patch_request_thinking` scans every
    ``assistant`` message and injects a cached ``thinking`` block when one
    is missing but ``tool_use`` blocks are present.
  * On the way out (upstream response), :func:`scan_response_json` and
    :func:`tee_stream_capture_thinking` capture the model-issued thinking
    text keyed by the tool_use ids of the same message and feed it back into
    the cache for future turns.

The cache is the same one OpenAI Chat uses (``gateway.reasoning_cache``)
because the upstream tool ids are globally unique within a process. We
deliberately don't store ``signature`` — MiMo doesn't enforce it for
re-submitted thinking blocks, and forging one would defeat the purpose.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from gateway.reasoning_cache import lookup_reasoning, remember_reasoning


def patch_request_thinking(body: dict[str, Any]) -> int:
    """Rehydrate missing ``thinking`` blocks in assistant history.

    Walks ``body["messages"]``. For every assistant message whose ``content``
    contains ``tool_use`` blocks but no ``thinking`` block, looks up the
    cache by the message's tool_use ids; on hit, prepends a synthetic
    ``{"type": "thinking", "thinking": "..."}`` block.

    Returns the number of messages patched (useful for metrics/tests).
    """
    messages = body.get("messages")
    if not isinstance(messages, list):
        return 0

    patched = 0
    for msg in messages:
        if not isinstance(msg, dict) or msg.get("role") != "assistant":
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            continue

        tool_use_ids: list[str] = []
        has_thinking = False
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "thinking":
                has_thinking = True
            elif btype == "tool_use":
                tid = block.get("id")
                if isinstance(tid, str) and tid:
                    tool_use_ids.append(tid)

        if has_thinking or not tool_use_ids:
            continue

        cached = lookup_reasoning(tool_use_ids)
        if not cached:
            continue

        # MiMo accepts thinking blocks without a signature; we don't forge
        # one. Prepend so it precedes the tool_use blocks, matching the order
        # the model produced originally.
        msg["content"] = [{"type": "thinking", "thinking": cached}, *content]
        patched += 1

    return patched


def scan_response_json(raw_body: bytes) -> None:
    """Capture thinking text from a non-stream Anthropic response.

    Pulls ``thinking`` text and ``tool_use`` ids out of the assistant content
    and stores them in the reasoning cache so the next turn — even if the
    client drops the thinking block — can be rehydrated by
    :func:`patch_request_thinking`.
    """
    try:
        data = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return
    if not isinstance(data, dict):
        return
    content = data.get("content")
    if not isinstance(content, list):
        return

    thinking_parts: list[str] = []
    tool_use_ids: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type")
        if btype == "thinking":
            text = block.get("thinking")
            if isinstance(text, str) and text:
                thinking_parts.append(text)
        elif btype == "tool_use":
            tid = block.get("id")
            if isinstance(tid, str) and tid:
                tool_use_ids.append(tid)

    if thinking_parts and tool_use_ids:
        remember_reasoning("".join(thinking_parts), tool_use_ids)


def tee_stream_capture_thinking(
    raw_iter: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    """Wrap an upstream byte stream so the client sees raw passthrough while
    we parse Anthropic SSE frames to harvest ``thinking_delta`` payloads and
    ``tool_use`` ids. On ``message_stop`` (or stream end), the collected
    thinking is committed to the reasoning cache.

    SSE framing per Anthropic: blank-line-separated frames, each containing
    ``event: <name>\\n`` and ``data: <json>\\n``. We only inspect ``data:``
    lines — event names are advisory.
    """

    async def _wrapped() -> AsyncIterator[bytes]:
        buf = b""
        thinking_by_idx: dict[int, list[str]] = {}
        tool_ids_by_idx: dict[int, str] = {}

        async for chunk in raw_iter:
            yield chunk
            buf += chunk
            # Parse complete frames (terminated by blank line). Anthropic
            # uses `\n\n` as the separator — keep the partial tail in `buf`.
            while b"\n\n" in buf:
                frame, _, buf = buf.partition(b"\n\n")
                _ingest_frame(frame, thinking_by_idx, tool_ids_by_idx)

        # Final flush: any remaining frame in the tail.
        if buf.strip():
            _ingest_frame(buf, thinking_by_idx, tool_ids_by_idx)

        ids = [tid for tid in tool_ids_by_idx.values() if tid]
        if not ids:
            return
        text = "".join(
            "".join(parts) for parts in thinking_by_idx.values()
        )
        if text:
            remember_reasoning(text, ids)

    return _wrapped()


def _ingest_frame(
    frame: bytes,
    thinking_by_idx: dict[int, list[str]],
    tool_ids_by_idx: dict[int, str],
) -> None:
    """Parse one Anthropic SSE frame and update the accumulators in place."""
    for line in frame.split(b"\n"):
        line = line.rstrip(b"\r")
        if not line.startswith(b"data:"):
            continue
        payload = line[5:].strip()
        if not payload:
            continue
        try:
            evt = json.loads(payload.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(evt, dict):
            continue
        etype = evt.get("type")
        idx = evt.get("index")
        if not isinstance(idx, int):
            # Some Anthropic frames (message_start, ping, message_delta,
            # message_stop) carry no index — ignore them for harvesting.
            continue
        if etype == "content_block_start":
            block = evt.get("content_block") or {}
            if isinstance(block, dict) and block.get("type") == "tool_use":
                tid = block.get("id")
                if isinstance(tid, str) and tid:
                    tool_ids_by_idx[idx] = tid
        elif etype == "content_block_delta":
            delta = evt.get("delta") or {}
            if isinstance(delta, dict) and delta.get("type") == "thinking_delta":
                text = delta.get("thinking", "")
                if isinstance(text, str) and text:
                    thinking_by_idx.setdefault(idx, []).append(text)
