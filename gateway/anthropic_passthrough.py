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

from gateway.reasoning_cache import (
    derive_conversation_key,
    lookup_reasoning,
    remember_reasoning,
)


def _canonical_anthropic_block(block: Any) -> Any:
    """Stable representation of a single Anthropic content block for hashing.

    Skips ``thinking`` blocks: clients drop them on re-sent history (the
    rehydration we do here is exactly to recover from that), so the hash
    must be invariant to whether thinking is present. ``signature`` and
    ``cache_control`` are also dropped — not part of conversation
    identity. Keeps ``text``, ``id``, ``name``, ``input``, etc.
    """
    if isinstance(block, str):
        return {"t": "text", "x": block}
    if not isinstance(block, dict):
        return None
    btype = block.get("type")
    if btype == "text":
        return {"t": "text", "x": block.get("text") or ""}
    if btype == "thinking":
        # Intentionally omitted from the canonical form. See docstring.
        return None
    if btype == "tool_use":
        return {
            "t": "tool_use",
            "id": block.get("id") or "",
            "name": block.get("name") or "",
            "input": block.get("input") or {},
        }
    if btype == "tool_result":
        return {
            "t": "tool_result",
            "id": block.get("tool_use_id") or "",
            "out": block.get("content"),
        }
    if btype == "image":
        src = block.get("source") or {}
        return {
            "t": "image",
            "media_type": src.get("media_type") if isinstance(src, dict) else "",
            "data": src.get("data") if isinstance(src, dict) else "",
        }
    return {"t": btype, "raw": block}


def _canonical_anthropic_system(system: Any) -> Any:
    """Stable form of the ``system`` field (top-level on Anthropic).

    Anthropic accepts ``system`` as either a string or a list of content
    blocks (with ``cache_control`` flags etc.). Normalize so both forms
    that are textually equivalent hash the same."""
    if system is None:
        return None
    if isinstance(system, str):
        return [{"t": "text", "x": system}]
    if isinstance(system, list):
        out: list[Any] = []
        for blk in system:
            if isinstance(blk, str):
                out.append({"t": "text", "x": blk})
            elif isinstance(blk, dict):
                # cache_control is operator-tuning, not conversation identity.
                out.append({
                    "t": blk.get("type") or "text",
                    "x": blk.get("text") or "",
                })
        return out
    return None


def _conversation_key_from_body(body: dict[str, Any]) -> str:
    """Conversation-scope key from an Anthropic ``/v1/messages`` body.

    Hashes every body field that contributes to "which conversation this
    is": ``system``, ``messages``, ``tools``, ``tool_choice``,
    ``metadata``. An attacker who forges a ``tool_use`` id must also
    reproduce *all* of these to land in the same cache scope —
    effectively requiring full conversation context to be guessed, which
    isn't materially easier than knowing a session token.

    Body fields that don't shape conversation identity are deliberately
    excluded so the hash is stable across runs even if the client tweaks
    them: ``max_tokens``, ``temperature``, ``top_p``, ``stop_sequences``,
    ``stream``, ``model``.
    """
    msgs = body.get("messages") or []
    canon_messages: list[dict[str, Any]] = []
    for m in msgs:
        if not isinstance(m, dict):
            continue
        role = m.get("role", "user")
        raw_content = m.get("content")
        if isinstance(raw_content, str):
            blocks = [{"t": "text", "x": raw_content}]
        elif isinstance(raw_content, list):
            blocks = [b for b in (_canonical_anthropic_block(c) for c in raw_content) if b is not None]
        else:
            blocks = []
        canon_messages.append({"role": role, "content": blocks})

    canonical = {
        "messages": canon_messages,
        "system": _canonical_anthropic_system(body.get("system")),
        "tools": body.get("tools") or None,
        "tool_choice": body.get("tool_choice"),
        "metadata": body.get("metadata") or None,
    }
    blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return derive_conversation_key(blob)


def _conversation_key_from_messages_up_to(
    body: dict[str, Any], up_to_idx: int,
) -> str:
    """Conversation-scope key for ``messages[:up_to_idx]`` — used when
    looking up a specific assistant turn's reasoning during the per-message
    walk in :func:`patch_request_thinking`."""
    truncated = dict(body)
    truncated["messages"] = (body.get("messages") or [])[:up_to_idx]
    return _conversation_key_from_body(truncated)


def patch_request_thinking(body: dict[str, Any]) -> int:
    """Rehydrate missing ``thinking`` blocks in assistant history.

    Walks ``body["messages"]``. For every assistant message whose ``content``
    contains ``tool_use`` blocks but no ``thinking`` block, looks up the
    cache by the message's tool_use ids; on hit, prepends a synthetic
    ``{"type": "thinking", "thinking": "..."}`` block.

    Mutates ``body`` in place — callers that need to keep the original
    untouched must ``deepcopy`` before calling. The handler runs this once
    per request on a body it already owns exclusively, so the in-place
    mutation is intentional and avoids an extra ~tens-of-KB copy per turn.

    Returns the number of messages patched (useful for metrics/tests).
    """
    messages = body.get("messages")
    if not isinstance(messages, list):
        return 0

    patched = 0
    for idx, msg in enumerate(messages):
        if not isinstance(msg, dict) or msg.get("role") != "assistant":
            continue
        content = msg.get("content")

        # MiMo's /anthropic/v1/messages rejects assistant messages whose
        # content is null or an empty list. Some Anthropic-style clients
        # (notably Claude Code) emit such stubs on re-sent history; normalize
        # them to a whitespace text block so the upstream call doesn't 400.
        if content is None or (isinstance(content, list) and not content):
            msg["content"] = [{"type": "text", "text": " "}]
            content = msg["content"]
            patched += 1
            continue

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

        # Conversation key scopes the lookup to this conversation's history
        # *before* this assistant turn — same prefix the response-capture
        # path stored under on the previous turn. Even if a malicious
        # client crafts a request with a known tool_use id from another
        # conversation, their prefix won't match → cache miss → no leak.
        conv_key = _conversation_key_from_messages_up_to(body, idx)
        cached = lookup_reasoning(tool_use_ids, conversation_key=conv_key)
        if not cached:
            continue

        # MiMo accepts thinking blocks without a signature; we don't forge
        # one. Prepend so it precedes the tool_use blocks, matching the order
        # the model produced originally.
        msg["content"] = [{"type": "thinking", "thinking": cached}, *content]
        patched += 1

    return patched


def scan_response_json(raw_body: bytes, *, conversation_key: str) -> None:
    """Capture thinking text from a non-stream Anthropic response.

    Pulls ``thinking`` text and ``tool_use`` ids out of the assistant content
    and stores them in the reasoning cache so the next turn — even if the
    client drops the thinking block — can be rehydrated by
    :func:`patch_request_thinking`. The ``conversation_key`` must be derived
    from the request body that produced this response (via
    :func:`_conversation_key_from_body`) so the entry can only be read back
    on a subsequent turn of the *same* conversation.
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
        remember_reasoning(
            "".join(thinking_parts), tool_use_ids,
            conversation_key=conversation_key,
        )


def tee_stream_capture_thinking(
    raw_iter: AsyncIterator[bytes],
    usage_sink: dict[str, int] | None = None,
    *,
    conversation_key: str,
) -> AsyncIterator[bytes]:
    """Wrap an upstream byte stream so the client sees raw passthrough while
    we parse Anthropic SSE frames to harvest ``thinking_delta`` payloads,
    ``tool_use`` ids, and final token usage. On ``message_stop`` (or stream
    end), the collected thinking is committed to the reasoning cache under
    ``conversation_key`` (same scope contract as :func:`scan_response_json`).

    ``usage_sink`` (if provided) is mutated in place with ``input_tokens`` /
    ``output_tokens`` once they arrive in ``message_start`` / ``message_delta``
    frames. Letting the caller pass a dict avoids the iterator needing to
    return a tuple, keeping the streaming contract intact.

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
                _ingest_frame(frame, thinking_by_idx, tool_ids_by_idx, usage_sink)

        # Final flush: any remaining frame in the tail.
        if buf.strip():
            _ingest_frame(buf, thinking_by_idx, tool_ids_by_idx, usage_sink)

        ids = [tid for tid in tool_ids_by_idx.values() if tid]
        if not ids:
            return
        text = "".join(
            "".join(parts) for parts in thinking_by_idx.values()
        )
        if text:
            remember_reasoning(text, ids, conversation_key=conversation_key)

    return _wrapped()


def _ingest_frame(
    frame: bytes,
    thinking_by_idx: dict[int, list[str]],
    tool_ids_by_idx: dict[int, str],
    usage_sink: dict[str, int] | None,
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

        # Usage extraction: arrives at message_start (input_tokens) and
        # message_delta (final output_tokens). Both events lack `index`.
        if usage_sink is not None and etype in ("message_start", "message_delta"):
            _harvest_usage(evt, etype, usage_sink)

        idx = evt.get("index")
        if not isinstance(idx, int):
            # Other index-less frames (ping, message_stop) are advisory only.
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


def _harvest_usage(evt: dict[str, Any], etype: str, sink: dict[str, int]) -> None:
    """Pull token counts out of message_start / message_delta frames into
    ``sink``. Anthropic puts the initial usage inside ``message.usage`` on
    ``message_start`` and the cumulative usage at the top level on
    ``message_delta``."""
    if etype == "message_start":
        msg = evt.get("message") or {}
        usage = msg.get("usage") if isinstance(msg, dict) else None
    else:  # message_delta
        usage = evt.get("usage")
    if not isinstance(usage, dict):
        return
    try:
        inp = int(usage.get("input_tokens") or 0)
        out = int(usage.get("output_tokens") or 0)
    except (TypeError, ValueError):
        return
    if inp:
        sink["input_tokens"] = inp
    if out:
        # Later frames overwrite earlier ones — message_delta carries the
        # final cumulative number, which is what we want for metrics.
        sink["output_tokens"] = out
