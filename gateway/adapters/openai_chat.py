"""
OpenAI Chat Completions adapter.

Dual role:
  * Client-facing adapter for /v1/chat/completions
  * UpstreamCodec — talks to the MiMo OpenAI-compatible upstream

Streaming uses ``data: <json>\\n\\n`` chunks terminating with ``data: [DONE]``.
Tool calls stream as incremental ``tool_calls[i].function.arguments`` deltas.
"""
from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

from gateway.reasoning_cache import (
    derive_conversation_key,
    lookup_reasoning,
    remember_reasoning,
)
from gateway.core import (
    AdapterError,
    BadRequestError,
    ContentBlockEnd,
    ContentBlockStart,
    FinishReason,
    GatewayError,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    ReasoningDelta,
    StreamError,
    TextDelta,
    ToolCallDelta,
    Usage,
)

from .base import ProtocolAdapter


# ────────────── finish_reason mapping ──────────────

_OPENAI_TO_IES_FINISH: dict[str, FinishReason] = {
    "stop": "stop",
    "length": "length",
    "tool_calls": "tool_calls",
    "function_call": "tool_calls",   # legacy
    "content_filter": "content_filter",
}

_IES_TO_OPENAI_FINISH: dict[FinishReason, str] = {
    "stop": "stop",
    "length": "length",
    "tool_calls": "tool_calls",
    "content_filter": "content_filter",
    "error": "stop",                  # OpenAI has no error finish reason
}


def _map_finish(openai_reason: str | None) -> FinishReason:
    if openai_reason is None:
        return "stop"
    return _OPENAI_TO_IES_FINISH.get(openai_reason, "stop")


# ────────────── conversation hashing ──────────────

def _canonical_message_bytes(m: InternalMessage) -> bytes:
    """Stable bytes representation of one ``InternalMessage`` for hashing.

    Deliberately omits ``reasoning_content`` and any ``thinking`` blocks —
    clients drop those between turns, so the hash must agree whether or
    not they're present (the gateway will inject them back in via cache
    rehydration after the hash has already been computed). Tool inputs
    are sorted to match the OpenAI Chat tool_call ordering. Image data
    is hashed by its base64 string (matches what gets sent upstream).
    """
    blocks: list[dict[str, Any]] = []
    for c in m.content:
        if c.type == "text":
            blocks.append({"t": "text", "x": c.text or ""})
        elif c.type == "tool_use":
            blocks.append({
                "t": "tool_use", "id": c.tool_id or "",
                "name": c.tool_name or "",
                "args": c.tool_input or {},
            })
        elif c.type == "tool_result":
            blocks.append({
                "t": "tool_result", "id": c.tool_id or "",
                "out": c.tool_output or "",
            })
        elif c.type == "image":
            blocks.append({"t": "image", "mime": c.image_mime or "", "d": c.image_data or ""})
        # NOTE: "thinking" blocks deliberately omitted — see docstring.
    payload = {"role": m.role, "content": blocks}
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _conversation_key_for_request(
    messages: list[InternalMessage],
    *,
    tools: list[InternalTool] | None = None,
    tool_choice: Any = None,
) -> str:
    """Conversation key for the request that produced this response.

    Hashes message history *plus* the tool surface (``tools`` /
    ``tool_choice``). Two requests with identical messages but a
    different tool catalog would produce different upstream behaviour
    (different tool_use ids, different reasoning), so they're treated
    as separate conversations — and an attacker can't pull another
    conversation's reasoning by replaying just the messages without
    also matching the exact tool set.

    For OpenAI Chat there's no separate ``system`` field — system goes
    in ``messages[0]``, so it's already covered.
    """
    msg_blob = b"[" + b",".join(_canonical_message_bytes(m) for m in messages) + b"]"
    tools_canonical = None
    if tools:
        tools_canonical = sorted(
            [{"n": t.name, "d": t.description, "p": t.input_schema} for t in tools],
            key=lambda d: d["n"],
        )
    extra = json.dumps(
        {"tools": tools_canonical, "tool_choice": tool_choice},
        sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")
    return derive_conversation_key(msg_blob + b"|" + extra)


# ────────────── SSE utility ──────────────

async def iter_sse_data(raw: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """Yield the JSON payload from each ``data:`` line in an SSE byte stream.

    Handles arbitrary chunk boundaries by buffering until ``\\n``. Skips empty
    data lines (used as keepalive). Tail without trailing newline is flushed.
    """
    buf = b""
    async for chunk in raw:
        buf += chunk
        while b"\n" in buf:
            line, _, rest = buf.partition(b"\n")
            buf = rest
            line = line.rstrip(b"\r")
            if line.startswith(b"data:"):
                data = line[5:].lstrip()
                if data:
                    yield data.decode("utf-8", errors="replace")
    line = buf.rstrip(b"\r\n")
    if line.startswith(b"data:"):
        data = line[5:].lstrip()
        if data:
            yield data.decode("utf-8", errors="replace")


# ────────────── Adapter ──────────────

class OpenAIChatAdapter(ProtocolAdapter):
    """OpenAI Chat Completions: ``/v1/chat/completions``."""

    name = "openai_chat"

    @classmethod
    def matches_path(cls, path: str) -> bool:
        return path.endswith("/chat/completions")

    # ============ Request side ============

    def parse_request(self, body: dict[str, Any]) -> InternalRequest:
        if not isinstance(body, dict):
            raise BadRequestError("Request body must be a JSON object")
        model = body.get("model")
        if not model:
            raise BadRequestError("Missing 'model'")
        raw_messages = body.get("messages")
        if not isinstance(raw_messages, list) or not raw_messages:
            raise BadRequestError("'messages' must be a non-empty array")

        messages = [self._parse_message(m) for m in raw_messages]

        tools = None
        if body.get("tools"):
            tools = [self._parse_tool(t) for t in body["tools"]]

        return InternalRequest(
            model=model,
            messages=messages,
            max_tokens=int(
                body.get("max_tokens") or body.get("max_completion_tokens") or 4096
            ),
            stream=bool(body.get("stream", False)),
            temperature=body.get("temperature"),
            top_p=body.get("top_p"),
            stop=self._parse_stop(body.get("stop")),
            tools=tools,
            tool_choice=body.get("tool_choice"),
            metadata={k: v for k, v in body.items() if k not in _CONSUMED_KEYS},
        )

    @staticmethod
    def _parse_stop(stop: Any) -> list[str] | None:
        if stop is None:
            return None
        if isinstance(stop, str):
            return [stop]
        if isinstance(stop, list):
            return [str(s) for s in stop]
        return None

    @staticmethod
    def _parse_tool(t: Any) -> InternalTool:
        fn = t.get("function") if isinstance(t, dict) else None
        if not fn:
            raise BadRequestError(f"Invalid tool definition: {t}")
        return InternalTool(
            name=fn.get("name", ""),
            description=fn.get("description", ""),
            input_schema=fn.get("parameters", {}) or {},
        )

    @staticmethod
    def _parse_message(m: dict[str, Any]) -> InternalMessage:
        role = m.get("role", "user")
        content_blocks: list[InternalContent] = []

        if role == "tool":
            content_blocks.append(InternalContent(
                type="tool_result",
                tool_id=m.get("tool_call_id", ""),
                tool_output=str(m.get("content", "")),
            ))
            return InternalMessage(role="tool", content=content_blocks)

        if role == "assistant":
            text_content = m.get("content")
            if isinstance(text_content, str) and text_content:
                content_blocks.append(InternalContent(type="text", text=text_content))
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function", {}) if isinstance(tc, dict) else {}
                args_raw = fn.get("arguments")
                tool_input: dict[str, Any] | None = None
                if isinstance(args_raw, str):
                    try:
                        tool_input = json.loads(args_raw) if args_raw else {}
                    except json.JSONDecodeError:
                        tool_input = {"_raw": args_raw}
                elif isinstance(args_raw, dict):
                    tool_input = args_raw
                content_blocks.append(InternalContent(
                    type="tool_use",
                    tool_id=tc.get("id", ""),
                    tool_name=fn.get("name", ""),
                    tool_input=tool_input or {},
                ))
            reasoning = m.get("reasoning_content")
            return InternalMessage(
                role="assistant",
                content=content_blocks,
                reasoning_content=reasoning if isinstance(reasoning, str) else None,
            )

        # user / system: string OR list of content blocks
        raw = m.get("content", "")
        if isinstance(raw, str):
            if raw:
                content_blocks.append(InternalContent(type="text", text=raw))
        elif isinstance(raw, list):
            for block in raw:
                if isinstance(block, str):
                    content_blocks.append(InternalContent(type="text", text=block))
                    continue
                if not isinstance(block, dict):
                    continue
                btype = block.get("type")
                if btype == "text":
                    content_blocks.append(InternalContent(type="text", text=block.get("text", "")))
                elif btype == "image_url":
                    img = block.get("image_url", {})
                    url = img.get("url", "") if isinstance(img, dict) else str(img)
                    if url.startswith("data:"):
                        try:
                            header, _, b64 = url.partition(",")
                            mime = header.split(";")[0].split(":", 1)[-1] or "image/png"
                            content_blocks.append(InternalContent(
                                type="image", image_data=b64, image_mime=mime,
                            ))
                        except Exception as e:
                            raise BadRequestError(f"Invalid data URL: {e}") from e
                    else:
                        # Remote URL — keep as text reference; upstream may not fetch
                        content_blocks.append(InternalContent(
                            type="text", text=f"[image: {url}]",
                        ))
        return InternalMessage(role=role, content=content_blocks)

    # ============ Upstream serialization (IES → OpenAI Chat dict) ============

    def serialize_to_upstream(self, req: InternalRequest) -> dict[str, Any]:
        # Per-message prefix hash: for each assistant message at index k,
        # rehydration must look up the cache under
        # ``_conversation_key_for_request(messages[:k], tools=..., tool_choice=...)``.
        # We compute incrementally so we don't re-canonicalize the prefix
        # every time, but the tools/tool_choice tail is appended each turn
        # so it stays part of the scope.
        tools_canonical = None
        if req.tools:
            tools_canonical = sorted(
                [{"n": t.name, "d": t.description, "p": t.input_schema} for t in req.tools],
                key=lambda d: d["n"],
            )
        extras_blob = b"|" + json.dumps(
            {"tools": tools_canonical, "tool_choice": req.tool_choice},
            sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")

        prefix_blob = b"["
        first = True
        serialized: list[dict[str, Any]] = []
        for m in req.messages:
            if m.role == "assistant":
                # The hash for this assistant turn is the conversation as
                # it was *before* the model produced this turn — i.e., all
                # messages that came before, plus the request's tool surface
                # (which shapes which reasoning the model emitted).
                conv_key = derive_conversation_key(prefix_blob + b"]" + extras_blob)
                serialized.append(self._serialize_message(m, conversation_key=conv_key))
            else:
                serialized.append(self._serialize_message(m, conversation_key=None))
            piece = _canonical_message_bytes(m)
            if first:
                prefix_blob += piece
                first = False
            else:
                prefix_blob += b"," + piece

        body: dict[str, Any] = {
            "model": req.model,
            "messages": serialized,
            "max_tokens": req.max_tokens,
            "stream": req.stream,
        }
        if req.temperature is not None:
            body["temperature"] = req.temperature
        if req.top_p is not None:
            body["top_p"] = req.top_p
        if req.stop:
            body["stop"] = req.stop
        if req.tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.input_schema,
                    },
                }
                for t in req.tools
            ]
        if req.tool_choice is not None:
            body["tool_choice"] = req.tool_choice
        if req.stream:
            body["stream_options"] = {"include_usage": True}
        # MiMo's thinking-mode switch lives at the top of the body
        # (clients pass it via OpenAI SDK ``extra_body``). Forward it so
        # upstream actually emits ``reasoning_content``; otherwise the
        # reasoning-passthrough plumbing has nothing to carry.
        thinking = req.metadata.get("thinking")
        if isinstance(thinking, dict):
            body["thinking"] = thinking
        return body

    @staticmethod
    def _serialize_message(
        m: InternalMessage,
        *,
        conversation_key: str | None = None,
    ) -> dict[str, Any]:
        """Serialize one IES message into MiMo's upstream chat shape.

        ``conversation_key`` must be supplied for assistant messages so the
        reasoning-cache lookup is scoped to *this* conversation only. For
        non-assistant messages (user/system/tool) the cache is never read,
        so the argument is ignored.
        """
        if m.role == "tool":
            tr = next((c for c in m.content if c.type == "tool_result"), None)
            return {
                "role": "tool",
                "tool_call_id": tr.tool_id if tr else "",
                "content": tr.tool_output if tr else "",
            }

        if m.role == "assistant":
            text_parts = [c.text for c in m.content if c.type == "text" and c.text]
            tool_uses = [c for c in m.content if c.type == "tool_use"]
            out: dict[str, Any] = {
                "role": "assistant",
                "content": "".join(text_parts) if text_parts else None,
            }
            if m.reasoning_content is not None:
                out["reasoning_content"] = m.reasoning_content
            elif tool_uses and conversation_key is not None:
                # MiMo requires reasoning_content to be present in later
                # thinking-mode tool-call turns. Rehydrate from the cache —
                # scoped to *this* conversation only so a different
                # conversation that happens to reuse the same tool id (or a
                # forged id from a malicious request) can't read this
                # conversation's reasoning.
                cached_reasoning = lookup_reasoning(
                    (t.tool_id for t in tool_uses),
                    conversation_key=conversation_key,
                )
                if cached_reasoning:
                    out["reasoning_content"] = cached_reasoning
            if tool_uses:
                out["tool_calls"] = [
                    {
                        "id": tu.tool_id or "",
                        "type": "function",
                        "function": {
                            "name": tu.tool_name or "",
                            "arguments": json.dumps(tu.tool_input or {}, ensure_ascii=False),
                        },
                    }
                    for tu in tool_uses
                ]
            # MiMo rejects assistant messages that supply none of content /
            # reasoning_content / tool_calls. Claude Code occasionally re-sends
            # historical {role: assistant, content: null} stubs; backfill a
            # whitespace placeholder so history can round-trip instead of 400.
            if (
                out["content"] is None
                and "reasoning_content" not in out
                and "tool_calls" not in out
            ):
                out["content"] = " "
            return out

        # system / user
        has_image = any(c.type == "image" for c in m.content)
        if not has_image:
            text = "".join(c.text or "" for c in m.content if c.type == "text")
            return {"role": m.role, "content": text}

        blocks: list[dict[str, Any]] = []
        for c in m.content:
            if c.type == "text" and c.text:
                blocks.append({"type": "text", "text": c.text})
            elif c.type == "image":
                url = f"data:{c.image_mime or 'image/png'};base64,{c.image_data}"
                blocks.append({"type": "image_url", "image_url": {"url": url}})
        return {"role": m.role, "content": blocks}

    # ============ Upstream parsing — non-stream ============

    def parse_upstream_response(
        self,
        body: bytes,
        *,
        conversation_key: str,
    ) -> list[InternalEvent]:
        """Parse a non-stream OpenAI Chat completion JSON into IES events.

        Emits the same event sequence a streaming response would, so any
        downstream adapter can use the same serializer for both modes.

        ``conversation_key`` scopes the reasoning-cache write so the
        captured reasoning_content can only be rehydrated on a subsequent
        turn of the *same* conversation. Required — passing the wrong
        value would silently store under the wrong scope.
        """
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            raise AdapterError(f"Upstream returned invalid JSON: {e}") from e

        message_id = data.get("id") or _gen_id()
        model = data.get("model", "")
        choices = data.get("choices") or []
        if not choices:
            raise AdapterError("Upstream response has no choices")

        choice = choices[0]
        msg = choice.get("message", {}) or {}
        finish = _map_finish(choice.get("finish_reason"))
        usage = _parse_upstream_usage(data.get("usage"))

        events: list[InternalEvent] = [MessageStart(message_id=message_id, model=model)]
        next_idx = 0

        reasoning = msg.get("reasoning_content")
        if isinstance(reasoning, str) and reasoning:
            events.append(ReasoningDelta(text=reasoning))

        tool_call_ids = [tc.get("id", "") for tc in msg.get("tool_calls") or [] if isinstance(tc, dict)]
        remember_reasoning(reasoning, tool_call_ids, conversation_key=conversation_key)

        text = msg.get("content")
        if isinstance(text, str) and text:
            events.append(ContentBlockStart(index=next_idx, block_type="text"))
            events.append(TextDelta(index=next_idx, text=text))
            events.append(ContentBlockEnd(index=next_idx))
            next_idx += 1

        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function", {}) if isinstance(tc, dict) else {}
            tid = tc.get("id", "")
            tname = fn.get("name", "")
            args = fn.get("arguments", "")
            if not isinstance(args, str):
                args = json.dumps(args, ensure_ascii=False)
            events.append(ContentBlockStart(
                index=next_idx, block_type="tool_use",
                tool_id=tid, tool_name=tname,
            ))
            if args:
                events.append(ToolCallDelta(
                    index=next_idx, tool_id=tid, arguments_delta=args,
                ))
            events.append(ContentBlockEnd(index=next_idx))
            next_idx += 1

        events.append(MessageEnd(finish_reason=finish, usage=usage))
        return events

    # ============ Upstream parsing — stream ============

    async def parse_upstream_stream(
        self,
        raw: AsyncIterator[bytes],
        *,
        conversation_key: str,
    ) -> AsyncIterator[InternalEvent]:
        """Parse OpenAI Chat SSE bytes into IES events.

        State machine: lazy text-block opening, OpenAI tool_call.index ↔ IES
        block index mapping, finish_reason captured but emitted at end with
        accumulated usage (which often arrives after finish_reason).

        ``conversation_key`` scopes any reasoning captured from this stream
        (both the mid-stream "tool call started" snapshot and the end-of-
        stream final write) to this conversation. Required.
        """
        message_started = False
        text_idx: int | None = None
        tool_idx_map: dict[int, int] = {}        # OpenAI idx → IES idx
        tool_id_by_idx: dict[int, str] = {}      # IES idx → tool_id
        reasoning_parts: list[str] = []
        next_block = 0
        finish_reason: str | None = None
        usage = Usage()
        last_message_id = ""
        last_model = ""

        async for payload in iter_sse_data(raw):
            if payload.strip() == "[DONE]":
                break
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                yield StreamError(message=f"malformed chunk: {payload[:120]}", recoverable=True)
                continue

            last_message_id = chunk.get("id") or last_message_id
            last_model = chunk.get("model") or last_model

            if chunk.get("usage"):
                usage = _parse_upstream_usage(chunk.get("usage"))

            choices = chunk.get("choices") or []
            if not choices:
                continue
            choice = choices[0]
            delta = choice.get("delta") or {}

            if not message_started:
                yield MessageStart(
                    message_id=last_message_id or _gen_id(),
                    model=last_model,
                )
                message_started = True

            reasoning = delta.get("reasoning_content")
            if isinstance(reasoning, str) and reasoning:
                reasoning_parts.append(reasoning)
                yield ReasoningDelta(text=reasoning)

            content = delta.get("content")
            if isinstance(content, str) and content:
                if text_idx is None:
                    text_idx = next_block
                    next_block += 1
                    yield ContentBlockStart(index=text_idx, block_type="text")
                yield TextDelta(index=text_idx, text=content)

            for tc in delta.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                oai_idx = tc.get("index", 0)
                if oai_idx not in tool_idx_map:
                    ies_idx = next_block
                    next_block += 1
                    tool_idx_map[oai_idx] = ies_idx
                    fn = tc.get("function", {}) or {}
                    tool_id = tc.get("id") or f"tool_{ies_idx}"
                    tool_name = fn.get("name", "")
                    tool_id_by_idx[ies_idx] = tool_id
                    yield ContentBlockStart(
                        index=ies_idx, block_type="tool_use",
                        tool_id=tool_id, tool_name=tool_name,
                    )
                    # Commit reasoning to the cache as soon as we know the
                    # tool-call ids. By this point the model has finished
                    # thinking and decided to call a tool, so reasoning_parts
                    # is effectively complete — and committing here means we
                    # still cache something useful even if the stream gets
                    # cut short before MessageEnd.
                    if reasoning_parts:
                        remember_reasoning(
                            "".join(reasoning_parts),
                            tool_id_by_idx.values(),
                            conversation_key=conversation_key,
                        )
                ies_idx = tool_idx_map[oai_idx]
                fn = tc.get("function", {}) or {}
                args_delta = fn.get("arguments")
                if isinstance(args_delta, str) and args_delta:
                    yield ToolCallDelta(
                        index=ies_idx,
                        tool_id=tool_id_by_idx[ies_idx],
                        arguments_delta=args_delta,
                    )

            if choice.get("finish_reason"):
                finish_reason = choice["finish_reason"]

        if not message_started:
            yield MessageStart(
                message_id=last_message_id or _gen_id(),
                model=last_model,
            )

        if text_idx is not None:
            yield ContentBlockEnd(index=text_idx)
        for ies_idx in sorted(tool_idx_map.values()):
            yield ContentBlockEnd(index=ies_idx)

        remember_reasoning(
            "".join(reasoning_parts),
            tool_id_by_idx.values(),
            conversation_key=conversation_key,
        )

        yield MessageEnd(finish_reason=_map_finish(finish_reason), usage=usage)

    # ============ Client serialization (IES → OpenAI Chat output) ============

    def serialize_response_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        return self._serialize_stream(events)

    async def _serialize_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        message_id = ""
        model = ""
        created = int(time.time())
        ies_to_oai: dict[int, int] = {}     # IES idx → OpenAI tool_call.index
        next_oai_tool = 0

        async for ev in events:
            if isinstance(ev, MessageStart):
                message_id = ev.message_id
                model = ev.model
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"role": "assistant"},
                        "finish_reason": None,
                    }],
                })
            elif isinstance(ev, TextDelta):
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": ev.text},
                        "finish_reason": None,
                    }],
                })
            elif isinstance(ev, ReasoningDelta):
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"reasoning_content": ev.text},
                        "finish_reason": None,
                    }],
                })
            elif isinstance(ev, ContentBlockStart) and ev.block_type == "tool_use":
                oai_idx = next_oai_tool
                next_oai_tool += 1
                ies_to_oai[ev.index] = oai_idx
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "tool_calls": [{
                                "index": oai_idx,
                                "id": ev.tool_id or "",
                                "type": "function",
                                "function": {"name": ev.tool_name or "", "arguments": ""},
                            }],
                        },
                        "finish_reason": None,
                    }],
                })
            elif isinstance(ev, ToolCallDelta):
                oai_idx = ies_to_oai.get(ev.index, 0)
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "tool_calls": [{
                                "index": oai_idx,
                                "function": {"arguments": ev.arguments_delta},
                            }],
                        },
                        "finish_reason": None,
                    }],
                })
            elif isinstance(ev, MessageEnd):
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": _IES_TO_OPENAI_FINISH.get(ev.finish_reason, "stop"),
                    }],
                    "usage": _serialize_usage(ev.usage),
                })
                yield b"data: [DONE]\n\n"
            elif isinstance(ev, StreamError):
                yield self._sse_chunk({
                    "id": message_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": f"\n[stream error: {ev.message}]"},
                        "finish_reason": None,
                    }],
                })
            # ContentBlockStart(text) and ContentBlockEnd are no-ops in OpenAI SSE

    @staticmethod
    def _sse_chunk(payload: dict[str, Any]) -> bytes:
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()

    def serialize_response(self, events: list[InternalEvent]) -> bytes:
        message_id = ""
        model = ""
        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls_by_idx: dict[int, dict[str, Any]] = {}
        finish: FinishReason = "stop"
        usage = Usage()

        for ev in events:
            if isinstance(ev, MessageStart):
                message_id = ev.message_id
                model = ev.model
            elif isinstance(ev, ContentBlockStart) and ev.block_type == "tool_use":
                tool_calls_by_idx[ev.index] = {
                    "id": ev.tool_id or "",
                    "type": "function",
                    "function": {"name": ev.tool_name or "", "arguments": ""},
                }
            elif isinstance(ev, TextDelta):
                text_parts.append(ev.text)
            elif isinstance(ev, ReasoningDelta):
                reasoning_parts.append(ev.text)
            elif isinstance(ev, ToolCallDelta):
                if ev.index in tool_calls_by_idx:
                    tool_calls_by_idx[ev.index]["function"]["arguments"] += ev.arguments_delta
            elif isinstance(ev, MessageEnd):
                finish = ev.finish_reason
                usage = ev.usage

        msg: dict[str, Any] = {
            "role": "assistant",
            "content": "".join(text_parts) if text_parts else None,
        }
        if reasoning_parts:
            msg["reasoning_content"] = "".join(reasoning_parts)
        if tool_calls_by_idx:
            msg["tool_calls"] = [tool_calls_by_idx[i] for i in sorted(tool_calls_by_idx)]
        # Avoid emitting bare {content: null} stubs to clients. Some agents
        # (notably Claude Code) preserve null content in their history and
        # re-send it on the next turn, which MiMo then rejects with 400. A
        # whitespace placeholder breaks that feedback loop without changing
        # the message semantics.
        if msg["content"] is None and "tool_calls" not in msg and "reasoning_content" not in msg:
            msg["content"] = " "

        body = {
            "id": message_id or _gen_id(),
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{
                "index": 0,
                "message": msg,
                "finish_reason": _IES_TO_OPENAI_FINISH.get(finish, "stop"),
            }],
            "usage": _serialize_usage(usage),
        }
        return json.dumps(body, ensure_ascii=False).encode()

    # ============ Error envelope ============

    def error_envelope(self, err: GatewayError) -> bytes:
        return json.dumps({
            "error": {
                "message": err.message,
                "type": err.error_code,
                "code": err.error_code,
            }
        }).encode()


# ────────────── helpers ──────────────

_CONSUMED_KEYS = frozenset({
    "model", "messages", "max_tokens", "max_completion_tokens", "stream",
    "temperature", "top_p", "stop", "tools", "tool_choice",
    # Things we don't pass through (yet)
    "n", "stream_options", "logprobs", "top_logprobs", "logit_bias",
    "presence_penalty", "frequency_penalty", "user", "response_format",
    "seed", "service_tier",
})


def _gen_id() -> str:
    return f"chatcmpl-{uuid.uuid4().hex[:24]}"


def _parse_upstream_usage(u_dict: Any) -> Usage:
    """Pull every OpenAI-style usage field MiMo upstream may include.

    MiMo's OpenAI endpoint extends OpenAI's standard ``usage`` with
    ``prompt_tokens_details`` (cached / audio / image / video) and
    ``completion_tokens_details`` (reasoning). It also adds ``web_search_usage``.
    NewAPI reads ``prompt_tokens_details.cached_tokens`` to apply the
    cache-hit billing rate — dropping it on this path makes clients pay full
    price for cached prompts.

    Returns a zero Usage if the input is malformed (defensive: upstream
    schema can drift; we don't want a single missing nested object to break
    the response path).
    """
    if not isinstance(u_dict, dict):
        return Usage()

    prompt_details = u_dict.get("prompt_tokens_details") or {}
    if not isinstance(prompt_details, dict):
        prompt_details = {}
    completion_details = u_dict.get("completion_tokens_details") or {}
    if not isinstance(completion_details, dict):
        completion_details = {}
    web_search_usage = u_dict.get("web_search_usage")
    if not isinstance(web_search_usage, dict):
        web_search_usage = None

    def _int(d: dict, key: str) -> int:
        try:
            return int(d.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    return Usage(
        input_tokens=_int(u_dict, "prompt_tokens"),
        output_tokens=_int(u_dict, "completion_tokens"),
        cached_tokens=_int(prompt_details, "cached_tokens"),
        audio_tokens=_int(prompt_details, "audio_tokens"),
        image_tokens=_int(prompt_details, "image_tokens"),
        video_tokens=_int(prompt_details, "video_tokens"),
        reasoning_tokens=_int(completion_details, "reasoning_tokens"),
        web_search_usage=web_search_usage,
    )


def _serialize_usage(u: Usage) -> dict[str, Any]:
    """Render an IES Usage as an OpenAI-shaped ``usage`` dict.

    Nested *_details / web_search_usage objects are emitted only when any
    sub-field is non-zero, so the wire payload stays clean for callers that
    don't use multimodal / web-search / cache features.
    """
    out: dict[str, Any] = {
        "prompt_tokens": u.input_tokens,
        "completion_tokens": u.output_tokens,
        "total_tokens": u.total_tokens,
    }
    prompt_details: dict[str, int] = {}
    if u.cached_tokens:
        prompt_details["cached_tokens"] = u.cached_tokens
    if u.audio_tokens:
        prompt_details["audio_tokens"] = u.audio_tokens
    if u.image_tokens:
        prompt_details["image_tokens"] = u.image_tokens
    if u.video_tokens:
        prompt_details["video_tokens"] = u.video_tokens
    if prompt_details:
        out["prompt_tokens_details"] = prompt_details
    if u.reasoning_tokens:
        out["completion_tokens_details"] = {"reasoning_tokens": u.reasoning_tokens}
    if u.web_search_usage:
        out["web_search_usage"] = u.web_search_usage
    return out
