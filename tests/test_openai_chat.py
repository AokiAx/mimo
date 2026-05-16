"""Unit tests for gateway.adapters.openai_chat.

Covered:
  * iter_sse_data — chunk boundary, keepalive, tail flush
  * parse_request — text / vision data URL / assistant tool_calls / tool result / errors
  * serialize_to_upstream — OpenAI body shape, stream_options, tools
  * parse_upstream_response — text-only and tool_calls non-stream
  * parse_upstream_stream — split SSE chunks, tool_call.index → IES idx mapping
  * serialize_response_stream — produces valid OpenAI SSE chunks ending [DONE]
  * serialize_response — non-stream collected JSON
  * finish_reason mapping — function_call → tool_calls
  * error_envelope — OpenAI {error:{message,type,code}} shape
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from gateway.adapters.openai_chat import (
    OpenAIChatAdapter,
    _IES_TO_OPENAI_FINISH,
    _map_finish,
    iter_sse_data,
)
from gateway.core import (
    BadRequestError,
    ContentBlockEnd,
    ContentBlockStart,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    ReasoningDelta,
    TextDelta,
    ToolCallDelta,
    Usage,
)


# ───────── helpers ─────────


def _run(coro):
    return asyncio.run(coro)


async def _gen(chunks: list[bytes]) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


async def _events(stream: AsyncIterator[InternalEvent]) -> list[InternalEvent]:
    return [ev async for ev in stream]


async def _drain(stream: AsyncIterator[bytes]) -> bytes:
    out = b""
    async for c in stream:
        out += c
    return out


def _adapter() -> OpenAIChatAdapter:
    return OpenAIChatAdapter()


# ───────── iter_sse_data ─────────


def test_iter_sse_data_handles_arbitrary_chunk_boundaries():
    # Splits "data: {…}\n\n" awkwardly across byte chunks.
    payload = b'data: {"a":1}\n\ndata: {"b":2}\n\n'
    chunks = [payload[:5], payload[5:9], payload[9:18], payload[18:]]

    async def run() -> list[str]:
        return [d async for d in iter_sse_data(_gen(chunks))]

    out = _run(run())
    assert out == ['{"a":1}', '{"b":2}']


def test_iter_sse_data_skips_empty_keepalive():
    # Empty `data:` lines (keepalives) must be skipped.
    payload = b"data:\n\ndata: hello\n\ndata: \n\n"

    async def run() -> list[str]:
        return [d async for d in iter_sse_data(_gen([payload]))]

    out = _run(run())
    assert out == ["hello"]


def test_iter_sse_data_flushes_tail_without_newline():
    # A final line without trailing \n should still be emitted.
    payload = b"data: trailing"

    async def run() -> list[str]:
        return [d async for d in iter_sse_data(_gen([payload]))]

    out = _run(run())
    assert out == ["trailing"]


# ───────── matches_path ─────────


def test_matches_path():
    assert OpenAIChatAdapter.matches_path("/v1/chat/completions")
    assert OpenAIChatAdapter.matches_path("/openai/v1/chat/completions")
    assert not OpenAIChatAdapter.matches_path("/v1/messages")


# ───────── parse_request ─────────


def test_parse_request_simple_text():
    body = {
        "model": "MiMo-VL-7B-RL-2508",
        "messages": [
            {"role": "system", "content": "be helpful"},
            {"role": "user", "content": "hi"},
        ],
        "max_tokens": 256,
        "stream": True,
        "temperature": 0.5,
    }
    req = _adapter().parse_request(body)
    assert req.model == "MiMo-VL-7B-RL-2508"
    assert req.max_tokens == 256
    assert req.stream is True
    assert req.temperature == 0.5
    assert len(req.messages) == 2
    assert req.messages[0].role == "system"
    assert req.messages[0].content[0].type == "text"
    assert req.messages[0].content[0].text == "be helpful"
    assert req.messages[1].role == "user"
    assert req.messages[1].content[0].text == "hi"


def test_parse_request_preserves_assistant_reasoning_content_with_tool_calls():
    req = _adapter().parse_request({
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": None,
            "reasoning_content": "I should call the tool.",
            "tool_calls": [{
                "id": "call_1", "type": "function",
                "function": {"name": "search", "arguments": "{\"q\":\"x\"}"},
            }],
        }],
    })
    msg = req.messages[0]
    assert msg.role == "assistant"
    assert msg.reasoning_content == "I should call the tool."
    assert msg.content[0].tool_id == "call_1"


def test_parse_request_missing_model_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"messages": [{"role": "user", "content": "x"}]})


def test_parse_request_missing_messages_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"model": "m"})
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"model": "m", "messages": []})


def test_parse_request_uses_max_completion_tokens_fallback_and_default():
    req = _adapter().parse_request({
        "model": "m", "messages": [{"role": "user", "content": "x"}],
        "max_completion_tokens": 123,
    })
    assert req.max_tokens == 123

    req2 = _adapter().parse_request({
        "model": "m", "messages": [{"role": "user", "content": "x"}],
    })
    assert req2.max_tokens == 4096


def test_parse_request_stop_normalized_to_list():
    req = _adapter().parse_request({
        "model": "m", "messages": [{"role": "user", "content": "x"}],
        "stop": "###",
    })
    assert req.stop == ["###"]
    req2 = _adapter().parse_request({
        "model": "m", "messages": [{"role": "user", "content": "x"}],
        "stop": ["a", "b"],
    })
    assert req2.stop == ["a", "b"]


def test_parse_request_image_url_data_url():
    body = {
        "model": "m",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this"},
                {"type": "image_url", "image_url": {
                    "url": "data:image/jpeg;base64,AAAA"
                }},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    blocks = req.messages[0].content
    assert len(blocks) == 2
    assert blocks[0].type == "text"
    assert blocks[1].type == "image"
    assert blocks[1].image_mime == "image/jpeg"
    assert blocks[1].image_data == "AAAA"


def test_parse_request_image_remote_url_falls_back_to_text():
    body = {
        "model": "m",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": "https://x.example/y.png"}},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    block = req.messages[0].content[0]
    assert block.type == "text"
    assert "https://x.example/y.png" in (block.text or "")


def test_parse_request_assistant_tool_calls():
    body = {
        "model": "m",
        "messages": [
            {"role": "user", "content": "search please"},
            {
                "role": "assistant",
                "content": "let me search",
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": "web_search",
                        "arguments": '{"q": "weather"}',
                    },
                }],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "sunny"},
        ],
    }
    req = _adapter().parse_request(body)
    asst = req.messages[1]
    assert asst.role == "assistant"
    types = [c.type for c in asst.content]
    assert "text" in types and "tool_use" in types
    tu = next(c for c in asst.content if c.type == "tool_use")
    assert tu.tool_id == "call_1"
    assert tu.tool_name == "web_search"
    assert tu.tool_input == {"q": "weather"}

    tool_msg = req.messages[2]
    assert tool_msg.role == "tool"
    tr = tool_msg.content[0]
    assert tr.type == "tool_result"
    assert tr.tool_id == "call_1"
    assert tr.tool_output == "sunny"


def test_parse_request_assistant_malformed_args_preserved():
    body = {
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "c2",
                "function": {"name": "fn", "arguments": "{not json"},
            }],
        }],
    }
    req = _adapter().parse_request(body)
    tu = req.messages[0].content[0]
    assert tu.type == "tool_use"
    assert tu.tool_input == {"_raw": "{not json"}


def test_parse_request_tools_definition():
    body = {
        "model": "m",
        "messages": [{"role": "user", "content": "x"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "calc",
                "description": "do math",
                "parameters": {"type": "object", "properties": {"x": {"type": "number"}}},
            },
        }],
        "tool_choice": "auto",
    }
    req = _adapter().parse_request(body)
    assert req.tools is not None and len(req.tools) == 1
    t = req.tools[0]
    assert isinstance(t, InternalTool)
    assert t.name == "calc"
    assert t.description == "do math"
    assert t.input_schema["type"] == "object"
    assert req.tool_choice == "auto"


def test_parse_request_metadata_passthrough():
    body = {
        "model": "m",
        "messages": [{"role": "user", "content": "x"}],
        "presence_penalty": 0.1,
        "x_custom_field": True,
    }
    req = _adapter().parse_request(body)
    # known consumed
    assert "model" not in req.metadata
    # known dropped (in _CONSUMED_KEYS)
    assert "presence_penalty" not in req.metadata
    # truly unknown — passed through
    assert req.metadata.get("x_custom_field") is True


# ───────── serialize_to_upstream ─────────


def test_serialize_to_upstream_includes_stream_options_when_streaming():
    req = InternalRequest(
        model="m", max_tokens=128, stream=True,
        messages=[InternalMessage(role="user", content=[
            InternalContent(type="text", text="hi"),
        ])],
    )
    body = _adapter().serialize_to_upstream(req)
    assert body["stream"] is True
    assert body["stream_options"] == {"include_usage": True}
    assert body["messages"] == [{"role": "user", "content": "hi"}]
    assert "stop" not in body  # None should be omitted


def test_serialize_to_upstream_omits_stream_options_when_not_streaming():
    req = InternalRequest(
        model="m", max_tokens=128, stream=False,
        messages=[InternalMessage(role="user", content=[
            InternalContent(type="text", text="hi"),
        ])],
    )
    body = _adapter().serialize_to_upstream(req)
    assert "stream_options" not in body


def test_serialize_to_upstream_assistant_with_tool_use_renders_tool_calls():
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[
            InternalMessage(role="user", content=[InternalContent(type="text", text="q")]),
            InternalMessage(role="assistant", content=[
                InternalContent(type="text", text="ok"),
                InternalContent(
                    type="tool_use", tool_id="t1",
                    tool_name="search", tool_input={"q": "weather"},
                ),
            ]),
            InternalMessage(role="tool", content=[
                InternalContent(type="tool_result", tool_id="t1", tool_output="sunny"),
            ]),
        ],
    )
    body = _adapter().serialize_to_upstream(req)
    msgs = body["messages"]
    assert msgs[1]["role"] == "assistant"
    assert msgs[1]["content"] == "ok"
    assert "reasoning_content" not in msgs[1]
    assert msgs[1]["tool_calls"][0]["id"] == "t1"
    assert msgs[1]["tool_calls"][0]["function"]["name"] == "search"
    assert json.loads(msgs[1]["tool_calls"][0]["function"]["arguments"]) == {"q": "weather"}
    assert msgs[2] == {"role": "tool", "tool_call_id": "t1", "content": "sunny"}


def test_serialize_to_upstream_preserves_assistant_reasoning_content():
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[InternalMessage(
            role="assistant",
            reasoning_content="hidden chain",
            content=[InternalContent(
                type="tool_use", tool_id="t1", tool_name="search", tool_input={"q": "x"},
            )],
        )],
    )
    msg = _adapter().serialize_to_upstream(req)["messages"][0]
    assert msg["reasoning_content"] == "hidden chain"
    assert msg["tool_calls"][0]["id"] == "t1"


def test_serialize_to_upstream_image_block_kept_as_blocks():
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[InternalMessage(role="user", content=[
            InternalContent(type="text", text="see"),
            InternalContent(type="image", image_data="AAAA", image_mime="image/png"),
        ])],
    )
    body = _adapter().serialize_to_upstream(req)
    content = body["messages"][0]["content"]
    assert isinstance(content, list)
    types = [b["type"] for b in content]
    assert types == ["text", "image_url"]
    assert content[1]["image_url"]["url"].startswith("data:image/png;base64,AAAA")


def test_serialize_to_upstream_tools_render():
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[InternalMessage(role="user", content=[
            InternalContent(type="text", text="x"),
        ])],
        tools=[InternalTool(name="t", description="d", input_schema={"type": "object"})],
        tool_choice="auto",
    )
    body = _adapter().serialize_to_upstream(req)
    assert body["tools"][0]["function"]["name"] == "t"
    assert body["tool_choice"] == "auto"


# ───────── parse_upstream_response (non-stream) ─────────


def test_parse_upstream_response_text_only():
    payload = json.dumps({
        "id": "chatcmpl-abc",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "hello world"},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
    }).encode()
    events = _adapter().parse_upstream_response(payload)
    assert isinstance(events[0], MessageStart)
    assert events[0].message_id == "chatcmpl-abc"
    assert any(isinstance(e, TextDelta) and e.text == "hello world" for e in events)
    end = events[-1]
    assert isinstance(end, MessageEnd)
    assert end.finish_reason == "stop"
    assert end.usage.input_tokens == 3
    assert end.usage.output_tokens == 2


def test_parse_upstream_response_with_tool_calls():
    payload = json.dumps({
        "id": "chatcmpl-1",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "c1", "type": "function",
                     "function": {"name": "f", "arguments": '{"x":1}'}},
                    {"id": "c2", "type": "function",
                     "function": {"name": "g", "arguments": '{"y":2}'}},
                ],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1},
    }).encode()
    events = _adapter().parse_upstream_response(payload)
    starts = [e for e in events if isinstance(e, ContentBlockStart)]
    assert len(starts) == 2
    assert starts[0].block_type == "tool_use"
    assert starts[0].tool_id == "c1"
    assert starts[1].tool_id == "c2"
    deltas = [e for e in events if isinstance(e, ToolCallDelta)]
    assert deltas[0].arguments_delta == '{"x":1}'
    assert deltas[1].arguments_delta == '{"y":2}'
    end = events[-1]
    assert isinstance(end, MessageEnd) and end.finish_reason == "tool_calls"


def test_parse_upstream_response_preserves_reasoning_content():
    payload = json.dumps({
        "id": "chatcmpl-r",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "reasoning_content": "thinking",
                "tool_calls": [{
                    "id": "c1", "type": "function",
                    "function": {"name": "f", "arguments": "{}"},
                }],
            },
            "finish_reason": "tool_calls",
        }],
    }).encode()
    events = _adapter().parse_upstream_response(payload)
    assert any(isinstance(e, ReasoningDelta) and e.text == "thinking" for e in events)


# ───────── parse_upstream_stream ─────────


def _sse(payload: dict) -> bytes:
    return f"data: {json.dumps(payload)}\n\n".encode()


def test_parse_upstream_stream_text_with_split_chunks():
    chunks_payloads = [
        {"id": "x1", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"content": "Hel"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"content": "lo"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {"choices": [], "usage": {"prompt_tokens": 4, "completion_tokens": 2}},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"
    # Split into awkward 7-byte chunks to stress boundary handling.
    raw_chunks = [bytes_stream[i:i + 7] for i in range(0, len(bytes_stream), 7)]

    async def run() -> list[InternalEvent]:
        return await _events(_adapter().parse_upstream_stream(_gen(raw_chunks)))

    events = _run(run())
    assert isinstance(events[0], MessageStart)
    text = "".join(e.text for e in events if isinstance(e, TextDelta))
    assert text == "Hello"
    end = events[-1]
    assert isinstance(end, MessageEnd)
    assert end.finish_reason == "stop"
    assert end.usage.input_tokens == 4
    assert end.usage.output_tokens == 2
    # text block opens once and closes once
    assert sum(1 for e in events if isinstance(e, ContentBlockStart)) == 1
    assert sum(1 for e in events if isinstance(e, ContentBlockEnd)) == 1


def test_parse_upstream_stream_tool_calls_with_index_mapping():
    """Two tool calls interleaved; OpenAI .index decides ordering, not arrival order."""
    chunks_payloads = [
        {"id": "x", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        # First fragment, tool index=0
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{
                "index": 0, "id": "c0", "type": "function",
                "function": {"name": "search", "arguments": '{"q":'},
            }]}, "finish_reason": None}]},
        # Second tool starts, tool index=1
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{
                "index": 1, "id": "c1", "type": "function",
                "function": {"name": "calc", "arguments": '{"a":'},
            }]}, "finish_reason": None}]},
        # Continuation of first
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 0, "function": {"arguments": '"x"}'}}]
        }, "finish_reason": None}]},
        # Continuation of second
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{"index": 1, "function": {"arguments": '1}'}}]
        }, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
         "usage": {"prompt_tokens": 1, "completion_tokens": 1}},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run() -> list[InternalEvent]:
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream])))

    events = _run(run())

    starts = [e for e in events if isinstance(e, ContentBlockStart)]
    assert len(starts) == 2
    assert starts[0].tool_name == "search" and starts[0].tool_id == "c0"
    assert starts[1].tool_name == "calc" and starts[1].tool_id == "c1"
    # IES indexes are 0 and 1 in arrival order
    assert starts[0].index == 0 and starts[1].index == 1

    # Reconstruct args by IES idx
    args0 = "".join(
        e.arguments_delta for e in events
        if isinstance(e, ToolCallDelta) and e.index == 0
    )
    args1 = "".join(
        e.arguments_delta for e in events
        if isinstance(e, ToolCallDelta) and e.index == 1
    )
    assert json.loads(args0) == {"q": "x"}
    assert json.loads(args1) == {"a": 1}

    end = events[-1]
    assert isinstance(end, MessageEnd) and end.finish_reason == "tool_calls"


def test_parse_upstream_stream_function_call_legacy_mapped_to_tool_calls():
    chunks_payloads = [
        {"id": "x", "model": "m",
         "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "function_call"}]},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream])))

    events = _run(run())
    end = events[-1]
    assert isinstance(end, MessageEnd)
    assert end.finish_reason == "tool_calls"


def test_parse_upstream_stream_preserves_reasoning_content_delta():
    chunks_payloads = [
        {"id": "x1", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"reasoning_content": "think"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"content": "answer"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream])))

    events = _run(run())
    assert any(isinstance(e, ReasoningDelta) and e.text == "think" for e in events)
    assert "answer" == "".join(e.text for e in events if isinstance(e, TextDelta))


# ───────── serialize_response_stream (IES → OpenAI SSE) ─────────


def test_serialize_response_stream_round_trip_text():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="chatcmpl-z", model="m")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="Hi")
        yield TextDelta(index=0, text=" there")
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=2))

    async def run() -> bytes:
        return await _drain(_adapter().serialize_response_stream(feed()))

    raw = _run(run())
    assert raw.endswith(b"data: [DONE]\n\n")
    # Each chunk starts with `data: ` and ends with double newline.
    chunks = [c for c in raw.split(b"\n\n") if c]
    decoded: list[dict] = []
    for c in chunks:
        if c == b"data: [DONE]":
            continue
        assert c.startswith(b"data: ")
        decoded.append(json.loads(c[6:].decode()))
    # First role chunk
    assert decoded[0]["choices"][0]["delta"] == {"role": "assistant"}
    # Two content deltas
    contents = [d["choices"][0]["delta"].get("content") for d in decoded]
    assert "Hi" in contents and " there" in contents
    # finish chunk has finish_reason and usage
    final = decoded[-1]
    assert final["choices"][0]["finish_reason"] == "stop"
    assert final["usage"]["prompt_tokens"] == 1
    assert final["usage"]["completion_tokens"] == 2
    assert final["usage"]["total_tokens"] == 3


def test_serialize_response_stream_emits_reasoning_content_delta():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="x", model="m")
        yield ReasoningDelta(text="think")
        yield MessageEnd(finish_reason="stop", usage=Usage())

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    payloads = [
        json.loads(c[6:].decode())
        for c in raw.split(b"\n\n")
        if c and c != b"data: [DONE]" and c.startswith(b"data: ")
    ]
    assert {"reasoning_content": "think"} in [p["choices"][0]["delta"] for p in payloads]


def test_serialize_response_stream_tool_call_delta():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="x", model="m")
        yield ContentBlockStart(index=0, block_type="tool_use", tool_id="c1", tool_name="search")
        yield ToolCallDelta(index=0, tool_id="c1", arguments_delta='{"q":')
        yield ToolCallDelta(index=0, tool_id="c1", arguments_delta='"x"}')
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="tool_calls", usage=Usage(input_tokens=1, output_tokens=1))

    async def run() -> bytes:
        return await _drain(_adapter().serialize_response_stream(feed()))

    raw = _run(run())
    payloads = [
        json.loads(c[6:].decode())
        for c in raw.split(b"\n\n")
        if c and c != b"data: [DONE]" and c.startswith(b"data: ")
    ]
    # 2nd chunk should announce the tool_call with index=0, name=search, id=c1
    announce = payloads[1]["choices"][0]["delta"]["tool_calls"][0]
    assert announce["index"] == 0
    assert announce["id"] == "c1"
    assert announce["function"]["name"] == "search"
    assert announce["function"]["arguments"] == ""
    # 3rd & 4th carry argument deltas keyed by the same OpenAI index
    arg_deltas = [
        p["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
        for p in payloads[2:4]
    ]
    assert arg_deltas == ['{"q":', '"x"}']
    assert payloads[-1]["choices"][0]["finish_reason"] == "tool_calls"


# ───────── serialize_response (non-stream) ─────────


def test_serialize_response_collects_text():
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-1", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="hello "),
        TextDelta(index=0, text="world"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=2, output_tokens=2)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    assert body["id"] == "chatcmpl-1"
    assert body["object"] == "chat.completion"
    msg = body["choices"][0]["message"]
    assert msg["content"] == "hello world"
    assert "tool_calls" not in msg
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["usage"]["total_tokens"] == 4


def test_serialize_response_collects_reasoning_content():
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-r", model="m"),
        ReasoningDelta(text="hidden"),
        MessageEnd(finish_reason="stop", usage=Usage()),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    assert body["choices"][0]["message"]["reasoning_content"] == "hidden"


def test_serialize_response_collects_tool_calls():
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-2", model="m"),
        ContentBlockStart(index=0, block_type="tool_use", tool_id="c1", tool_name="f"),
        ToolCallDelta(index=0, tool_id="c1", arguments_delta='{"x"'),
        ToolCallDelta(index=0, tool_id="c1", arguments_delta=':1}'),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="tool_calls", usage=Usage(input_tokens=1, output_tokens=1)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    msg = body["choices"][0]["message"]
    assert msg["content"] is None
    assert msg["tool_calls"][0]["id"] == "c1"
    assert msg["tool_calls"][0]["function"]["name"] == "f"
    assert json.loads(msg["tool_calls"][0]["function"]["arguments"]) == {"x": 1}
    assert body["choices"][0]["finish_reason"] == "tool_calls"


# ───────── finish_reason mapping ─────────


def test_finish_reason_legacy_function_call_maps_to_tool_calls():
    assert _map_finish("function_call") == "tool_calls"
    assert _map_finish("stop") == "stop"
    assert _map_finish("length") == "length"
    assert _map_finish("content_filter") == "content_filter"
    assert _map_finish(None) == "stop"
    assert _map_finish("unknown_garbage") == "stop"  # safe default


def test_finish_reason_ies_to_openai_error_demotes_to_stop():
    assert _IES_TO_OPENAI_FINISH["error"] == "stop"


# ───────── error_envelope ─────────


def test_error_envelope_openai_shape():
    err = BadRequestError("missing model")
    payload = json.loads(_adapter().error_envelope(err).decode())
    assert payload == {
        "error": {
            "message": "missing model",
            "type": "invalid_request",
            "code": "invalid_request",
        }
    }


def test_reasoning_cache_rehydrates_missing_assistant_reasoning_content():
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()
    _adapter().parse_upstream_response(json.dumps({
        "id": "chatcmpl-r",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "reasoning_content": "cached reasoning",
                "tool_calls": [{
                    "id": "call_cached", "type": "function",
                    "function": {"name": "search", "arguments": "{}"},
                }],
            },
            "finish_reason": "tool_calls",
        }],
    }).encode())

    req = _adapter().parse_request({
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "call_cached", "type": "function",
                "function": {"name": "search", "arguments": "{}"},
            }],
        }],
    })
    msg = _adapter().serialize_to_upstream(req)["messages"][0]
    assert msg["reasoning_content"] == "cached reasoning"


def test_missing_uncached_assistant_tool_call_does_not_inject_empty_reasoning_content():
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()
    req = _adapter().parse_request({
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "unknown", "type": "function",
                "function": {"name": "search", "arguments": "{}"},
            }],
        }],
    })
    msg = _adapter().serialize_to_upstream(req)["messages"][0]
    assert "reasoning_content" not in msg


def test_serialize_to_upstream_forwards_thinking_switch_from_metadata():
    # OpenAI SDK clients pass `extra_body={"thinking": {...}}`, which lands in
    # InternalRequest.metadata. The adapter must hoist it onto the upstream
    # body so MiMo actually enters thinking mode.
    body = _adapter().parse_request({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "thinking": {"type": "enabled"},
    })
    upstream = _adapter().serialize_to_upstream(body)
    assert upstream["thinking"] == {"type": "enabled"}


def test_serialize_to_upstream_omits_thinking_when_metadata_missing():
    body = _adapter().parse_request({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
    })
    upstream = _adapter().serialize_to_upstream(body)
    assert "thinking" not in upstream


def test_serialize_to_upstream_ignores_non_dict_thinking_metadata():
    body = _adapter().parse_request({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "thinking": "enabled",  # malformed — must not break upstream serialization
    })
    upstream = _adapter().serialize_to_upstream(body)
    assert "thinking" not in upstream


def test_reasoning_cache_commits_mid_stream_on_first_tool_id():
    """The cache must hold reasoning even if the upstream stream is cut short
    after the model emits its first tool_call — otherwise short-circuited
    streams (client disconnect, network blip) leave the cache empty for the
    very turns that need it most."""
    from gateway.reasoning_cache import clear_reasoning_cache, lookup_reasoning

    clear_reasoning_cache()

    chunks_payloads = [
        {"id": "x", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        # Reasoning streams first…
        {"choices": [{"index": 0,
                      "delta": {"reasoning_content": "Thinking it through. "},
                      "finish_reason": None}]},
        {"choices": [{"index": 0,
                      "delta": {"reasoning_content": "Decision made."},
                      "finish_reason": None}]},
        # …then the tool_call appears. Cache should commit at this point.
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{
                "index": 0, "id": "call_midstream", "type": "function",
                "function": {"name": "search", "arguments": '{"q":"x"}'},
            }]}, "finish_reason": None}]},
        # Simulate stream truncation: no further chunks, no MessageEnd.
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads)

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream])))

    _run(run())

    # Without the mid-stream commit, this would be None.
    assert lookup_reasoning(["call_midstream"]) == "Thinking it through. Decision made."


def test_reasoning_cache_stats_after_stream():
    """Stream completion path still works end-to-end (stats recorded)."""
    from gateway.reasoning_cache import clear_reasoning_cache, get_cache_stats

    clear_reasoning_cache()

    chunks_payloads = [
        {"id": "x", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        {"choices": [{"index": 0,
                      "delta": {"reasoning_content": "reason"},
                      "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {
            "tool_calls": [{
                "index": 0, "id": "call_end", "type": "function",
                "function": {"name": "f", "arguments": "{}"},
            }]}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream])))

    _run(run())

    stats = get_cache_stats()
    # At least one store (mid-stream commit). Second commit (post-stream)
    # overwrites; both count as stores in the current accounting.
    assert stats["stores"] >= 1
    assert stats["size"] == 1
