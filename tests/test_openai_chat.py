"""Unit tests for gateway.adapters.openai_chat and gateway.routes TTS helpers.

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
  * /v1/audio/speech helper translation / extraction
"""
from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator

import pytest

from gateway.adapters.openai_chat import (
    OpenAIChatAdapter,
    _IES_TO_OPENAI_FINISH,
    _map_finish,
    iter_sse_data,
)
from gateway.audio_speech import AudioSpeechRequest
from gateway.core import (
    AudioDelta,
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
from gateway.routes import _extract_audio_response_bytes, _translate_audio_speech_request


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


def test_parse_request_preserves_assistant_audio_payload():
    req = _adapter().parse_request({
        "model": "mimo-v2.5-tts",
        "messages": [{
            "role": "assistant",
            "content": None,
            "audio": {"data": "QUJD", "format": "wav"},
        }],
    })
    msg = req.messages[0]
    assert msg.role == "assistant"
    assert msg.audio == {"data": "QUJD", "format": "wav"}


def test_conversation_key_hash_includes_assistant_audio_payload():
    audio_req = _adapter().parse_request({
        "model": "mimo-v2.5-tts",
        "messages": [{
            "role": "assistant",
            "content": None,
            "audio": {"data": "QUJD", "format": "wav"},
        }],
    })
    no_audio_req = _adapter().parse_request({
        "model": "mimo-v2.5-tts",
        "messages": [{
            "role": "assistant",
            "content": None,
        }],
    })
    assert _adapter().serialize_to_upstream(audio_req)["messages"][0]["audio"] == {"data": "QUJD", "format": "wav"}
    from gateway.adapters.openai_chat import _conversation_key_for_request
    assert _conversation_key_for_request(audio_req.messages) != _conversation_key_for_request(no_audio_req.messages)


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


def test_serialize_to_upstream_backfills_empty_assistant_content():
    # Clients sometimes re-send {role: assistant, content: null} stubs from
    # earlier turns. MiMo would 400; the adapter must backfill so the message
    # carries at least one of content / reasoning_content / tool_calls.
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[
            InternalMessage(role="user", content=[InternalContent(type="text", text="hi")]),
            InternalMessage(role="assistant", content=[]),
        ],
    )
    msg = _adapter().serialize_to_upstream(req)["messages"][1]
    assert msg["content"] == " "
    assert "tool_calls" not in msg
    assert "reasoning_content" not in msg


def test_serialize_to_upstream_leaves_tool_call_only_assistant_null_content():
    # When tool_calls are present, content=None is valid OpenAI semantics and
    # MiMo accepts it. Don't backfill in that case.
    req = InternalRequest(
        model="m", max_tokens=128,
        messages=[InternalMessage(role="assistant", content=[
            InternalContent(
                type="tool_use", tool_id="t1", tool_name="f", tool_input={},
            ),
        ])],
    )
    msg = _adapter().serialize_to_upstream(req)["messages"][0]
    assert msg["content"] is None
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
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
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
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
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
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
    assert any(isinstance(e, ReasoningDelta) and e.text == "thinking" for e in events)


def test_parse_upstream_response_preserves_audio_payload():
    payload = json.dumps({
        "id": "chatcmpl-audio",
        "model": "mimo-v2.5-tts",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": None,
                "audio": {"data": "QUJD", "format": "wav"},
            },
            "finish_reason": "stop",
        }],
    }).encode()
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
    audio = next(e for e in events if isinstance(e, AudioDelta))
    assert audio.data == "QUJD"
    assert audio.format == "wav"


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
        return await _events(_adapter().parse_upstream_stream(_gen(raw_chunks), conversation_key="test-conv"))

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
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

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
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

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
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

    events = _run(run())
    assert any(isinstance(e, ReasoningDelta) and e.text == "think" for e in events)
    assert "answer" == "".join(e.text for e in events if isinstance(e, TextDelta))


def test_parse_upstream_stream_collects_audio_delta_payload():
    chunks_payloads = [
        {"id": "x1", "model": "mimo-v2.5-tts",
         "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"audio": {"data": "QU", "format": "pcm16"}}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"audio": {"data": "JD"}}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

    events = _run(run())
    audio_deltas = [e for e in events if isinstance(e, AudioDelta)]
    assert len(audio_deltas) == 2
    assert audio_deltas[0].data == "QU"
    assert audio_deltas[0].format == "pcm16"
    assert audio_deltas[1].data == "JD"
    assert audio_deltas[1].format == "pcm16"
    end = events[-1]
    assert isinstance(end, MessageEnd) and end.finish_reason == "stop"


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


def test_serialize_response_stream_emits_audio_delta_chunk():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="x", model="mimo-v2.5-tts")
        yield AudioDelta(data="QUJD", format="pcm16")
        yield MessageEnd(finish_reason="stop", usage=Usage())

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    payloads = [
        json.loads(c[6:].decode())
        for c in raw.split(b"\n\n")
        if c and c != b"data: [DONE]" and c.startswith(b"data: ")
    ]
    assert payloads[0]["choices"][0]["delta"] == {"role": "assistant"}
    assert payloads[1]["choices"][0]["delta"] == {
        "audio": {"data": "QUJD", "format": "pcm16"},
    }


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


def test_serialize_response_collects_audio_payload():
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-a", model="mimo-v2.5-tts"),
        AudioDelta(data="QU", format="wav"),
        AudioDelta(data="JD"),
        MessageEnd(finish_reason="stop", usage=Usage()),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    msg = body["choices"][0]["message"]
    assert msg["audio"] == {"data": "QUJD", "format": "wav"}
    assert msg["content"] is None


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


def test_serialize_response_backfills_empty_assistant_content():
    # Returning {content: null} with no tool_calls / reasoning poisons client
    # history: Claude Code preserves the null and re-sends it next turn,
    # causing MiMo to 400. Emit a whitespace placeholder to break the loop.
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-empty", model="m"),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=0)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    msg = body["choices"][0]["message"]
    assert msg["content"] == " "
    assert "tool_calls" not in msg
    assert "reasoning_content" not in msg


def test_serialize_response_does_not_backfill_whitespace_when_audio_present():
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-audio-only", model="mimo-v2.5-tts"),
        AudioDelta(data="QUJD", format="wav"),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=0)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    msg = body["choices"][0]["message"]
    assert msg["audio"] == {"data": "QUJD", "format": "wav"}
    assert msg["content"] is None


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


# ───────── /v1/audio/speech helpers ─────────


def test_translate_audio_speech_request_builds_chat_payload():
    payload = AudioSpeechRequest(
        input="你好，世界",
        model="tts-1",
        voice="alloy",
        response_format="wav",
        instructions="用开心一点的语气",
    )
    translated = _translate_audio_speech_request(payload)
    assert translated == {
        "model": "mimo-v2.5-tts",
        "messages": [
            {"role": "user", "content": "用开心一点的语气"},
            {"role": "assistant", "content": "你好，世界"},
        ],
        "audio": {"format": "wav", "voice": "mimo_default"},
        "stream": False,
    }


def test_translate_audio_speech_request_supports_mimo_v2_tts():
    payload = AudioSpeechRequest(
        input="你好",
        model="mimo-v2-tts",
        voice="mimo_meet",
        response_format="mp3",
    )
    translated = _translate_audio_speech_request(payload)
    assert translated == {
        "model": "mimo-v2-tts",
        "messages": [
            {"role": "assistant", "content": "你好"},
        ],
        "audio": {"format": "mp3", "voice": "mimo_meet"},
        "stream": False,
    }


def test_translate_audio_speech_request_supports_voice_design_model():
    payload = AudioSpeechRequest(
        input="你好，今天过得怎么样？",
        model="mimo-v2.5-tts-voicedesign",
        voice_description="年轻女声，温柔一点，像朋友聊天",
        voice="alloy",
        response_format="wav",
        optimize_text_preview=True,
    )
    translated = _translate_audio_speech_request(payload)
    assert translated == {
        "model": "mimo-v2.5-tts-voicedesign",
        "messages": [
            {"role": "user", "content": "年轻女声，温柔一点，像朋友聊天"},
            {"role": "assistant", "content": "你好，今天过得怎么样？"},
        ],
        "audio": {
            "format": "wav",
            "voice": "mimo_default",
            "optimize_text_preview": True,
        },
        "stream": False,
    }


def test_translate_audio_speech_request_voice_design_requires_voice_description():
    payload = AudioSpeechRequest(
        input="你好",
        model="mimo-v2.5-tts-voicedesign",
    )
    with pytest.raises(ValueError, match="voice_description"):
        _translate_audio_speech_request(payload)


def test_translate_audio_speech_request_supports_voice_clone_model():
    payload = AudioSpeechRequest(
        input="你好，来试一下克隆音色",
        model="mimo-v2.5-tts-voiceclone",
        instructions="语气平稳一些",
        voice_sample_base64="QUJDRA==",
        voice_sample_mime_type="audio/wav",
        response_format="flac",
    )
    translated = _translate_audio_speech_request(payload)
    assert translated == {
        "model": "mimo-v2.5-tts-voiceclone",
        "messages": [
            {"role": "user", "content": "语气平稳一些"},
            {"role": "assistant", "content": "你好，来试一下克隆音色"},
        ],
        "audio": {
            "format": "flac",
            "voice": "data:audio/wav;base64,QUJDRA==",
        },
        "stream": False,
    }


def test_translate_audio_speech_request_voice_clone_requires_sample_fields():
    payload = AudioSpeechRequest(
        input="你好",
        model="mimo-v2.5-tts-voiceclone",
    )
    with pytest.raises(ValueError, match="voice_sample_base64"):
        _translate_audio_speech_request(payload)

    payload = AudioSpeechRequest(
        input="你好",
        model="mimo-v2.5-tts-voiceclone",
        voice_sample_base64="QUJDRA==",
    )
    with pytest.raises(ValueError, match="voice_sample_mime_type"):
        _translate_audio_speech_request(payload)


def test_extract_audio_response_bytes_reads_message_audio():
    payload = {
        "choices": [{
            "message": {
                "audio": {
                    "data": base64.b64encode(b"abc").decode(),
                    "format": "mp3",
                }
            }
        }]
    }
    audio_bytes, audio_format = _extract_audio_response_bytes(payload, fallback_format="wav")
    assert audio_bytes == b"abc"
    assert audio_format == "mp3"


def test_extract_audio_response_bytes_uses_fallback_format_when_missing():
    payload = {
        "choices": [{
            "message": {
                "audio": {
                    "data": base64.b64encode(b"xyz").decode(),
                }
            }
        }]
    }
    audio_bytes, audio_format = _extract_audio_response_bytes(payload, fallback_format="wav")
    assert audio_bytes == b"xyz"
    assert audio_format == "wav"


def test_extract_audio_response_bytes_raises_when_audio_missing():
    with pytest.raises(ValueError, match="没有音频数据"):
        _extract_audio_response_bytes({"choices": [{"message": {}}]}, fallback_format="wav")


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
    from gateway.adapters.openai_chat import _conversation_key_for_request
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()
    # Capture must happen under the same conversation_key the next-turn
    # lookup will derive. In serialize_to_upstream, the lookup for the
    # assistant message at index 0 hashes ``messages[:0]`` = empty list.
    # So we capture under hash([]) too — by passing an empty messages list.
    capture_conv = _conversation_key_for_request([])

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
    }).encode(), conversation_key=capture_conv)

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


# ───────── Conversation-isolation safety tests ─────────


def test_reasoning_cache_isolates_different_conversations_with_same_tool_id():
    """Security-critical: if two unrelated conversations happen to use the
    same upstream tool_call_id (collision or attacker forging the id), the
    cache must NOT cross-rehydrate reasoning between them. This is the
    confused-deputy vector Codex flagged on PR #34."""
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()

    # Conversation A: produces tool_call "call_x" with reasoning "secret_A".
    _adapter().parse_upstream_response(json.dumps({
        "id": "chatcmpl-A", "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant", "content": None,
                "reasoning_content": "secret_A",
                "tool_calls": [{"id": "call_x", "type": "function",
                                "function": {"name": "f", "arguments": "{}"}}],
            },
            "finish_reason": "tool_calls",
        }],
    }).encode(), conversation_key="conv-A")

    # Conversation B (different message history) tries to look up call_x.
    # In serialize_to_upstream the lookup uses B's prefix-hash, which
    # differs from "conv-A" → must miss.
    req_b = _adapter().parse_request({
        "model": "m",
        "messages": [
            # Different user message → different prefix hash than conv A.
            {"role": "user", "content": "totally different question"},
            {"role": "assistant", "content": None,
             "tool_calls": [{"id": "call_x", "type": "function",
                             "function": {"name": "f", "arguments": "{}"}}]},
        ],
    })
    msg_b = _adapter().serialize_to_upstream(req_b)["messages"][1]
    assert "reasoning_content" not in msg_b, (
        "Cross-conversation leak: B should not see A's reasoning"
    )


def test_reasoning_cache_continues_within_same_conversation():
    """The flip side of isolation: a real next-turn within the same
    conversation must still rehydrate. Otherwise the security fix would
    just break the feature."""
    from gateway.adapters.openai_chat import _conversation_key_for_request
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()

    # First turn's request was [user "ask Q"]. Capture the response under
    # the conversation key for that exact prefix.
    first_turn_messages = [
        InternalMessage(role="user", content=[InternalContent(type="text", text="ask Q")]),
    ]
    capture_conv = _conversation_key_for_request(first_turn_messages)

    _adapter().parse_upstream_response(json.dumps({
        "id": "chatcmpl-first", "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant", "content": None,
                "reasoning_content": "step by step thinking",
                "tool_calls": [{"id": "call_real", "type": "function",
                                "function": {"name": "search", "arguments": "{}"}}],
            },
            "finish_reason": "tool_calls",
        }],
    }).encode(), conversation_key=capture_conv)

    # Second turn: same prefix + assistant tool_use + tool result.
    req = _adapter().parse_request({
        "model": "m",
        "messages": [
            {"role": "user", "content": "ask Q"},
            {"role": "assistant", "content": None,
             "tool_calls": [{"id": "call_real", "type": "function",
                             "function": {"name": "search", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "call_real", "content": "answer"},
            {"role": "user", "content": "and?"},
        ],
    })
    # The assistant message is at index 1 → its prefix is messages[:1] =
    # [user "ask Q"] → same conversation key we wrote under.
    msg = _adapter().serialize_to_upstream(req)["messages"][1]
    assert msg["reasoning_content"] == "step by step thinking"


def test_reasoning_cache_isolates_different_thinking_configs():
    """OpenAI Chat puts ``thinking`` in ``req.metadata`` (not in
    messages). Two requests with identical messages + tools but
    different thinking budgets must produce different scopes —
    otherwise user A's reasoning could leak into user B's request when
    they happen to share message history but use different thinking
    configs.
    """
    from gateway.adapters.openai_chat import _conversation_key_for_request
    from gateway.reasoning_cache import clear_reasoning_cache

    clear_reasoning_cache()

    common_messages = [
        InternalMessage(role="user", content=[InternalContent(type="text", text="ask")]),
    ]
    # Conversation A: thinking budget 8000
    key_a = _conversation_key_for_request(
        common_messages, thinking={"type": "enabled", "budget_tokens": 8000},
    )
    # Conversation B: same messages, different thinking
    key_b = _conversation_key_for_request(
        common_messages, thinking={"type": "enabled", "budget_tokens": 200},
    )
    # Conversation C: same messages, no thinking
    key_c = _conversation_key_for_request(common_messages, thinking=None)
    assert key_a != key_b, "Same messages + different thinking budget must have different scope"
    assert key_a != key_c, "Thinking-on vs thinking-off must have different scope"
    assert key_b != key_c


def test_reasoning_write_does_not_populate_no_thinking_fallback_scope():
    """Fallback lookup is read-only compatibility, not a second write scope."""
    from gateway.adapters.openai_chat import _conversation_key_for_request
    from gateway.reasoning_cache import clear_reasoning_cache, lookup_reasoning

    clear_reasoning_cache()

    messages = [
        InternalMessage(role="user", content=[InternalContent(type="text", text="ask")]),
    ]
    scoped_key = _conversation_key_for_request(
        messages,
        thinking={"type": "enabled", "budget_tokens": 8000},
    )
    fallback_key = _conversation_key_for_request(messages, thinking=None)

    body = json.dumps({
        "id": "chatcmpl-test",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "reasoning_content": "scoped reasoning",
                "tool_calls": [{
                    "id": "call_shared",
                    "type": "function",
                    "function": {"name": "search", "arguments": "{}"},
                }],
            },
            "finish_reason": "tool_calls",
        }],
    }).encode()

    _adapter().parse_upstream_response(body, conversation_key=scoped_key)

    assert lookup_reasoning(["call_shared"], conversation_key=scoped_key) == "scoped reasoning"
    assert lookup_reasoning(["call_shared"], conversation_key=fallback_key) is None


def test_remember_reasoning_rejects_empty_conversation_key():
    """Fail loud, not silent: caller passing an empty string for
    conversation_key is a bug, not a "use the default scope" instruction."""
    from gateway.reasoning_cache import (
        clear_reasoning_cache,
        lookup_reasoning,
        remember_reasoning,
    )

    clear_reasoning_cache()
    with pytest.raises(ValueError):
        remember_reasoning("r", ["tid"], conversation_key="")
    with pytest.raises(ValueError):
        lookup_reasoning(["tid"], conversation_key="")


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


def test_serialize_to_upstream_preserves_assistant_audio_payload():
    req = _adapter().parse_request({
        "model": "mimo-v2.5-tts",
        "messages": [{
            "role": "assistant",
            "content": None,
            "audio": {"data": "QUJD", "format": "wav"},
        }],
    })
    upstream = _adapter().serialize_to_upstream(req)
    assert upstream["messages"][0]["audio"] == {"data": "QUJD", "format": "wav"}
    assert upstream["messages"][0]["content"] is None


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
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

    _run(run())

    # Without the mid-stream commit, this would be None.
    assert lookup_reasoning(
        ["call_midstream"], conversation_key="test-conv",
    ) == "Thinking it through. Decision made."


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
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

    _run(run())

    stats = get_cache_stats()
    # At least one store (mid-stream commit). Second commit (post-stream)
    # overwrites; both count as stores in the current accounting.
    assert stats["stores"] >= 1
    assert stats["size"] == 1


# ───────── usage details forwarding ─────────


def test_parse_upstream_response_forwards_full_usage_details():
    """MiMo's OpenAI endpoint returns nested cache / reasoning / multimodal
    breakdowns; the adapter must surface every field so NewAPI can bill at
    cached-rate and so clients can see real token splits."""
    payload = json.dumps({
        "id": "chatcmpl-u",
        "model": "m",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 1234,
            "completion_tokens": 56,
            "total_tokens": 1290,
            "prompt_tokens_details": {
                "cached_tokens": 1000,
                "audio_tokens": 7,
                "image_tokens": 11,
                "video_tokens": 13,
            },
            "completion_tokens_details": {"reasoning_tokens": 42},
            "web_search_usage": {"tool_usage": 2, "page_usage": 9},
        },
    }).encode()
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
    end = next(e for e in events if isinstance(e, MessageEnd))
    u = end.usage
    assert u.input_tokens == 1234
    assert u.output_tokens == 56
    assert u.cached_tokens == 1000
    assert u.audio_tokens == 7
    assert u.image_tokens == 11
    assert u.video_tokens == 13
    assert u.reasoning_tokens == 42
    assert u.web_search_usage == {"tool_usage": 2, "page_usage": 9}


def test_parse_upstream_response_handles_missing_usage_details_gracefully():
    """Older upstream / non-thinking modes won't include the nested objects;
    we should default to 0 rather than crash."""
    payload = json.dumps({
        "id": "x", "model": "m",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
    }).encode()
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
    end = next(e for e in events if isinstance(e, MessageEnd))
    assert end.usage.cached_tokens == 0
    assert end.usage.reasoning_tokens == 0
    assert end.usage.web_search_usage is None


def test_parse_upstream_response_tolerates_malformed_nested_usage():
    """If upstream returns prompt_tokens_details as a string or null (e.g.
    a glitch), we shouldn't break — IES Usage must always be constructable."""
    payload = json.dumps({
        "id": "x", "model": "m",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 5, "completion_tokens": 2,
            "prompt_tokens_details": "garbage",
            "completion_tokens_details": None,
            "web_search_usage": ["unexpected", "list"],
        },
    }).encode()
    events = _adapter().parse_upstream_response(payload, conversation_key="test-conv")
    end = next(e for e in events if isinstance(e, MessageEnd))
    assert end.usage.input_tokens == 5
    assert end.usage.cached_tokens == 0
    assert end.usage.web_search_usage is None


def test_parse_upstream_stream_forwards_full_usage_details():
    """Streaming uses the final chunk's usage object — same fields apply."""
    chunks_payloads = [
        {"id": "x", "model": "m",
         "choices": [{"index": 0, "delta": {"role": "assistant"},
                      "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {"content": "hi"},
                      "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {"choices": [], "usage": {
            "prompt_tokens": 100, "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 80, "image_tokens": 4},
            "completion_tokens_details": {"reasoning_tokens": 3},
        }},
    ]
    bytes_stream = b"".join(_sse(p) for p in chunks_payloads) + b"data: [DONE]\n\n"

    async def run():
        return await _events(_adapter().parse_upstream_stream(_gen([bytes_stream]), conversation_key="test-conv"))

    events = _run(run())
    end = next(e for e in events if isinstance(e, MessageEnd))
    assert end.usage.input_tokens == 100
    assert end.usage.cached_tokens == 80
    assert end.usage.image_tokens == 4
    assert end.usage.reasoning_tokens == 3


def test_serialize_response_emits_usage_details_in_openai_shape():
    """Verify the wire shape matches OpenAI's standard schema so NewAPI's
    billing logic finds prompt_tokens_details.cached_tokens where expected."""
    usage = Usage(
        input_tokens=1234, output_tokens=56,
        cached_tokens=1000, audio_tokens=7, image_tokens=11, video_tokens=13,
        reasoning_tokens=42,
        web_search_usage={"tool_usage": 2, "page_usage": 9},
    )
    events: list[InternalEvent] = [
        MessageStart(message_id="chatcmpl-u", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="ok"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=usage),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    u = body["usage"]
    assert u["prompt_tokens"] == 1234
    assert u["completion_tokens"] == 56
    assert u["total_tokens"] == 1290
    assert u["prompt_tokens_details"] == {
        "cached_tokens": 1000, "audio_tokens": 7,
        "image_tokens": 11, "video_tokens": 13,
    }
    assert u["completion_tokens_details"] == {"reasoning_tokens": 42}
    assert u["web_search_usage"] == {"tool_usage": 2, "page_usage": 9}


def test_serialize_response_omits_empty_usage_details_objects():
    """No multimodal / cache / web-search data → don't pollute output with
    empty nested objects. Keeps the response wire-compatible with vanilla
    OpenAI clients that expect a minimal usage block."""
    usage = Usage(input_tokens=5, output_tokens=2)
    events: list[InternalEvent] = [
        MessageStart(message_id="x", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="ok"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=usage),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    u = body["usage"]
    assert u == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    assert "prompt_tokens_details" not in u
    assert "completion_tokens_details" not in u
    assert "web_search_usage" not in u


def test_serialize_stream_emits_usage_details_in_final_chunk():
    """Stream final chunk carries the usage block exactly like non-stream."""
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="x", model="m")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="ok")
        yield ContentBlockEnd(index=0)
        yield MessageEnd(
            finish_reason="stop",
            usage=Usage(
                input_tokens=100, output_tokens=5,
                cached_tokens=80, reasoning_tokens=3,
            ),
        )

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    payloads = [
        json.loads(c[6:].decode())
        for c in raw.split(b"\n\n")
        if c and c != b"data: [DONE]" and c.startswith(b"data: ")
    ]
    final = payloads[-1]
    assert final["usage"]["prompt_tokens"] == 100
    assert final["usage"]["prompt_tokens_details"] == {"cached_tokens": 80}
    assert final["usage"]["completion_tokens_details"] == {"reasoning_tokens": 3}
