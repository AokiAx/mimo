"""Tests for gateway.anthropic_passthrough.

Covers:
  * patch_request_thinking — rehydrate from cache, skip when present, miss
  * scan_response_json — populate cache from final response
  * tee_stream_capture_thinking — populate cache from streamed thinking_delta
  * end-to-end round-trip: response harvest → next-turn patch
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from gateway.anthropic_passthrough import (
    patch_request_thinking,
    scan_response_json,
    tee_stream_capture_thinking,
)
from gateway.reasoning_cache import (
    clear_reasoning_cache,
    get_cache_stats,
    remember_reasoning,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_reasoning_cache()
    yield
    clear_reasoning_cache()


def _run(coro):
    return asyncio.run(coro)


async def _gen(chunks: list[bytes]) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


async def _drain(it: AsyncIterator[bytes]) -> bytes:
    out = b""
    async for chunk in it:
        out += chunk
    return out


# ───────── patch_request_thinking ─────────


def test_patch_rehydrates_when_cache_has_matching_ids():
    remember_reasoning("I planned the search.", ["toolu_a", "toolu_b"])
    body = {
        "model": "mimo-v2.5-pro",
        "messages": [
            {"role": "user", "content": "hi"},
            {
                "role": "assistant",
                "content": [
                    # Client dropped the thinking block, leaving only tool_use.
                    {"type": "tool_use", "id": "toolu_a", "name": "search", "input": {}},
                    {"type": "tool_use", "id": "toolu_b", "name": "calc", "input": {}},
                ],
            },
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_a", "content": "ok"},
                {"type": "tool_result", "tool_use_id": "toolu_b", "content": "42"},
            ]},
        ],
    }
    n = patch_request_thinking(body)
    assert n == 1
    asst_content = body["messages"][1]["content"]
    assert asst_content[0] == {"type": "thinking", "thinking": "I planned the search."}
    # Original blocks preserved in order
    assert asst_content[1]["type"] == "tool_use" and asst_content[1]["id"] == "toolu_a"
    assert asst_content[2]["type"] == "tool_use" and asst_content[2]["id"] == "toolu_b"


def test_patch_leaves_message_alone_when_thinking_already_present():
    remember_reasoning("cached", ["toolu_a"])
    body = {
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "real reasoning", "signature": "sig"},
                {"type": "tool_use", "id": "toolu_a", "name": "f", "input": {}},
            ],
        }],
    }
    n = patch_request_thinking(body)
    assert n == 0
    content = body["messages"][0]["content"]
    # Untouched
    assert content[0] == {"type": "thinking", "thinking": "real reasoning", "signature": "sig"}


def test_patch_skips_when_no_tool_use_blocks():
    remember_reasoning("cached", ["toolu_a"])
    body = {
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello"}],
        }],
    }
    n = patch_request_thinking(body)
    assert n == 0


def test_patch_skips_on_cache_miss():
    body = {
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "toolu_unknown", "name": "f", "input": {}}],
        }],
    }
    n = patch_request_thinking(body)
    assert n == 0
    # Original content unchanged
    assert body["messages"][0]["content"] == [
        {"type": "tool_use", "id": "toolu_unknown", "name": "f", "input": {}},
    ]


def test_patch_handles_multiple_assistant_messages_independently():
    remember_reasoning("first reasoning", ["toolu_1"])
    remember_reasoning("second reasoning", ["toolu_2"])
    body = {
        "model": "m",
        "messages": [
            {"role": "user", "content": "q1"},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_1", "name": "f", "input": {}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "x"},
            ]},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_2", "name": "g", "input": {}},
            ]},
        ],
    }
    n = patch_request_thinking(body)
    assert n == 2
    assert body["messages"][1]["content"][0]["thinking"] == "first reasoning"
    assert body["messages"][3]["content"][0]["thinking"] == "second reasoning"


def test_patch_tolerates_string_content_assistant():
    # Plain-string content can't have tool_use blocks, so nothing to patch.
    body = {
        "model": "m",
        "messages": [{"role": "assistant", "content": "Just text"}],
    }
    n = patch_request_thinking(body)
    assert n == 0


def test_patch_ignores_malformed_blocks():
    # Garbage blocks shouldn't crash or be treated as tool_use.
    body = {
        "model": "m",
        "messages": [{
            "role": "assistant",
            "content": [None, "string-block", {"type": "tool_use"}],  # missing id
        }],
    }
    n = patch_request_thinking(body)
    assert n == 0


# ───────── scan_response_json ─────────


def test_scan_response_populates_cache_for_future_turn():
    raw = json.dumps({
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "thinking", "thinking": "I'll search both.", "signature": "abc"},
            {"type": "tool_use", "id": "toolu_x", "name": "search", "input": {"q": "weather"}},
            {"type": "tool_use", "id": "toolu_y", "name": "calc", "input": {}},
        ],
        "model": "mimo-v2.5-pro",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 10, "output_tokens": 20},
    }).encode()
    scan_response_json(raw)

    # Now simulate next turn with thinking dropped
    body = {"model": "m", "messages": [{
        "role": "assistant",
        "content": [
            {"type": "tool_use", "id": "toolu_x", "name": "search", "input": {"q": "weather"}},
            {"type": "tool_use", "id": "toolu_y", "name": "calc", "input": {}},
        ],
    }]}
    assert patch_request_thinking(body) == 1
    assert body["messages"][0]["content"][0]["thinking"] == "I'll search both."


def test_scan_response_ignores_response_without_tool_use():
    raw = json.dumps({
        "content": [
            {"type": "thinking", "thinking": "no tool needed"},
            {"type": "text", "text": "Answer is 42."},
        ],
    }).encode()
    scan_response_json(raw)
    # Cache should have nothing (thinking without tool_use isn't keyable).
    stats = get_cache_stats()
    assert stats["size"] == 0
    assert stats["stores"] == 0


def test_scan_response_ignores_response_without_thinking():
    raw = json.dumps({
        "content": [
            {"type": "tool_use", "id": "toolu_z", "name": "f", "input": {}},
        ],
    }).encode()
    scan_response_json(raw)
    assert get_cache_stats()["size"] == 0


def test_scan_response_handles_malformed_json_gracefully():
    scan_response_json(b"not json {{{{")
    # No crash, no cache entry.
    assert get_cache_stats()["size"] == 0


# ───────── tee_stream_capture_thinking ─────────


def _sse(event: str, payload: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n".encode()


def test_tee_stream_passes_bytes_through_and_caches_thinking():
    frames = [
        _sse("message_start", {"type": "message_start", "message": {
            "id": "msg_1", "role": "assistant", "model": "m", "content": [],
        }}),
        _sse("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""},
        }),
        _sse("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Step one. "},
        }),
        _sse("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Step two."},
        }),
        _sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
        _sse("content_block_start", {
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "tool_use", "id": "toolu_stream", "name": "f", "input": {}},
        }),
        _sse("content_block_stop", {"type": "content_block_stop", "index": 1}),
        _sse("message_delta", {"type": "message_delta",
                               "delta": {"stop_reason": "tool_use"},
                               "usage": {"output_tokens": 5}}),
        _sse("message_stop", {"type": "message_stop"}),
    ]

    async def run():
        wrapped = tee_stream_capture_thinking(_gen(frames))
        return await _drain(wrapped)

    out = _run(run())
    # Every input byte must come back out unchanged (byte passthrough).
    assert out == b"".join(frames)

    # Cache populated by the message's tool_use id.
    body = {"model": "m", "messages": [{
        "role": "assistant",
        "content": [{"type": "tool_use", "id": "toolu_stream", "name": "f", "input": {}}],
    }]}
    assert patch_request_thinking(body) == 1
    assert body["messages"][0]["content"][0]["thinking"] == "Step one. Step two."


def test_tee_stream_with_arbitrary_chunk_boundaries():
    # Split SSE bytes mid-frame to stress the buffering.
    frames = (
        _sse("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""},
        })
        + _sse("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "splittable text"},
        })
        + _sse("content_block_start", {
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "tool_use", "id": "toolu_split", "name": "f", "input": {}},
        })
        + _sse("message_stop", {"type": "message_stop"})
    )
    # 13-byte chunks: chosen to land inside JSON bodies.
    chunks = [frames[i:i + 13] for i in range(0, len(frames), 13)]

    async def run():
        return await _drain(tee_stream_capture_thinking(_gen(chunks)))

    out = _run(run())
    assert out == frames

    body = {"model": "m", "messages": [{
        "role": "assistant",
        "content": [{"type": "tool_use", "id": "toolu_split", "name": "f", "input": {}}],
    }]}
    assert patch_request_thinking(body) == 1
    assert body["messages"][0]["content"][0]["thinking"] == "splittable text"


def test_tee_stream_without_tool_use_does_not_cache():
    # Thinking but no tool_use → nothing to key the cache by → don't store.
    frames = [
        _sse("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""},
        }),
        _sse("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "lonely thinking"},
        }),
        _sse("message_stop", {"type": "message_stop"}),
    ]

    async def run():
        await _drain(tee_stream_capture_thinking(_gen(frames)))

    _run(run())
    assert get_cache_stats()["size"] == 0


# ───────── reasoning_cache stats ─────────


def test_cache_stats_track_hits_and_misses():
    remember_reasoning("text", ["toolu_a"])
    s1 = get_cache_stats()
    assert s1["stores"] == 1
    assert s1["size"] == 1

    # Hit
    assert "text" == _lookup(["toolu_a"])
    # Miss
    assert _lookup(["toolu_unknown"]) is None

    s2 = get_cache_stats()
    assert s2["hits"] == 1
    assert s2["misses"] == 1


def test_cache_stats_track_evictions_on_overflow(monkeypatch):
    """LRU-dropped entries should show up in stats so operators can tell
    whether the cache is undersized."""
    import gateway.reasoning_cache as rc

    # Shrink the cap so we can force overflow with a few writes.
    monkeypatch.setattr(rc, "_MAX_ENTRIES", 2)
    for i in range(5):
        remember_reasoning(f"r{i}", [f"toolu_{i}"])

    s = get_cache_stats()
    # 5 stores total, 3 must have been evicted (5 - 2).
    assert s["stores"] == 5
    assert s["evictions"] == 3
    assert s["size"] == 2


def _lookup(ids):
    from gateway.reasoning_cache import lookup_reasoning
    return lookup_reasoning(ids)


# ───────── tee_stream usage capture ─────────


def test_tee_stream_captures_input_and_output_tokens():
    """The metrics layer needs token counts post-stream. Verify both
    message_start.input_tokens and message_delta.output_tokens reach the
    sink."""
    frames = [
        _sse("message_start", {
            "type": "message_start",
            "message": {
                "id": "msg_t", "role": "assistant", "model": "m",
                "content": [],
                "usage": {"input_tokens": 42, "output_tokens": 1},
            },
        }),
        _sse("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""},
        }),
        _sse("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "ok"},
        }),
        _sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
        _sse("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 17},
        }),
        _sse("message_stop", {"type": "message_stop"}),
    ]

    usage: dict[str, int] = {}

    async def run():
        await _drain(tee_stream_capture_thinking(_gen(frames), usage))

    _run(run())
    assert usage.get("input_tokens") == 42
    assert usage.get("output_tokens") == 17


def test_tee_stream_usage_sink_optional():
    """The sink parameter is optional — omitting it must not break."""
    frames = [_sse("message_stop", {"type": "message_stop"})]

    async def run():
        await _drain(tee_stream_capture_thinking(_gen(frames)))

    _run(run())  # Must not raise.
