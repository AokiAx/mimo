from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import pytest

from gateway.adapters import AnthropicAdapter, OpenAIChatAdapter, OpenAIResponsesAdapter
from gateway.core import AuthError, BadRequestError, RequestContext
from gateway.handler import GatewayHandler
from gateway.routing import Backend, BackendRegistry, Router


class FakeTransport:
    def __init__(self, responses: dict[str, tuple[int, bytes]]):
        self.responses = responses
        self.calls: list[str] = []

    async def post_json(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=60.0, proxy=None):
        self.calls.append(url)
        return self.responses[url]

    async def post_stream(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=600.0, proxy=None):
        raise AssertionError("stream not used")

    async def close(self):
        pass


class FakeStreamTransport:
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks
        self.calls: list[str] = []

    async def post_json(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=60.0, proxy=None):
        raise AssertionError("json not used")

    async def post_stream(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=600.0, proxy=None):
        self.calls.append(url)

        async def gen():
            for chunk in self.chunks:
                await asyncio.sleep(0)
                yield chunk

        return 200, gen()

    async def close(self):
        pass


class FakeMetrics:
    def __init__(self):
        self.rows = []

    def record(self, **kwargs):
        self.rows.append(kwargs)


@dataclass(frozen=True)
class Principal:
    allowed_models: tuple[str, ...]


def _payload(model: str = "m") -> bytes:
    return json.dumps({
        "id": "chatcmpl-test",
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2},
    }).encode()


def _body(model: str = "m") -> dict[str, Any]:
    return {"model": model, "messages": [{"role": "user", "content": "hi"}]}


def _stream_body(model: str = "m") -> dict[str, Any]:
    body = _body(model)
    body["stream"] = True
    return body


def _sse(payload: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(payload)}\n\n".encode()


async def _drain(stream):
    out = bytearray()
    async for chunk in stream:
        out.extend(chunk)
    return bytes(out)


def test_non_stream_retries_next_backend_on_5xx(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    a = Backend(backend_id="a", base_url="http://a", models=["m"])
    b = Backend(backend_id="b", base_url="http://b", models=["m"])
    a.record_success()
    b.record_success()
    transport = FakeTransport({
        "http://a/v1/chat/completions": (500, b"boom"),
        "http://b/v1/chat/completions": (200, _payload()),
    })
    metrics = FakeMetrics()
    handler = GatewayHandler(
        router=Router(BackendRegistry([a, b])),
        transport=transport,
        metrics=metrics,
    )

    content_type, stream, body = asyncio.run(
        handler.handle(RequestContext(), OpenAIChatAdapter(), _body())
    )

    assert content_type == "application/json"
    assert stream is None
    assert json.loads(body)["choices"][0]["message"]["content"] == "ok"
    assert transport.calls == [
        "http://a/v1/chat/completions",
        "http://b/v1/chat/completions",
    ]
    assert metrics.rows[0]["backend_id"] == "a"
    assert metrics.rows[0]["status_code"] == 500
    assert metrics.rows[0]["ttft_ms"] == 0
    assert metrics.rows[1]["backend_id"] == "b"
    assert metrics.rows[1]["status_code"] == 200
    assert metrics.rows[1]["ttft_ms"] > 0
    assert metrics.rows[1]["latency_ms"] >= metrics.rows[1]["ttft_ms"]


def test_stream_records_ttft_after_first_client_chunk(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["m"])
    backend.record_success()
    chunks = [
        _sse({
            "id": "chatcmpl-stream",
            "model": "m",
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        }),
        _sse({
            "id": "chatcmpl-stream",
            "model": "m",
            "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": None}],
        }),
        _sse({
            "id": "chatcmpl-stream",
            "model": "m",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2},
        }) + b"data: [DONE]\n\n",
    ]
    transport = FakeStreamTransport(chunks)
    metrics = FakeMetrics()
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
        metrics=metrics,
    )

    content_type, stream, body = asyncio.run(
        handler.handle(RequestContext(), OpenAIChatAdapter(), _stream_body())
    )
    assert content_type == "text/event-stream"
    assert body == b""
    assert stream is not None
    raw = asyncio.run(_drain(stream))

    assert b"data:" in raw
    assert transport.calls == ["http://a/v1/chat/completions"]
    assert len(metrics.rows) == 1
    row = metrics.rows[0]
    assert row["status_code"] == 200
    assert row["ttft_ms"] > 0
    assert row["latency_ms"] >= row["ttft_ms"]
    assert row["prompt_tokens"] == 1
    assert row["completion_tokens"] == 2


def test_allowed_models_are_enforced_before_routing(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["m"])
    backend.record_success()
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=FakeTransport({"http://a/v1/chat/completions": (200, _payload())}),
    )
    ctx = RequestContext(principal=Principal(allowed_models=("other",)))

    with pytest.raises(AuthError):
        asyncio.run(handler.handle(ctx, OpenAIChatAdapter(), _body("m")))


def test_non_stream_4xx_does_not_mark_backend_failure(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["m"])
    backend.record_success()
    raw = json.dumps({
        "error": {
            "code": "400",
            "message": "Param Incorrect",
            "param": "tools[6] is missing function.name",
            "type": "",
        }
    }).encode()
    transport = FakeTransport({"http://a/v1/chat/completions": (400, raw)})
    metrics = FakeMetrics()
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
        metrics=metrics,
    )

    with pytest.raises(BadRequestError) as exc:
        asyncio.run(handler.handle(RequestContext(), OpenAIChatAdapter(), _body()))

    assert exc.value.message == "客户端请求体参数不符合 MiMo API 要求"
    assert exc.value.details["upstream_error"]["param"] == "tools[6] is missing function.name"
    assert backend.total_failures == 0
    assert backend.health == "alive"
    assert metrics.rows[0]["status_code"] == 400


def test_user_request_5xx_does_not_mark_backend_failure(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["m"])
    backend.record_success()
    before_successes = backend.total_requests
    transport = FakeTransport({"http://a/v1/chat/completions": (500, b"bad gateway")})
    metrics = FakeMetrics()
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
        metrics=metrics,
    )

    # After exhausting all (here: one) backends on a retryable 5xx, the gateway
    # surfaces a friendly high-load 503 rather than the raw per-backend error.
    from gateway.core import BackendUnavailableError
    with pytest.raises(BackendUnavailableError):
        asyncio.run(handler.handle(RequestContext(), OpenAIChatAdapter(), _body()))

    assert backend.total_failures == 0
    assert backend.consecutive_failures == 0
    assert backend.health == "alive"
    assert backend.total_requests == before_successes
    assert metrics.rows[0]["status_code"] == 500


def test_user_request_success_does_not_update_backend_rating(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["m"])
    backend.record_success()
    backend.ewma_latency_ms = 123.0
    before_successes = backend.total_requests
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=FakeTransport({"http://a/v1/chat/completions": (200, _payload())}),
    )

    content_type, stream, body = asyncio.run(
        handler.handle(RequestContext(), OpenAIChatAdapter(), _body())
    )

    assert content_type == "application/json"
    assert stream is None
    assert json.loads(body)["choices"][0]["message"]["content"] == "ok"
    assert backend.health == "alive"
    assert backend.total_requests == before_successes
    assert backend.ewma_latency_ms == 123.0


def test_openai_image_request_rejected_for_text_model_before_routing(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["mimo-v2.5-pro"])
    backend.record_success()
    transport = FakeTransport({
        "http://a/v1/chat/completions": (200, _payload("mimo-v2.5-pro")),
    })
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
    )
    body = {
        "model": "mimo-v2.5-pro",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
            ],
        }],
    }

    with pytest.raises(BadRequestError) as exc:
        asyncio.run(handler.handle(RequestContext(), OpenAIChatAdapter(), body))

    assert "该模型不支持多模态输入" in exc.value.message
    assert transport.calls == []


def test_openai_image_request_allowed_for_mimo_multimodal_model(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["mimo-v2-omni"])
    backend.record_success()
    transport = FakeTransport({
        "http://a/v1/chat/completions": (200, _payload("mimo-v2-omni")),
    })
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
    )
    body = {
        "model": "mimo-v2-omni",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
            ],
        }],
    }

    content_type, stream, raw = asyncio.run(
        handler.handle(RequestContext(), OpenAIChatAdapter(), body)
    )

    assert content_type == "application/json"
    assert stream is None
    assert json.loads(raw)["choices"][0]["message"]["content"] == "ok"
    assert transport.calls == ["http://a/v1/chat/completions"]


def test_responses_image_request_rejected_for_text_model_before_routing(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["mimo-v2.5-pro"])
    backend.record_success()
    transport = FakeTransport({
        "http://a/v1/chat/completions": (200, _payload("mimo-v2.5-pro")),
    })
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
    )
    body = {
        "model": "mimo-v2.5-pro",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "what is this?"},
                {"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
            ],
        }],
    }

    with pytest.raises(BadRequestError):
        asyncio.run(handler.handle(RequestContext(), OpenAIResponsesAdapter(), body))

    assert transport.calls == []


def test_anthropic_image_request_rejected_for_text_model_before_routing(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["mimo-v2.5-pro"])
    backend.record_success()
    transport = FakeTransport({
        "http://a/anthropic/v1/messages": (200, b"{}"),
    })
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
    )
    body = {
        "model": "mimo-v2.5-pro",
        "max_tokens": 64,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "AAAA",
                }},
            ],
        }],
    }

    with pytest.raises(BadRequestError) as exc:
        asyncio.run(handler.handle(RequestContext(), AnthropicAdapter(), body))

    assert exc.value.details["unsupported_input"] == "image"
    assert transport.calls == []


def test_anthropic_url_image_request_rejected_for_text_model_before_routing(monkeypatch):
    monkeypatch.setattr("gateway.model_groups_store.resolve", lambda model, proto: model)
    backend = Backend(backend_id="a", base_url="http://a", models=["mimo-v2.5-pro"])
    backend.record_success()
    transport = FakeTransport({
        "http://a/anthropic/v1/messages": (200, b"{}"),
    })
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
    )
    body = {
        "model": "mimo-v2.5-pro",
        "max_tokens": 64,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image", "source": {
                    "type": "url", "url": "https://example.com/image.png",
                }},
            ],
        }],
    }

    with pytest.raises(BadRequestError) as exc:
        asyncio.run(handler.handle(RequestContext(), AnthropicAdapter(), body))

    assert exc.value.details["unsupported_input"] == "image"
    assert transport.calls == []
