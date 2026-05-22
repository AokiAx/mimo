from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import pytest

from gateway.adapters import OpenAIChatAdapter
from gateway.core import AuthError, RequestContext
from gateway.handler import GatewayHandler
from gateway.routing import Backend, BackendRegistry, Router


class FakeTransport:
    def __init__(self, responses: dict[str, tuple[int, bytes]]):
        self.responses = responses
        self.calls: list[str] = []

    async def post_json(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=60.0):
        self.calls.append(url)
        return self.responses[url]

    async def post_stream(self, url: str, body: dict[str, Any], *, headers=None, timeout_s=600.0):
        raise AssertionError("stream not used")

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
    assert metrics.rows[1]["backend_id"] == "b"
    assert metrics.rows[1]["status_code"] == 200


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
    transport = FakeTransport({"http://a/v1/chat/completions": (400, b"bad request")})
    metrics = FakeMetrics()
    handler = GatewayHandler(
        router=Router(BackendRegistry([backend])),
        transport=transport,
        metrics=metrics,
    )

    from gateway.core import UpstreamError
    with pytest.raises(UpstreamError):
        asyncio.run(handler.handle(RequestContext(), OpenAIChatAdapter(), _body()))

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

    from gateway.core import UpstreamError
    with pytest.raises(UpstreamError):
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
