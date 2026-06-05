from __future__ import annotations

import asyncio
import json
import logging

import httpx

from gateway.transport import HttpxTransport


async def _collect(src):
    out = []
    async for chunk in src:
        out.append(chunk)
    return b"".join(out)


def test_post_json_logs_upstream_400_details(caplog):
    detail = {"error": {"message": "messages[0].content is required", "field": "messages"}}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json=detail, request=request)

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with caplog.at_level(logging.ERROR, logger="gateway.transport"):
        status, raw = asyncio.run(transport.post_json(
            "https://mimo.example/v1/chat/completions",
            {"model": "m", "messages": [{"role": "user"}]},
        ))

    asyncio.run(transport.close())
    assert status == 400
    assert json.loads(raw) == detail
    assert "Upstream MiMo API returned HTTP 400" in caplog.text
    assert "messages[0].content is required" in caplog.text
    assert "messages_count" in caplog.text


def test_post_stream_logs_and_preserves_upstream_error_body(caplog):
    raw = b'{"error":{"message":"model missing"}}'

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=raw, request=request)

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with caplog.at_level(logging.ERROR, logger="gateway.transport"):
        status, body_iter = asyncio.run(transport.post_stream(
            "https://mimo.example/v1/chat/completions",
            {"messages": []},
        ))
        body = asyncio.run(_collect(body_iter))

    asyncio.run(transport.close())
    assert status == 400
    assert body == raw
    assert "Upstream MiMo API returned HTTP 400" in caplog.text
    assert "model missing" in caplog.text
