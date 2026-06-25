"""Regression test for the 2026-06-25 deploy failure root cause.

MiMo's ws-proxy rejects the /ws/proxy upgrade with HTTP 401
(X-Ws-Proxy-Error: idc_route_decision_unavailable) unless the handshake carries
the session Cookie — it resolves the claw's IDC route from that cookie. Our WS
callers historically sent only ticket/userId in the query string and no cookies.
The fix adds Cookie + Origin headers (what the browser sends); this test pins
that every /ws/proxy connect includes them.
"""
from __future__ import annotations

import asyncio
import json

import pytest

import app

SAMPLE_COOKIES = [
    {"name": "serviceToken", "value": "tok-abc"},
    {"name": "userId", "value": "u-42"},
]


class _FakeWS:
    def __init__(self) -> None:
        self._outbox = ['{"type":"event","event":"connect.challenge"}']

    async def recv(self) -> str:
        assert self._outbox, "recv() with empty outbox"
        return self._outbox.pop(0)

    async def send(self, raw: str) -> None:
        msg = json.loads(raw)
        if msg.get("method") in ("connect", "agents.files.set"):
            self._outbox.append(json.dumps({"type": "res", "id": msg.get("id"), "ok": True}))


class _CapturingConnect:
    """Captures the additional_headers passed to websockets.connect."""

    captured: dict | None = None

    def __init__(self, *args, **kwargs) -> None:
        _CapturingConnect.captured = kwargs.get("additional_headers")
        self.ws = _FakeWS()

    async def __aenter__(self) -> _FakeWS:
        return self.ws

    async def __aexit__(self, *exc) -> bool:
        return False


@pytest.fixture
def _patched(monkeypatch):
    async def fake_acurl(method, path, body=None, with_ph=True, cookies=None):
        if path == "/open-apis/user/ws/ticket":
            return "HTTP_200", {"code": 0, "data": {"ticket": "tkt"}}
        if path == "/open-apis/user/mi/get":
            return "HTTP_200", {"code": 0, "data": {"userId": "u-42"}}
        return "HTTP_200", {"code": 0, "data": {}}

    monkeypatch.setattr(app, "acurl", fake_acurl)
    monkeypatch.setattr("websockets.connect", _CapturingConnect)
    _CapturingConnect.captured = None


def test_set_agent_files_sends_cookie_and_origin_on_handshake(_patched):
    ok, err = asyncio.run(
        app.claw_ws_set_agent_files({"SOUL.md": "s"}, cookies=SAMPLE_COOKIES)
    )
    assert (ok, err) == (True, None)

    headers = _CapturingConnect.captured
    assert headers is not None, "WS handshake must pass additional_headers"
    # Cookie carries the IDC affinity — without it the upgrade 401s.
    assert "serviceToken=tok-abc" in headers.get("Cookie", "")
    assert headers.get("Origin") == app.MIMO_BASE


def test_ws_handshake_headers_falls_back_to_default_cookies(monkeypatch):
    monkeypatch.setattr(app, "load_cookies", lambda: SAMPLE_COOKIES)
    headers = app._ws_handshake_headers(None)
    assert "serviceToken=tok-abc" in headers["Cookie"]
    assert headers["Origin"] == app.MIMO_BASE
