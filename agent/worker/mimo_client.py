"""MiMo client: create claw + chat with claw over WS. Self-contained.

Why this must run on a mainland-China IP: MiMo's ws-proxy gateway gates chat by
the client's source-IP region. A non-mainland IP gets chat.send intercepted at
the gateway and replaced with a canned "let's change the topic" reply (it never
reaches the openclaw agent — 0 agent events). REST (create/status/ticket) works
from any IP; only the WS chat content is region-gated.

Deps: httpx, websockets (+ python-socks if MIMO_PROXY is a socks proxy).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from urllib.parse import quote

import httpx
import websockets

BASE = "https://aistudio.xiaomimimo.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
# Optional: route MiMo traffic through a mainland egress proxy (e.g. a SOCKS on
# the same LAN). Leave unset when the host itself already has a mainland IP.
PROXY = os.environ.get("MIMO_PROXY") or None

_DEFLECTION_MARKERS = ("无法回答", "换个话题")


def is_region_blocked(reply: str) -> bool:
    """Heuristic: the canned gateway deflection returned to non-mainland IPs."""
    return any(k in (reply or "") for k in _DEFLECTION_MARKERS)


def _cookie_header(cookies: list) -> str:
    return "; ".join(
        f"{c['name']}={c['value']}" for c in cookies if "xiaomimimo" in c.get("domain", "")
    )


def _ph(cookies: list) -> str:
    v = next((c["value"] for c in cookies if c["name"] == "xiaomichatbot_ph"), "")
    return v[1:-1] if v.startswith('"') and v.endswith('"') else v


def _client(cookies: list) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        proxy=PROXY, timeout=30, trust_env=False,
        headers={"cookie": _cookie_header(cookies), "content-type": "application/json",
                 "user-agent": UA},
    )


async def egress_info() -> dict:
    """Report the egress IP/region MiMo would see (through MIMO_PROXY if set).
    Used by the worker heartbeat so the panel can refuse to dispatch claw jobs
    to a worker that is NOT on a mainland IP."""
    try:
        async with httpx.AsyncClient(proxy=PROXY, timeout=10, trust_env=False) as c:
            d = (await c.get("http://ip-api.com/json/?fields=query,country,countryCode")).json()
            return {"ip": d.get("query"), "country": d.get("country"),
                    "cn": d.get("countryCode") == "CN"}
    except Exception as e:
        return {"ip": None, "country": None, "cn": False, "error": str(e)[:80]}


async def claw_status(cookies: list) -> str | None:
    async with _client(cookies) as c:
        s = (await c.get(f"{BASE}/open-apis/user/mimo-claw/status")).json()
        return (s.get("data") or {}).get("status")


async def create_claw(cookies: list, budget_s: int = 600, log=print) -> bool:
    """Create claw: retry through 429 gate AND CREATE_FAILED until AVAILABLE."""
    ph = quote(_ph(cookies), safe="")
    deadline = time.time() + budget_s
    async with _client(cookies) as c:
        while time.time() < deadline:
            r = (await c.post(
                f"{BASE}/open-apis/user/mimo-claw/create?xiaomichatbot_ph={ph}",
                content="{}")).json()
            if r.get("code") != 0:
                await asyncio.sleep(3)
                continue
            for _ in range(40):
                await asyncio.sleep(5)
                st = (await c.get(f"{BASE}/open-apis/user/mimo-claw/status")).json()
                st = (st.get("data") or {}).get("status")
                if st == "AVAILABLE":
                    log("claw AVAILABLE")
                    return True
                if st in ("CREATE_FAILED", "FAILED", "ERROR", "DESTROYED"):
                    log(f"claw {st}, recreating")
                    break
            await asyncio.sleep(2)
    return False


async def destroy_claw(cookies: list) -> None:
    async with _client(cookies) as c:
        await c.post(f"{BASE}/open-apis/user/mimo-claw/destroy", content="{}")
    for _ in range(24):
        await asyncio.sleep(5)
        if (await claw_status(cookies)) in ("DESTROYED", "", None):
            return


async def claw_chat(cookies: list, message: str, session_key: str):
    """One WS chat round. Short-lived connection. Returns (reply, error)."""
    ph = quote(_ph(cookies), safe="")
    async with _client(cookies) as c:
        tk = (await c.get(f"{BASE}/open-apis/user/ws/ticket?xiaomichatbot_ph={ph}")).json()
        ticket = (tk.get("data") or {}).get("ticket")
    if not ticket:
        return "", f"no ticket: {tk}"
    url = f"wss://aistudio.xiaomimimo.com/ws/proxy?ticket={ticket}"
    hdr = {"Origin": BASE, "Cookie": _cookie_header(cookies), "User-Agent": UA}
    try:
        async with websockets.connect(url, proxy=PROXY, additional_headers=hdr,
                                      ping_interval=30, ping_timeout=10) as ws:
            await asyncio.wait_for(ws.recv(), 10)
            rid = str(uuid.uuid4())
            await ws.send(json.dumps({"type": "req", "id": rid, "method": "connect", "params": {
                "minProtocol": 3, "maxProtocol": 3,
                "client": {"id": "cli", "version": "mimo-claw-worker", "platform": "Linux", "mode": "cli"},
                "role": "operator",
                "scopes": ["operator.admin", "operator.read", "operator.write",
                           "operator.approvals", "operator.pairing"],
                "caps": ["tool-events"], "userAgent": UA, "locale": "zh-CN"}}))
            while True:
                m = json.loads(await asyncio.wait_for(ws.recv(), 10))
                if m.get("id") == rid:
                    if not m.get("ok"):
                        return "", f"connect failed: {m}"
                    break
            mid = str(uuid.uuid4())
            await ws.send(json.dumps({"type": "req", "id": mid, "method": "chat.send", "params": {
                "sessionKey": session_key, "message": message, "deliver": False,
                "idempotencyKey": mid}}))
            full = ""
            for _ in range(400):
                m = json.loads(await asyncio.wait_for(ws.recv(), 120))
                ev, pl = m.get("event"), m.get("payload", {})
                if m.get("type") == "res" and m.get("id") == mid and not m.get("ok"):
                    return "", f"chat.send error: {m}"
                if ev == "agent" and pl.get("stream") == "assistant":
                    full = pl.get("data", {}).get("text", full)
                if ev == "chat" and pl.get("state") == "final":
                    for b in pl.get("message", {}).get("content", []):
                        if b.get("type") == "text":
                            full = b.get("text", full)
                    break
            return full, None
    except Exception as e:
        return "", f"WS {type(e).__name__}: {str(e)[:160]}"
