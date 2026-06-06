#!/usr/bin/env python3
"""MiMo API proxy (claw-side data plane).

Listens on loopback only; the reverse SSH tunnel exposes it on the target
machine. Resolves the MiMo API key from the OpenClaw gateway process env
(/proc/<pid>/environ), then forwards OpenAI/Anthropic-compatible requests to
the MiMo upstream with that key injected and streams the response back.

Env:
  PROXY_HOST        bind host (default 127.0.0.1 — keep loopback, tunnel exposes it)
  PROXY_PORT        bind port (default 18800)
  MIMO_API_KEY      override upstream key (else read from gateway /proc)
  MIMO_API_ENDPOINT override upstream base (else gateway /proc, else sgp default)
  PROXY_VERIFY_SSL  "0" to disable upstream cert verification (default verify on)
  PROXY_CONN_LIMIT       upstream connection pool size (default 200)
  PROXY_CONN_PER_HOST    per-host pool cap (default = PROXY_CONN_LIMIT)
  PROXY_PREWARM          warm N upstream TLS connections at startup (default 10)

No proxy-level auth: it binds loopback and is reached only via the reverse
tunnel landing on the target's loopback, so there is no in-between exposure.
"""
import asyncio
import os
import ssl
import time
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PORT = int(os.environ.get("PROXY_PORT", "18800"))
REQUEST_TIMEOUT = float(os.environ.get("PROXY_REQUEST_TIMEOUT", "300"))
STREAM_MAX_SECONDS = float(os.environ.get("PROXY_STREAM_TIMEOUT", "600"))
CONN_LIMIT = int(os.environ.get("PROXY_CONN_LIMIT", "200"))
CONN_PER_HOST = int(os.environ.get("PROXY_CONN_PER_HOST", str(CONN_LIMIT)))
PREWARM = int(os.environ.get("PROXY_PREWARM", "10"))
DEFAULT_ENDPOINT = "https://api-sgp-oc.xiaomimimo.com"

_start = time.time()
_reqs = 0


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def _resolve_upstream() -> tuple[str, str]:
    """(api_key, api_base) — prefer gateway /proc env, then this process env."""
    key = ep = ""
    try:
        import subprocess
        pid = subprocess.check_output(["pgrep", "-f", "openclaw-gateway"], text=True).strip().split("\n")[0]
        if pid:
            with open(f"/proc/{pid}/environ", "rb") as f:
                env = dict(kv.split(b"=", 1) for kv in f.read().split(b"\x00") if b"=" in kv)
            key = env.get(b"MIMO_API_KEY", b"").decode()
            ep = env.get(b"MIMO_API_ENDPOINT", b"").decode()
            if key or ep:
                log(f"config source: /proc/{pid}/environ")
    except Exception:
        pass
    key = key or os.environ.get("MIMO_API_KEY", "")
    ep = ep or os.environ.get("MIMO_API_ENDPOINT", "") or DEFAULT_ENDPOINT
    parsed = urlparse(ep)
    base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else ep.rstrip("/")
    return key, base


API_KEY, API_BASE = _resolve_upstream()
if not API_KEY:
    log("WARNING: no MIMO_API_KEY resolved; upstream calls will 401")
log(f"upstream: {API_BASE}  pool: {CONN_LIMIT} (per-host {CONN_PER_HOST})  prewarm: {PREWARM}")

# The gateway's MIMO_API_KEY rotates. Reading it once at startup and caching it
# forever means a rotated key leaves us forwarding a STALE token -> upstream 401
# (api-proxy 401 while the live key works). Re-resolve at most once per _KEY_TTL
# so rotations are picked up automatically, no proxy restart needed.
_KEY_TTL = float(os.environ.get("PROXY_KEY_TTL", "60"))
_key_cache = {"key": API_KEY, "base": API_BASE, "ts": time.time()}


def _current_key_base() -> tuple[str, str]:
    now = time.time()
    if now - _key_cache["ts"] > _KEY_TTL:
        k, b = _resolve_upstream()
        _key_cache["ts"] = now
        if k:
            _key_cache["key"], _key_cache["base"] = k, b
    return _key_cache["key"], _key_cache["base"]

_verify = os.environ.get("PROXY_VERIFY_SSL", "1") != "0"
_ssl_ctx: ssl.SSLContext | bool
if _verify:
    _ssl_ctx = ssl.create_default_context()
else:
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE

_session: aiohttp.ClientSession | None = None


def _upstream_path(path: str) -> str:
    # Anthropic Messages API lives under /anthropic on the MiMo upstream.
    if path == "/v1/messages":
        return "/anthropic/v1/messages"
    return path


async def handle(req: web.Request) -> web.StreamResponse:
    global _reqs
    if req.path == "/health":
        return web.json_response({"ok": True, "uptime": int(time.time() - _start), "reqs": _reqs})

    _reqs += 1
    api_key, api_base = _current_key_base()
    target = api_base + _upstream_path(req.path)
    if req.query_string:
        target += "?" + req.query_string

    headers = {"Content-Type": req.headers.get("Content-Type", "application/json"), "Accept": "*/*"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    for h in ("anthropic-version", "anthropic-beta", "x-request-id"):
        if h in req.headers:
            headers[h] = req.headers[h]

    body = await req.read()
    t0 = time.monotonic()
    try:
        assert _session is not None
        async with _session.request(req.method, target, headers=headers, data=body or None) as up:
            resp = web.StreamResponse(status=up.status)
            ct = up.headers.get("Content-Type")
            if ct:
                resp.headers["Content-Type"] = ct
            await resp.prepare(req)
            deadline = time.monotonic() + STREAM_MAX_SECONDS
            async for chunk in up.content.iter_any():
                if time.monotonic() > deadline:
                    log(f"{req.path} stream timeout")
                    break
                await resp.write(chunk)
            await resp.write_eof()
            log(f"{req.method} {req.path} -> {up.status} ({(time.monotonic()-t0)*1000:.0f}ms)")
            return resp
    except Exception as exc:
        log(f"{req.method} {req.path} ERR {type(exc).__name__}: {exc}")
        return web.json_response({"error": f"{type(exc).__name__}: {exc}"}, status=502)


async def _prewarm() -> None:
    """Open a few upstream TLS connections so the first real requests don't pay
    the TLS+TCP handshake latency. Best-effort; failures are ignored."""
    if PREWARM <= 0 or _session is None:
        return
    async def _ping():
        try:
            async with _session.get(API_BASE + "/health", timeout=aiohttp.ClientTimeout(total=10)):
                pass
        except Exception:
            pass
    await asyncio.gather(*[_ping() for _ in range(PREWARM)], return_exceptions=True)
    log(f"prewarmed up to {PREWARM} connections")


async def _on_startup(app: web.Application) -> None:
    global _session
    conn = aiohttp.TCPConnector(
        limit=CONN_LIMIT,
        limit_per_host=CONN_PER_HOST,
        ttl_dns_cache=300,
        keepalive_timeout=30,
        ssl=_ssl_ctx,
    )
    _session = aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_read=REQUEST_TIMEOUT),
    )
    asyncio.create_task(_prewarm())


async def _on_cleanup(app: web.Application) -> None:
    if _session:
        await _session.close()


def main() -> None:
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    app.router.add_route("*", "/{tail:.*}", handle)
    log(f"listening on {HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT, print=None)


if __name__ == "__main__":
    main()
