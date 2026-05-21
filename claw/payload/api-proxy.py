#!/usr/bin/env python3
"""
MiMo API Proxy v2 — aiohttp 高性能 OpenAI 兼容代理
===================================================
基于 aiohttp，自带连接池 + 零拷贝流式转发，性能远超手写 socket。
运行: python3 api-proxy-v2.py [--port 18800] [--host 127.0.0.1]
"""
import asyncio
import json
import os
import secrets
import ssl
import time
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

# ────────────── 配置 ──────────────

PORT = int(os.environ.get("PROXY_PORT", "18800"))
AUTH_TOKEN = os.environ.get("PROXY_AUTH_TOKEN", "sk-Aoki-MiMo")
REQUEST_TIMEOUT = 300
STREAM_MAX_SECONDS = 600
MAX_BODY = 50 * 1024 * 1024
CONN_LIMIT = 600          # 连接池上限（>= 并发数）
CONN_LIMIT_PER_HOST = 600 # 单主机连接上限
DNS_CACHE_TTL = 300       # DNS 缓存 TTL

START_TIME = time.time()
_req_count = 0


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ────────────── env bootstrap ──────────────

def _resolve_mimo_config() -> tuple:
    """按优先级获取 MIMO_API_KEY 和 MIMO_API_ENDPOINT：
    1. 从 OpenClaw Gateway 进程的 /proc/pid/environ 读取
    2. 当前进程环境变量（手动 export 或 systemd 注入）
    """
    key = ""
    ep = ""
    source = ""

    # --- 1. 从 Gateway 进程读取 ---
    try:
        import subprocess
        gw_pid = subprocess.check_output(
            ["pgrep", "-f", "openclaw-gateway"], text=True
        ).strip().split("\n")[0]
        if gw_pid:
            with open(f"/proc/{gw_pid}/environ", "rb") as f:
                env = dict(
                    kv.split(b"=", 1)
                    for kv in f.read().split(b"\x00")
                    if b"=" in kv
                )
            key = key or env.get(b"MIMO_API_KEY", b"").decode()
            ep = ep or env.get(b"MIMO_API_ENDPOINT", b"").decode()
            if key or ep:
                source = f"/proc/{gw_pid}/environ (Gateway)"
                if key and ep:
                    log(f"Config source: {source}")
                    return key, ep
    except Exception:
        pass

    # --- 2. 环境变量 ---
    env_used = False
    env_key = os.environ.get("MIMO_API_KEY", "")
    env_ep = os.environ.get("MIMO_API_ENDPOINT", "")
    if not key and env_key:
        key = env_key
        env_used = True
    if not ep and env_ep:
        ep = env_ep
        env_used = True

    if source and env_used:
        log(f"Config source: {source} + environment variables")
    elif source:
        log(f"Config source: {source}")
    elif env_used:
        log("Config source: environment variables")

    return key, ep


API_KEY, _raw_ep = _resolve_mimo_config()

# ────────────── 解析后端 ──────────────

if not _raw_ep:
    _raw_ep = "https://api-sgp-oc.xiaomimimo.com"

_parsed = urlparse(_raw_ep)
# 只取 scheme + host，不带 path（客户端请求自带完整路径）
API_BASE = f"{_parsed.scheme}://{_parsed.netloc}"

if not API_KEY:
    log("WARNING: MIMO_API_KEY not found anywhere, backend requests will fail")

log(f"Backend: {API_BASE}")

# SSL
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

# ────────────── 预热 & 连接池 ──────────────

_connector: aiohttp.TCPConnector = None
_session: aiohttp.ClientSession = None


async def _init_session():
    global _connector, _session
    _connector = aiohttp.TCPConnector(
        limit=CONN_LIMIT,
        limit_per_host=CONN_LIMIT_PER_HOST,
        ttl_dns_cache=DNS_CACHE_TTL,
        ssl=_ssl_ctx,
        enable_cleanup_closed=True,
        force_close=False,
        keepalive_timeout=30,
    )
    _session = aiohttp.ClientSession(
        connector=_connector,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=10),
    )
    # 预热：并行建 30 条连接（OPTIONS 请求，不触发推理）
    log("Pre-warming connections...")

    async def _warm_once():
        try:
            async with _session.options(
                API_BASE,
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                await r.read()
                return True
        except Exception:
            return False

    results = await asyncio.gather(
        *[_warm_once() for _ in range(30)], return_exceptions=True,
    )
    warmed = sum(1 for r in results if r is True)
    log(f"Pre-warmed {warmed}/30 connections")


async def _close_session():
    if _session:
        await _session.close()
    if _connector:
        await _connector.close()


# ────────────── 静态响应 ──────────────

_MODELS_BODY = json.dumps({
    "object": "list",
    "data": [{"id": m, "object": "model", "owned_by": "mimo"}
             for m in ("mimo-v2.5-pro", "mimo-v2.5", "mimo-v2-pro", "mimo-v2-flash",
                       "mimo-v2-omni", "mimo-v2-tts", "mimo-v2.5-tts",
                       "mimo-v2.5-tts-voiceclone", "mimo-v2.5-tts-voicedesign")]
}).encode()

_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
}


# ────────────── 认证 ──────────────

def _check_auth(request: web.Request) -> bool:
    if not AUTH_TOKEN:
        return False
    auth = request.headers.get("Authorization", "")
    x_key = request.headers.get("X-Api-Key", "")
    return (
        (auth.startswith("Bearer ") and secrets.compare_digest(auth[7:].strip(), AUTH_TOKEN))
        or (x_key and secrets.compare_digest(x_key.strip(), AUTH_TOKEN))
    )


# ────────────── 路由处理 ──────────────

async def handle_health(request: web.Request):
    stats = {
        "status": "running",
        "uptime": int(time.time() - START_TIME),
        "requests": _req_count,
        "endpoint": API_BASE,
    }
    return web.json_response(stats)


async def handle_models(request: web.Request):
    return web.Response(body=_MODELS_BODY, content_type="application/json",
                        headers=_CORS_HEADERS)


async def handle_options(request: web.Request):
    return web.Response(status=204, headers=_CORS_HEADERS)


async def handle_proxy(request: web.Request):
    global _req_count
    _req_count += 1

    if not _check_auth(request):
        return web.json_response(
            {"error": {"message": "Missing or invalid Authorization", "type": "proxy_error"}},
            status=401,
        )

    # 读取请求体
    body = await request.read()
    if len(body) > MAX_BODY:
        return web.json_response(
            {"error": {"message": "Request body too large", "type": "proxy_error"}},
            status=413,
        )

    # 构建后端请求头
    backend_headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": request.headers.get("Content-Type", "application/json"),
        "Accept": request.headers.get("Accept", "*/*"),
    }
    for h in ("X-Request-Id", "X-Conversation-Id", "Anthropic-Version", "Anthropic-Beta"):
        v = request.headers.get(h)
        if v:
            backend_headers[h] = v

    # Anthropic 路径重写：/v1/messages → /anthropic/v1/messages
    path = request.path
    if path == "/v1/messages":
        path = "/anthropic/v1/messages"

    # 路径：保留客户端原始路径 + query string（aiohttp 的 path_qs）
    if request.query_string:
        backend_url = f"{API_BASE}{path}?{request.query_string}"
    else:
        backend_url = f"{API_BASE}{path}"

    t0 = time.monotonic()
    try:
        async with _session.request(
            request.method, backend_url,
            headers=backend_headers,
            data=body,
        ) as resp:
            ct = resp.headers.get("Content-Type", "")
            is_stream = "text/event-stream" in ct.lower()

            # 透传后端的非标准 headers
            passthrough = {}
            for key in ("X-Request-Id", "X-RateLimit-Limit", "X-RateLimit-Remaining",
                        "X-RateLimit-Reset", "Retry-After"):
                val = resp.headers.get(key)
                if val:
                    passthrough[key] = val

            if is_stream:
                # ─── SSE 流式转发 ───
                response = web.StreamResponse(
                    status=resp.status,
                    headers={
                        "Content-Type": ct,
                        "Access-Control-Allow-Origin": "*",
                        "Cache-Control": "no-cache",
                        **passthrough,
                    },
                )
                await response.prepare(request)

                deadline = time.monotonic() + STREAM_MAX_SECONDS
                total_bytes = 0
                timed_out = False
                async for chunk in resp.content.iter_any():
                    if time.monotonic() > deadline:
                        timed_out = True
                        break
                    total_bytes += len(chunk)
                    await response.write(chunk)

                if timed_out:
                    # 通知客户端流被超时截断
                    await response.write(b'data: {"error":{"message":"Stream timeout","type":"proxy_error"}}\n\n')

                latency_ms = (time.monotonic() - t0) * 1000
                log(f"SSE {request.path} → {resp.status} {total_bytes}B"
                    f"{' (timeout)' if timed_out else ''} ({latency_ms:.0f}ms)")
                await response.write_eof()
                return response

            else:
                # ─── JSON 完整响应 ───
                resp_body = await resp.read()
                latency_ms = (time.monotonic() - t0) * 1000
                level = "JSON" if resp.status < 400 else "ERR"
                log(f"{request.method} {request.path} → {resp.status} {level} ({latency_ms:.0f}ms)")
                return web.Response(
                    status=resp.status,
                    body=resp_body,
                    content_type=ct,
                    headers={"Access-Control-Allow-Origin": "*", **passthrough},
                )

    except asyncio.TimeoutError:
        latency_ms = (time.monotonic() - t0) * 1000
        log(f"{request.method} {request.path} → TIMEOUT ({latency_ms:.0f}ms)")
        return web.json_response(
            {"error": {"message": "Upstream timeout", "type": "proxy_error"}},
            status=504,
        )
    except aiohttp.ClientError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        log(f"{request.method} {request.path} → ERROR {e} ({latency_ms:.0f}ms)")
        return web.json_response(
            {"error": {"message": f"Upstream error: {e}", "type": "proxy_error"}},
            status=502,
        )


# ────────────── App ──────────────

async def on_startup(app):
    await _init_session()


async def on_cleanup(app):
    await _close_session()


def create_app():
    app = web.Application(client_max_size=MAX_BODY)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Specific routes first — aiohttp 按注册顺序匹配，先 specific 再 catch-all
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/v1/models", handle_models)
    app.router.add_route("OPTIONS", "/{path:.*}", handle_options)

    # Catch-all proxy: cover OpenAI (/v1/...) + Anthropic (/anthropic/v1/...) 等任意路径
    app.router.add_route("*", "/{path:.*}", handle_proxy)

    return app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MiMo API Proxy v2 (aiohttp)")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    log(f"MiMo API Proxy v2 (aiohttp) starting on {args.host}:{args.port}")
    # web.run_app 自带 SIGINT/SIGTERM 处理 + 调用 on_cleanup，不要自己 add_signal_handler
    # —— 否则 signal handler 里 raise GracefulExit 会跟 run_app 内部 loop 冲突。
    web.run_app(
        create_app(),
        host=args.host, port=args.port,
        print=lambda _: log(f"Listening on {args.host}:{args.port}"),
        access_log=None,
    )
