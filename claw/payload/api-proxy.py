#!/usr/bin/env python3
"""
MiMo API Proxy — asyncio 高性能 OpenAI 兼容代理
================================================
纯标准库实现，零外部依赖。设计目标：
  - asyncio 单线程协程，零线程开销，轻松处理数千并发
  - 连接池复用后端 TLS 连接
  - 原始字节流式 SSE 转发，零缓冲
  - SSL 跳过验证 + TCP_NODELAY + 连接复用

运行环境: Claw ECS (Python 3.12+)
从环境变量读取 MIMO_API_KEY / MIMO_API_ENDPOINT
认证: 调用方需携带 Authorization: Bearer sk-Aoki-MiMo

用法:
  python3 api-proxy.py                    # 默认 0.0.0.0:18800
  python3 api-proxy.py --port 18800
"""
import asyncio
import json
import os
import secrets as _secrets
import socket
import ssl
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

# ────────────── 配置 ──────────────

PORT = int(os.environ.get("PROXY_PORT", "18800"))
MAX_IDLE_CONN = 200
IDLE_TIMEOUT = 120        # 连接池空闲回收 (秒)
REQUEST_TIMEOUT = 300     # 请求最大时长 (秒)
MAX_BODY = 50 * 1024 * 1024  # 50MB 请求体上限
AUTH_TOKEN = "sk-Aoki-MiMo"
CHUNK_SIZE = 16384        # 流式读取块大小
BACKLOG = 256             # TCP listen backlog

START_TIME = time.time()
_req_count = 0


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ────────────── 解析后端 ──────────────

def _parse_endpoint():
    """从环境变量解析 MiMo API base URL，去掉多余路径。"""
    raw = os.environ.get("MIMO_API_ENDPOINT", "")
    if not raw:
        log("FATAL: MIMO_API_ENDPOINT not set")
        sys.exit(1)
    parsed = urlparse(raw)
    base = f"{parsed.scheme}://{parsed.netloc}"
    log(f"API endpoint: {base} (from {raw})")
    return base, parsed


_base, _parsed_ep = _parse_endpoint()
API_BASE = _base
BACKEND_HOST = _parsed_ep.hostname
BACKEND_PORT = _parsed_ep.port or (443 if _parsed_ep.scheme == "https" else 80)
BACKEND_SSL = _parsed_ep.scheme == "https"
API_KEY = os.environ.get("MIMO_API_KEY", "")
if not API_KEY:
    log("WARNING: MIMO_API_KEY not set, requests will fail")


# ────────────── SSL context (跳过验证，内网环境) ──────────────

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE


# ────────────── 连接池 ──────────────

class AsyncConnection:
    """封装 asyncio 的 reader/writer 对。"""
    __slots__ = ("reader", "writer", "created_at")

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.created_at = time.monotonic()

    def is_alive(self) -> bool:
        """对端 FIN 或自己已关都视为死。仅看 ``writer.is_closing()`` 不够 ——
        服务端 keep-alive 超时后我们这边 socket 还在，但 ``reader.at_eof()``
        会变 True，下次复用就 EOF/502。"""
        return not self.writer.is_closing() and not self.reader.at_eof()

    async def close(self):
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


class AsyncConnectionPool:
    """异步连接池，复用后端连接避免重复 TLS 握手。"""

    def __init__(self, host: str, port: int, use_ssl: bool = True,
                 max_idle: int = MAX_IDLE_CONN, idle_timeout: int = IDLE_TIMEOUT):
        self._host = host
        self._port = port
        self._use_ssl = use_ssl
        self._max_idle = max_idle
        self._idle_timeout = idle_timeout
        self._pool: asyncio.Queue[AsyncConnection] = asyncio.Queue(maxsize=max_idle)
        self._created = 0
        self._reused = 0

    async def _new_conn(self) -> AsyncConnection:
        ssl_ctx = _ssl_ctx if self._use_ssl else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port, ssl=ssl_ctx),
            timeout=15,
        )
        sock = writer.get_extra_info("socket")
        if sock:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        self._created += 1
        return AsyncConnection(reader, writer)

    async def acquire(self) -> AsyncConnection:
        """获取一个连接，优先复用池中空闲的。"""
        now = time.monotonic()
        while True:
            try:
                conn = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            # 检查空闲超时
            if now - conn.created_at > self._idle_timeout:
                await conn.close()
                self._created -= 1
                continue
            # 检查连接是否还活着
            if conn.is_alive():
                self._reused += 1
                return conn
            else:
                await conn.close()
                self._created -= 1
        return await self._new_conn()

    async def release(self, conn: AsyncConnection, healthy: bool = True):
        """归还连接到池中。"""
        if not healthy:
            await conn.close()
            self._created -= 1
            return
        if self._pool.qsize() < self._max_idle:
            self._pool.put_nowait(conn)
        else:
            await conn.close()
            self._created -= 1

    async def close_all(self):
        """关闭所有池中连接。"""
        while True:
            try:
                conn = self._pool.get_nowait()
                await conn.close()
            except asyncio.QueueEmpty:
                break

    def stats(self) -> dict:
        idle = self._pool.qsize()
        active = self._created - idle
        total = self._created + self._reused
        return {
            "idle": idle,
            "active": max(active, 0),
            "total_created": self._created,
            "total_reused": self._reused,
            "reuse_rate": round(self._reused / max(total, 1) * 100, 1),
        }


_pool = AsyncConnectionPool(BACKEND_HOST, BACKEND_PORT, BACKEND_SSL)


# ────────────── HTTP 解析工具 ──────────────

async def _parse_headers(reader: asyncio.StreamReader) -> dict[str, str]:
    """读取 HTTP 头直到空行。Key 统一小写（HTTP 头本就大小写不敏感），
    避免 ``Authorization`` vs ``authorization`` 误判。"""
    headers: dict[str, str] = {}
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT)
        if line in (b"\r\n", b"\n"):
            break
        try:
            decoded = line.decode("utf-8", errors="replace")
            if ":" in decoded:
                key, val = decoded.split(":", 1)
                headers[key.strip().lower()] = val.strip().rstrip("\r\n")
        except Exception:
            continue
    return headers


async def _read_request_body(reader: asyncio.StreamReader, content_length: int) -> bytes:
    if content_length <= 0:
        return b""
    if content_length > MAX_BODY:
        return None
    return await asyncio.wait_for(reader.readexactly(content_length), timeout=REQUEST_TIMEOUT)


async def _read_response_headers(reader: asyncio.StreamReader) -> tuple[int, str, dict[str, str]]:
    """读取响应状态行和头，返回 (status_code, status_text, headers)。"""
    status_line = await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT)
    parts = status_line.decode("utf-8", errors="replace").split(None, 2)
    status = int(parts[1]) if len(parts) >= 2 else 502
    reason = parts[2].strip() if len(parts) >= 3 else ""
    headers = await _parse_headers(reader)
    return status, reason, headers


async def _read_chunked_body(reader: asyncio.StreamReader) -> bytes:
    """读取 chunked Transfer-Encoding 的完整 body。"""
    chunks = []
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT)
        size_str = line.decode("utf-8", errors="replace").strip().split(";")[0]
        size = int(size_str, 16)
        if size == 0:
            await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT)  # trailing CRLF
            break
        chunk = await asyncio.wait_for(reader.readexactly(size), timeout=REQUEST_TIMEOUT)
        chunks.append(chunk)
        await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT)  # trailing CRLF
    return b"".join(chunks)


async def _read_full_body(reader: asyncio.StreamReader, headers: dict[str, str]) -> bytes:
    """根据响应头读取完整 body：

    * ``transfer-encoding: chunked`` → chunked decode
    * 有 ``content-length`` → readexactly
    * 都没有 → 视为 close-delimited，``reader.read()`` 直到 EOF
    """
    te = headers.get("transfer-encoding", "").lower()
    if "chunked" in te:
        return await _read_chunked_body(reader)
    cl_raw = headers.get("content-length")
    if cl_raw is not None:
        try:
            cl = int(cl_raw)
        except ValueError:
            cl = 0
        if cl <= 0:
            return b""
        return await asyncio.wait_for(reader.readexactly(cl), timeout=REQUEST_TIMEOUT)
    # No length info → read until server closes the connection.
    return await asyncio.wait_for(reader.read(), timeout=REQUEST_TIMEOUT)


# ────────────── 响应构建 ──────────────

_HTTP_STATUS_REASONS = {
    200: "OK", 201: "Created", 204: "No Content",
    301: "Moved Permanently", 302: "Found",
    400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
    404: "Not Found", 408: "Request Timeout", 413: "Payload Too Large",
    429: "Too Many Requests", 500: "Internal Server Error",
    502: "Bad Gateway", 503: "Service Unavailable",
}


def _http_status_line(code: int) -> str:
    reason = _HTTP_STATUS_REASONS.get(code, "Error")
    return f"HTTP/1.1 {code} {reason}\r\n"


def _build_response(code: int, hdrs: dict[str, str], body: bytes = b"") -> bytes:
    """构建完整 HTTP 响应。"""
    line = _http_status_line(code)
    header_block = "".join(f"{k}: {v}\r\n" for k, v in hdrs.items())
    return (line + header_block + "\r\n").encode() + body


def _build_error(code: int, message: str) -> bytes:
    body = json.dumps({"error": {"message": message, "type": "proxy_error"}}).encode()
    return _build_response(code, {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "Connection": "close",
    }, body)


# ────────────── 请求处理 ──────────────

# 预编译静态响应
_MODELS_BODY = json.dumps({
    "object": "list",
    "data": [
        {"id": m, "object": "model", "owned_by": "mimo"}
        for m in (
            "mimo-v2.5-pro", "mimo-v2.5",
            "mimo-v2-pro", "mimo-v2-flash", "mimo-v2-omni",
            "mimo-v2-tts", "mimo-v2.5-tts",
            "mimo-v2.5-tts-voiceclone", "mimo-v2.5-tts-voicedesign",
        )
    ]
}).encode()

_CORS_RESP = _build_response(200, {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Content-Length": "0",
})


async def _handle_status(writer: asyncio.StreamWriter):
    body = json.dumps({
        "status": "running",
        "uptime": int(time.time() - START_TIME),
        "requests": _req_count,
        "endpoint": API_BASE,
        "pool": _pool.stats(),
    }, indent=2).encode()
    writer.write(_build_response(200, {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
    }, body))
    await writer.drain()


async def _handle_models(writer: asyncio.StreamWriter):
    writer.write(_build_response(200, {
        "Content-Type": "application/json",
        "Content-Length": str(len(_MODELS_BODY)),
    }, _MODELS_BODY))
    await writer.drain()


# ────────────── 核心代理 ──────────────

async def _proxy(method: str, path: str, req_headers: dict[str, str],
                 body: bytes, client_reader: asyncio.StreamReader,
                 client_writer: asyncio.StreamWriter):
    global _req_count
    _req_count += 1

    # 构建后端路径: 处理 MIMO_API_ENDPOINT 含路径前缀的情况
    endpoint_path = _parsed_ep.path
    if endpoint_path and endpoint_path != "/":
        if path.startswith(endpoint_path):
            pass  # 已经匹配，直接用
        # 否则保持客户端请求路径不变（大多数情况 endpoint 是 base URL）

    # 构建请求头（注意 req_headers 的 key 都是小写）
    backend_headers = {
        "Host": BACKEND_HOST,
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": req_headers.get("content-type", "application/json"),
        "Accept": req_headers.get("accept", "*/*"),
        "Connection": "keep-alive",
        "Content-Length": str(len(body)),
    }
    # 保留自定义头（lookup 用小写，转发时保留惯用大小写）
    for canonical, key in (
        ("X-Request-Id", "x-request-id"),
        ("X-Conversation-Id", "x-conversation-id"),
        ("anthropic-version", "anthropic-version"),
    ):
        val = req_headers.get(key)
        if val:
            backend_headers[canonical] = val

    t0 = time.monotonic()
    conn = await _pool.acquire()

    try:
        # 发送请求到后端
        request_line = f"{method} {path} HTTP/1.1\r\n"
        header_block = "".join(f"{k}: {v}\r\n" for k, v in backend_headers.items())
        conn.writer.write((request_line + header_block + "\r\n").encode() + body)
        await conn.writer.drain()

        # 读取后端响应头 —— 让响应头决定是不是 SSE，比请求体里的 stream 字段更准
        # （客户端可能没标 stream，但上游决定流式返回；或反之）
        resp_status, resp_reason, resp_headers = await _read_response_headers(conn.reader)
        resp_ct = resp_headers.get("content-type", "")
        is_stream = "text/event-stream" in resp_ct.lower()

        if is_stream:
            # ─── 流式 SSE 转发 ───
            # 透传后端响应头和 body，不做 chunk 解码再编码
            resp_header = _http_status_line(resp_status)
            for k, v in resp_headers.items():
                # 跳过 connection / transfer-encoding（我们这层直接 read→write，
                # 客户端按字节流接收即可，无需声明 chunked）
                if k.lower() in ("connection", "transfer-encoding"):
                    continue
                resp_header += f"{k}: {v}\r\n"
            resp_header += "Access-Control-Allow-Origin: *\r\n"
            resp_header += "Connection: close\r\n"
            resp_header += "\r\n"

            client_writer.write(resp_header.encode())
            await client_writer.drain()

            # 单 chunk 30s 超时，整体不另设限（长流量靠 chunk 间隔超时兜底）
            total_bytes = 0
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        conn.reader.read(CHUNK_SIZE), timeout=30,
                    )
                except (asyncio.TimeoutError, ConnectionError):
                    break
                if not chunk:
                    break
                total_bytes += len(chunk)
                client_writer.write(chunk)
                await client_writer.drain()

            latency_ms = (time.monotonic() - t0) * 1000
            log(f"{method} {path} → {resp_status} SSE {total_bytes}B ({latency_ms:.0f}ms)")
            # SSE 流结束意味着上游已经 close 或半 close —— 不要放回池
            await _pool.release(conn, healthy=False)

        else:
            # ─── 非流式：读完整响应 ───
            resp_body = await _read_full_body(conn.reader, resp_headers)
            latency_ms = (time.monotonic() - t0) * 1000

            client_writer.write(_build_response(resp_status, {
                "Content-Type": resp_headers.get("content-type", "application/json"),
                "Content-Length": str(len(resp_body)),
                "Access-Control-Allow-Origin": "*",
                "Connection": "close",
            }, resp_body))
            await client_writer.drain()

            level = "JSON" if resp_status < 400 else "ERR"
            log(f"{method} {path} → {resp_status} {level} ({latency_ms:.0f}ms)")
            # 上游可能用 Connection: close 结束 body —— 此时 reader 已 EOF，
            # 复用会立即报 502。让 is_alive() 二次检查兜底，这里仍按状态码判定。
            await _pool.release(conn, healthy=(resp_status < 500))

    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        # 内部错误日志保留细节，但回给客户端的消息脱敏，避免泄露 Python 异常栈
        log(f"{method} {path} → ERROR {type(e).__name__}: {e} ({latency_ms:.0f}ms)")
        await _pool.release(conn, healthy=False)
        try:
            client_writer.write(_build_error(502, "Upstream error"))
            await client_writer.drain()
        except Exception:
            pass


# ────────────── 客户端连接处理 ──────────────

async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """处理一个客户端连接。"""
    try:
        # 解析请求行
        request_line = await asyncio.wait_for(reader.readline(), timeout=30)
        parts = request_line.decode("utf-8", errors="replace").split()
        if len(parts) < 3:
            writer.close()
            await writer.wait_closed()
            return

        method, path, _ = parts[0], parts[1], parts[2]

        # 解析请求头
        req_headers = await _parse_headers(reader)

        # 处理 OPTIONS (CORS preflight)
        if method == "OPTIONS":
            writer.write(_CORS_RESP)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        # 状态和模型接口（无需认证）
        if method == "GET" and path in ("/", "/health"):
            await _handle_status(writer)
            writer.close()
            await writer.wait_closed()
            return

        if method == "GET" and path == "/v1/models":
            await _handle_models(writer)
            writer.close()
            await writer.wait_closed()
            return

        # 认证检查（timing-safe 比较，防侧信道；header key 已小写）
        auth = req_headers.get("authorization", "")
        if not auth.startswith("Bearer ") or not _secrets.compare_digest(
            auth[7:].strip(), AUTH_TOKEN
        ):
            writer.write(_build_error(401, "Missing or invalid Authorization"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        # 读取请求体
        content_length = int(req_headers.get("content-length", 0))
        body = await _read_request_body(reader, content_length)
        if body is None:
            writer.write(_build_error(413, "Request body too large"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        # 代理到后端
        await _proxy(method, path, req_headers, body, reader, writer)
        writer.close()
        await writer.wait_closed()

    except asyncio.TimeoutError:
        try:
            writer.write(_build_error(408, "Request timeout"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        log(f"Client handler error: {e}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ────────────── Server ──────────────

async def _shutdown(sig, loop):
    """优雅关闭：停止接受新连接，等待进行中的请求完成。"""
    log(f"Received {sig.name}, shutting down...")
    await _pool.close_all()
    loop.stop()


async def main():
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="MiMo API Proxy (asyncio)")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument(
        "--host", default="127.0.0.1",
        help="bind address (default 127.0.0.1 — sshd reverse-tunnel client "
             "在同 netns 内通过 loopback 连接即可；显式传 0.0.0.0 才暴露到其他接口)",
    )
    args = parser.parse_args()

    server = await asyncio.start_server(
        _handle_client, args.host, args.port,
        backlog=BACKLOG,
        limit=256 * 1024,  # 256KB stream buffer
    )

    # 注册信号处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_shutdown(s, loop)))
        except NotImplementedError:
            pass  # Windows 不支持 add_signal_handler

    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log(f"MiMo API Proxy (asyncio) listening on {addrs}")
    log(f"Backend: {API_BASE}")
    log(f"Auth token: {AUTH_TOKEN}")
    log(f"Max idle conns: {MAX_IDLE_CONN}, Timeout: {REQUEST_TIMEOUT}s")

    async with server:
        await server.serve_forever()

    await _pool.close_all()
    log("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Shutting down...")
