"""WebSocket reverse tunnel for Claw-hosted upstream requests.

The public gateway accepts Claw bridge nodes on ``/ws``.  Normal gateway
traffic can then target a backend whose ``base_url`` starts with ``ws://`` or
``wss://``; the transport packages each HTTP-like request as JSON, sends it to
one connected bridge node, and streams response chunks back through an
in-process queue.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from gateway.core import BackendUnavailableError, UpstreamError, UpstreamTimeoutError

logger = logging.getLogger(__name__)

_NODE_RESPONSE_TIMEOUT_S = float(os.environ.get("MIMO_WS_NODE_RESPONSE_TIMEOUT_S", "30"))
_STREAM_CHUNK_TIMEOUT_S = float(os.environ.get("MIMO_WS_STREAM_CHUNK_TIMEOUT_S", "120"))
_MAX_PENDING = int(os.environ.get("MIMO_WS_MAX_PENDING", "2000"))
_RETRYABLE_STATUSES = {401, 403, 429}


class _Node:
    __slots__ = ("ws", "label", "account", "connected_at", "lock", "cooldown_until")

    def __init__(
        self,
        ws: WebSocket,
        label: str,
        account: str | None = None,
    ) -> None:
        self.ws = ws
        self.label = label
        # The account this Claw bridge serves (from ``?account=`` on connect).
        # ``None`` means the node joins the shared, account-agnostic pool.
        self.account = account
        self.connected_at = time.time()
        self.lock = asyncio.Lock()
        self.cooldown_until = 0.0


class WebSocketTunnel:
    def __init__(self) -> None:
        self._nodes: list[_Node] = []
        self._pending: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._req_to_node: dict[str, _Node] = {}
        self._node_reqs: dict[int, set[str]] = {}
        self._idx = 0
        self._lock = asyncio.Lock()

    async def accept(self, ws: WebSocket) -> None:
        if not _check_node_token(ws):
            await ws.close(code=1008)
            return
        await ws.accept()
        label = f"{ws.client.host}:{ws.client.port}" if ws.client else "unknown"
        account = (ws.query_params.get("account") or "").strip() or None
        node = _Node(ws=ws, label=label, account=account)
        async with self._lock:
            self._nodes.append(node)
        logger.info(
            "WS tunnel node connected: %s account=%s (online=%d)",
            label, account or "-", self.online_count,
        )
        try:
            while True:
                raw = await ws.receive_text()
                data = json.loads(raw)
                req_id = data.get("req_id")
                if isinstance(req_id, str):
                    queue = self._pending.get(req_id)
                    if queue is not None:
                        queue.put_nowait(data)
        except WebSocketDisconnect:
            logger.warning("WS tunnel node disconnected: %s", label)
        except Exception as e:  # noqa: BLE001
            logger.warning("WS tunnel node failed: %s: %s", label, e)
        finally:
            await self._remove_node(node)

    @property
    def online_count(self) -> int:
        return len(self._nodes)

    def online_count_for(self, account: str | None) -> int:
        """Nodes that can serve ``account``: account-specific matches plus the
        account-agnostic pool (``node.account is None``). Snapshots the node
        list so it is safe to read from another thread (the deploy worker)."""
        nodes = list(self._nodes)
        if account is None:
            return len(nodes)
        return sum(1 for n in nodes if n.account in (account, None))

    def has_account(self, account: str) -> bool:
        """True iff a node explicitly registered for this account is online.
        Safe to call cross-thread (snapshots the node list)."""
        return any(n.account == account for n in list(self._nodes))

    def status(self) -> dict[str, Any]:
        now = time.time()
        return {
            "online": len(self._nodes),
            "pending": len(self._pending),
            "nodes": [
                {
                    "label": n.label,
                    "account": n.account,
                    "connected_for_s": round(now - n.connected_at, 1),
                    "cooldown_for_s": max(0, round(n.cooldown_until - now, 1)),
                    "pending": len(self._node_reqs.get(id(n), set())),
                }
                for n in self._nodes
            ],
        }

    async def request(
        self,
        url: str,
        body: dict[str, Any],
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout_s: float = 600.0,
        stream: bool = False,
    ) -> tuple[int, bytes | AsyncIterator[bytes], dict[str, str]]:
        account = _account_from_url(url)
        attempts = max(1, min(3, self.online_count_for(account)))
        last_error = (
            f"no available ws tunnel node for account {account!r}"
            if account else "no available ws tunnel node"
        )
        for attempt in range(attempts):
            req_id = ""
            try:
                req_id, queue, node = await self._dispatch(
                    method=method,
                    url=url,
                    body=body,
                    headers=headers or {},
                    attempt=attempt + 1,
                    account=account,
                )
                first = await asyncio.wait_for(queue.get(), timeout=_NODE_RESPONSE_TIMEOUT_S)
                msg_type = first.get("type")
                if msg_type == "error":
                    last_error = str(first.get("body") or "node error")
                    await self._cleanup(req_id)
                    continue
                if msg_type != "start":
                    last_error = f"unexpected node message: {msg_type!r}"
                    await self._cleanup(req_id)
                    continue

                status = int(first.get("status") or 502)
                if _should_retry_status(status):
                    self._cooldown(node, status)
                    last_error = f"node returned retryable HTTP {status}"
                    asyncio.create_task(self._drain_and_cleanup(req_id, queue))
                    continue

                resp_headers = first.get("headers") if isinstance(first.get("headers"), dict) else {}
                if stream:
                    return status, self._stream_chunks(req_id, queue, timeout_s=timeout_s), resp_headers
                raw = await self._collect_body(req_id, queue, timeout_s=timeout_s)
                return status, raw, resp_headers
            except asyncio.TimeoutError as e:
                last_error = "ws tunnel node timeout"
                if req_id:
                    await self._cleanup(req_id)
                if attempt + 1 >= attempts:
                    raise UpstreamTimeoutError(last_error) from e
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                if req_id:
                    await self._cleanup(req_id)
                if attempt + 1 >= attempts:
                    raise UpstreamError(f"WS tunnel failed: {last_error}") from e
        raise BackendUnavailableError(f"WS tunnel unavailable: {last_error}")

    async def _dispatch(
        self,
        *,
        method: str,
        url: str,
        body: dict[str, Any],
        headers: dict[str, str],
        attempt: int,
        account: str | None = None,
    ) -> tuple[str, asyncio.Queue[dict[str, Any]], _Node]:
        node = self._next_node(account)
        if node is None:
            if account:
                raise BackendUnavailableError(
                    f"No WS tunnel node connected for account {account!r}")
            raise BackendUnavailableError("No WS tunnel node connected")
        if len(self._pending) >= _MAX_PENDING:
            raise BackendUnavailableError("WS tunnel pending queue is full")

        req_id = str(uuid.uuid4())
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._pending[req_id] = queue
        self._req_to_node[req_id] = node
        self._node_reqs.setdefault(id(node), set()).add(req_id)

        payload = {
            "req_id": req_id,
            "method": method,
            "path": _path_from_url(url),
            "headers": headers,
            "body": json.dumps(body, ensure_ascii=False),
            "attempt": attempt,
        }
        try:
            async with node.lock:
                await node.ws.send_text(json.dumps(payload, ensure_ascii=False))
        except Exception:
            await self._cleanup(req_id)
            raise
        return req_id, queue, node

    def _next_node(self, account: str | None = None) -> _Node | None:
        now = time.time()
        available = [
            n for n in self._nodes
            if n.cooldown_until <= now and (account is None or n.account in (account, None))
        ]
        if not available:
            return None
        if self._idx >= len(available):
            self._idx = 0
        node = available[self._idx]
        self._idx = (self._idx + 1) % len(available)
        return node

    def _cooldown(self, node: _Node, status: int) -> None:
        seconds = 900 if status in {401, 403} else 60
        node.cooldown_until = time.time() + seconds
        logger.warning("WS tunnel node %s cooldown %ss after HTTP %s", node.label, seconds, status)

    async def _collect_body(
        self,
        req_id: str,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        timeout_s: float,
    ) -> bytes:
        chunks: list[bytes] = []
        try:
            while True:
                msg = await asyncio.wait_for(queue.get(), timeout=timeout_s)
                msg_type = msg.get("type")
                if msg_type == "finish":
                    return b"".join(chunks)
                if msg_type == "error":
                    raise UpstreamError(str(msg.get("body") or "node error"))
                if msg_type == "chunk":
                    chunks.append(str(msg.get("body") or "").encode("utf-8"))
        finally:
            await self._cleanup(req_id)

    async def _stream_chunks(
        self,
        req_id: str,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        timeout_s: float,
    ) -> AsyncIterator[bytes]:
        timeout = min(timeout_s, _STREAM_CHUNK_TIMEOUT_S)
        try:
            while True:
                msg = await asyncio.wait_for(queue.get(), timeout=timeout)
                msg_type = msg.get("type")
                if msg_type == "finish":
                    break
                if msg_type == "error":
                    raise UpstreamError(str(msg.get("body") or "node error"))
                if msg_type == "chunk":
                    chunk = str(msg.get("body") or "")
                    if chunk:
                        yield chunk.encode("utf-8")
        finally:
            await self._cleanup(req_id)

    async def _drain_and_cleanup(self, req_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        try:
            while True:
                msg = await asyncio.wait_for(queue.get(), timeout=5)
                if msg.get("type") in {"finish", "error"}:
                    break
        except Exception:
            pass
        finally:
            await self._cleanup(req_id)

    async def _cleanup(self, req_id: str) -> None:
        self._pending.pop(req_id, None)
        node = self._req_to_node.pop(req_id, None)
        if node is None:
            return
        reqs = self._node_reqs.get(id(node))
        if reqs is not None:
            reqs.discard(req_id)
            if not reqs:
                self._node_reqs.pop(id(node), None)

    async def _remove_node(self, node: _Node) -> None:
        async with self._lock:
            if node in self._nodes:
                self._nodes.remove(node)
        orphaned = list(self._node_reqs.pop(id(node), set()))
        for req_id in orphaned:
            queue = self._pending.pop(req_id, None)
            self._req_to_node.pop(req_id, None)
            if queue is not None:
                queue.put_nowait({"type": "error", "body": "WS tunnel node disconnected"})


def _check_node_token(ws: WebSocket) -> bool:
    expected = os.environ.get("MIMO_WS_TUNNEL_TOKEN", "").strip()
    if not expected:
        return True
    auth = ws.headers.get("authorization", "")
    bearer = auth[7:].strip() if auth.startswith("Bearer ") else ""
    supplied = (
        ws.query_params.get("token")
        or ws.headers.get("x-ws-tunnel-token")
        or bearer
    )
    return supplied == expected


def _account_from_url(url: str) -> str | None:
    """Routing key: the ``account`` query param of a ws:// backend URL.

    ``wss://host/ws?account=kuro-aoki`` → ``"kuro-aoki"``. Absent/empty means
    the request targets the shared, account-agnostic node pool."""
    for key, value in parse_qsl(urlsplit(url).query, keep_blank_values=False):
        if key == "account" and value.strip():
            return value.strip()
    return None


def _path_from_url(url: str) -> str:
    """Build the HTTP-like path forwarded to the bridge node.

    Strips the ``/ws`` tunnel-endpoint prefix and the ``account`` routing
    param (it's a gateway-side selector, not part of the upstream request),
    while preserving any other query string the caller passed."""
    parsed = urlsplit(url)
    path = parsed.path or "/"
    if path == "/ws":
        path = "/"
    elif path.startswith("/ws/"):
        path = path[3:]
    kept = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "account"]
    if kept:
        path += f"?{urlencode(kept)}"
    return path


def _should_retry_status(status: int) -> bool:
    return status in _RETRYABLE_STATUSES or status >= 500


tunnel = WebSocketTunnel()


def is_ws_url(url: str) -> bool:
    return url.startswith(("ws://", "wss://"))


def compose_upstream_url(base_url: str, path: str) -> str:
    """Append a request ``path`` to a backend ``base_url``.

    For HTTP backends this is the plain ``base_url.rstrip("/") + path``. For
    ws:// backends the base may carry an ``?account=`` routing query, so we
    insert ``path`` into the URL path component and keep the query intact
    (a naive concat would shove ``/v1/...`` inside the query string)."""
    if not is_ws_url(base_url):
        return base_url.rstrip("/") + path
    parts = urlsplit(base_url)
    new_path = parts.path.rstrip("/") + path
    return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


async def request_json(
    url: str,
    body: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    timeout_s: float = 60.0,
) -> tuple[int, bytes]:
    status, payload, _headers = await tunnel.request(
        url,
        body,
        headers=headers,
        timeout_s=timeout_s,
        stream=False,
    )
    assert isinstance(payload, bytes)
    return status, payload


async def request_stream(
    url: str,
    body: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    timeout_s: float = 600.0,
) -> tuple[int, AsyncIterator[bytes]]:
    status, payload, _headers = await tunnel.request(
        url,
        body,
        headers=headers,
        timeout_s=timeout_s,
        stream=True,
    )
    assert not isinstance(payload, bytes)
    return status, payload


def register_ws_routes(app: FastAPI) -> None:
    @app.websocket("/ws")
    async def ws_tunnel_endpoint(ws: WebSocket) -> None:
        await tunnel.accept(ws)

    @app.get("/api/gateway/ws-tunnel")
    async def ws_tunnel_status() -> dict[str, Any]:
        return tunnel.status()
