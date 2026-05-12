"""Gateway runtime — wires the handler pipeline into app.py.

Backends live in ``data/backends.json`` (managed by ``backend_store``).
Credentials come from ``data/secrets.json`` (managed by ``secrets_store``).
No auto-discovery, no hardcoded tokens.

``dispatch()`` is the only entry point used by app.py. ``get_router_status``,
``get_all_backends``, and ``toggle_backend`` feed the dashboard.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from gateway.adapters import (
    AnthropicAdapter,
    OpenAIChatAdapter,
    OpenAIResponsesAdapter,
    ProtocolAdapter,
)
from gateway.backend_store import list_backends as _list_persisted
from gateway.core import BadRequestError, GatewayError, RequestContext
from gateway.handler import GatewayHandler
from gateway.routing import Backend, BackendRegistry, InMemoryDecisionLog, Router
from gateway.secrets_store import secrets
from gateway.transport import HttpxTransport

_PROBE_INTERVAL_S = 60.0
_PROBE_TIMEOUT_S = 5.0
_PROBE_FAILURE_THRESHOLD = 3
_PROBE_COOLDOWN_S = 30.0
_DEFAULT_REQUEST_TIMEOUT_S = 600.0

# ────────────── singleton state ──────────────

_registry: BackendRegistry | None = None
_router: Router | None = None
_transport: HttpxTransport | None = None
_handler: GatewayHandler | None = None
_decision_log: InMemoryDecisionLog | None = None
_adapters: dict[str, ProtocolAdapter] = {}
_probe_task: asyncio.Task | None = None
_started_at: float = time.time()
_total_requests: int = 0


# ────────────── backend loading ──────────────


def _build_backends_from_store() -> list[Backend]:
    """Read ``data/backends.json`` and produce Backend objects."""
    out: list[Backend] = []
    for entry in _list_persisted():
        meta: dict[str, str] = {}
        name = entry.get("name") or ""
        if name:
            meta["name"] = name
        api_key = entry.get("api_key") or secrets.upstream_api_key
        models = entry.get("models") or []
        if not isinstance(models, list):
            models = []
        out.append(Backend(
            backend_id=entry["id"],
            base_url=entry["base_url"],
            models=[m for m in models if isinstance(m, str) and m],
            account_id=entry.get("account_id") or "",
            api_key=api_key,
            weight=max(1, int(entry.get("weight") or 1)),
            enabled=bool(entry.get("enabled", True)),
            metadata=meta,
        ))
    return out


def _ensure_initialized() -> None:
    global _registry, _router, _transport, _handler, _decision_log, _adapters
    if _registry is not None:
        return
    _registry = BackendRegistry(_build_backends_from_store())
    _router = Router(_registry)
    _transport = HttpxTransport()
    _decision_log = InMemoryDecisionLog(capacity=4096)
    _adapters = {
        "openai_chat": OpenAIChatAdapter(),
        "anthropic": AnthropicAdapter(),
        "openai_responses": OpenAIResponsesAdapter(),
    }
    _handler = GatewayHandler(
        router=_router,
        transport=_transport,
        decision_log=_decision_log,
        metrics=_make_metrics_recorder(),
        upstream_timeout_s=_DEFAULT_REQUEST_TIMEOUT_S,
    )


def _make_metrics_recorder():
    try:
        from gateway.metrics import SQLiteMetricsRecorder
        return SQLiteMetricsRecorder()
    except Exception:
        return None


def reload_backends() -> int:
    """Re-read ``backends.json`` and replace the in-memory registry.

    Returns the new backend count. Existing EWMA / breaker state is discarded.
    """
    _ensure_initialized()
    assert _registry is not None
    new = _build_backends_from_store()
    _registry.replace_all(new)
    return len(new)


# ────────────── dispatch (used by /v1/* routes) ──────────────


async def dispatch(adapter_name: str, request: Request) -> Response:
    """Run a single request through the pipeline and return a FastAPI Response."""
    global _total_requests
    _ensure_initialized()
    assert _handler is not None
    adapter = _adapters[adapter_name]

    try:
        body = await _read_json_body(request)
    except BadRequestError as e:
        return _error_response(adapter, e)

    ctx = _ctx_from_request(request, adapter)
    _total_requests += 1

    try:
        content_type, stream_iter, body_bytes = await _handler.handle(ctx, adapter, body)
    except GatewayError as e:
        return _error_response(adapter, e)
    except Exception as e:  # noqa: BLE001
        err = BadRequestError(f"Internal error: {type(e).__name__}: {e}")
        err.error_code = "internal_error"
        err.http_status = 500
        return _error_response(adapter, err)

    headers = {"Access-Control-Allow-Origin": "*"}
    if stream_iter is not None:
        headers["Cache-Control"] = "no-cache"
        return StreamingResponse(stream_iter, media_type=content_type, headers=headers)
    return Response(content=body_bytes, media_type=content_type, status_code=200, headers=headers)


# ────────────── status helpers (used by panel) ──────────────


def get_router_status() -> dict[str, Any]:
    _ensure_initialized()
    assert _registry is not None
    backends = _registry.all()
    healthy = sum(1 for b in backends if b.is_selectable())
    latencies = [b.ewma_latency_ms for b in backends if b.ewma_latency_ms > 0]
    avg_lat = round(sum(latencies) / len(latencies), 1) if latencies else 0
    uptime = int(time.time() - _started_at)
    return {
        "uptime": uptime,
        "total_requests": _total_requests,
        "qps": round(_total_requests / max(uptime, 1), 2),
        "avg_latency_ms": avg_lat,
        "backends_total": len(backends),
        "backends_healthy": healthy,
        "pool_idle": 0,
        "pool_active": 0,
        "pool_reuse_rate": 0,
    }


def get_all_backends() -> list[dict[str, Any]]:
    _ensure_initialized()
    assert _registry is not None
    out: list[dict[str, Any]] = []
    for b in _registry.all():
        out.append({
            "id": b.backend_id,
            "name": b.metadata.get("name", b.backend_id),
            "url": b.base_url,
            "models": list(b.models),
            "healthy": b.is_selectable(),
            "weight": b.weight,
            "avg_latency_ms": round(b.ewma_latency_ms, 1),
            "p95_latency_ms": round(b.ewma_latency_ms, 1),
            "circuit": "open" if b.is_open() else b.health,
            "total_requests": b.total_requests,
            "enabled": b.enabled,
            "account": b.account_id,
        })
    return out


def toggle_backend(backend_id: str) -> dict[str, Any]:
    """Flip a backend's enabled flag and persist the change."""
    _ensure_initialized()
    assert _registry is not None
    b = _registry.get(backend_id)
    if b is None:
        return {"success": False, "error": f"Backend {backend_id!r} not found"}

    new_enabled = not b.enabled
    b.enabled = new_enabled
    if new_enabled:
        b.reset_breaker()
        b.health = "alive"
        b.consecutive_failures = 0

    # Persist
    from gateway.backend_store import update_backend
    update_backend(backend_id, enabled=new_enabled)

    label = "启用" if new_enabled else "禁用"
    return {"success": True, "message": f"Backend {backend_id!r} {label}"}


# ────────────── HTTP HEAD probe loop ──────────────


async def _probe_loop() -> None:
    """Lightweight HTTP GET /health probe — no tokens wasted."""
    _ensure_initialized()
    assert _registry is not None and _transport is not None
    client = _transport._client  # noqa: SLF001
    while True:
        try:
            for b in _registry.all():
                if not b.enabled:
                    continue
                try:
                    started = time.monotonic()
                    resp = await client.get(
                        b.base_url.rstrip("/") + "/v1/models",
                        headers={"Authorization": f"Bearer {b.api_key}"} if b.api_key else {},
                        timeout=_PROBE_TIMEOUT_S,
                    )
                    latency = (time.monotonic() - started) * 1000
                    if 200 <= resp.status_code < 400:
                        b.record_success()
                        b.record_latency(latency)
                    else:
                        b.record_failure(
                            f"http {resp.status_code}",
                            cooldown_s=_PROBE_COOLDOWN_S,
                            threshold=_PROBE_FAILURE_THRESHOLD,
                        )
                except Exception as e:
                    b.record_failure(
                        f"{type(e).__name__}: {e}",
                        cooldown_s=_PROBE_COOLDOWN_S,
                        threshold=_PROBE_FAILURE_THRESHOLD,
                    )
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(_PROBE_INTERVAL_S)


def start_probe() -> None:
    """Idempotent. Spawns the background probe task."""
    global _probe_task
    _ensure_initialized()
    if _probe_task is not None and not _probe_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _probe_task = loop.create_task(_probe_loop())


# ────────────── helpers ──────────────


def _ctx_from_request(request: Request, adapter: ProtocolAdapter) -> RequestContext:
    headers = {k.lower(): v for k, v in request.headers.items()}
    return RequestContext(
        client_ip=request.client.host if request.client else "",
        user_agent=headers.get("user-agent", ""),
        headers=headers,
        src_protocol=adapter.name,
        src_path=str(request.url.path),
        src_method=request.method,
    )


async def _read_json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        raise BadRequestError("Empty request body")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise BadRequestError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise BadRequestError("Request body must be a JSON object")
    return data


def _error_response(adapter: ProtocolAdapter, err: GatewayError) -> Response:
    body = adapter.error_envelope(err)
    return Response(
        content=body,
        media_type="application/json",
        status_code=getattr(err, "http_status", 500),
    )


__all__ = [
    "dispatch",
    "reload_backends",
    "get_router_status",
    "get_all_backends",
    "toggle_backend",
    "start_probe",
]
