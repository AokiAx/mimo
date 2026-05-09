"""
Gateway runtime — wires the new pipeline (handler + adapters + routing +
transport) into a single process-wide singleton, with backends auto-
discovered from ``data/auto_deploy.json`` (the same file the auto-deploy
scheduler writes).

This module exists so ``app.py`` can call ``dispatch(adapter_name, request)``
without knowing about the pipeline internals. It also exposes status /
backend management helpers in the shape the dashboard already consumes.

Backends are reloaded on startup and on demand via ``reload_backends()``.
A background asyncio task probes every backend every 30s.
"""
from __future__ import annotations

import asyncio
import json
import os
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
from gateway.core import BadRequestError, GatewayError, RequestContext
from gateway.handler import GatewayHandler
from gateway.routing import Backend, BackendRegistry, InMemoryDecisionLog, Router
from gateway.routing.probe import chat_probe
from gateway.transport import HttpxTransport


_AUTO_DEPLOY_JSON = Path(__file__).parent.parent / "data" / "auto_deploy.json"
_DEFAULT_MODEL = "mimo-v2.5-pro"
_PROBE_INTERVAL_S = 30.0
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


def _load_backends_from_auto_deploy() -> list[Backend]:
    """Build a Backend list from enabled accounts in ``auto_deploy.json``.

    Each enabled account becomes one backend pointing at
    ``http://127.0.0.1:{api_port}``. Missing or malformed config yields an
    empty list — the gateway then returns 503 until the file is written.
    """
    if not _AUTO_DEPLOY_JSON.exists():
        return []
    try:
        cfg = json.loads(_AUTO_DEPLOY_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    backends: list[Backend] = []
    accounts = cfg.get("accounts") or {}
    if not isinstance(accounts, dict):
        return []
    for acc_name, acc_cfg in accounts.items():
        if not isinstance(acc_cfg, dict) or not acc_cfg.get("enabled"):
            continue
        api_port = acc_cfg.get("api_port") or acc_cfg.get("port")
        if not api_port:
            continue
        backends.append(Backend(
            backend_id=str(acc_name),
            base_url=f"http://127.0.0.1:{int(api_port)}",
            model=_DEFAULT_MODEL,
            account_id=str(acc_name),
            api_key="sk-Aoki-MiMo",
            metadata={"aliases": "mimo-v2.5,mimo-v2-flash,mimo-v2.5-tts,"
                                 "claude-3-5-sonnet-20241022,gpt-4,gpt-4o"},
        ))
    return backends


def _ensure_initialized() -> None:
    global _registry, _router, _transport, _handler, _decision_log, _adapters
    if _registry is not None:
        return
    _registry = BackendRegistry(_load_backends_from_auto_deploy())
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
    """Re-read ``auto_deploy.json`` and replace the registry contents.

    Returns the new backend count. Existing routing state (EWMA latency,
    in-flight counters, breaker) on backends that survive the reload is
    discarded — the simplest correct behavior. Call after the deploy
    scheduler updates the config file.
    """
    _ensure_initialized()
    assert _registry is not None
    new = _load_backends_from_auto_deploy()
    _registry.replace_all(new)
    return len(new)


# ────────────── dispatch (used by /v1/* routes) ──────────────

async def dispatch(adapter_name: str, request: Request) -> Response:
    """Run a single request through the pipeline and return a FastAPI Response.

    ``adapter_name`` is one of ``openai_chat`` / ``anthropic`` / ``openai_responses``.
    """
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
    """Same shape the old ``gateway.router.get_router_status`` returned."""
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
    """Same shape the old ``gateway.router.get_all_backends`` returned."""
    _ensure_initialized()
    assert _registry is not None
    out: list[dict[str, Any]] = []
    for b in _registry.all():
        out.append({
            "id": b.backend_id,
            "url": b.base_url,
            "healthy": b.is_selectable(),
            "weight": b.weight,
            "avg_latency_ms": round(b.ewma_latency_ms, 1),
            "p95_latency_ms": round(b.ewma_latency_ms, 1),  # no p95 in EWMA, reuse
            "circuit": "open" if b.is_open() else b.health,
            "total_requests": b.total_requests,
            "enabled": not b.is_open() and b.health != "dead",
            "account": b.account_id,
        })
    return out


def toggle_backend(backend_id: str) -> dict[str, Any]:
    """Flip a backend between selectable and not.

    Implementation note: backend health is not a simple bool — we simulate
    a manual disable by tripping or resetting the breaker.
    """
    _ensure_initialized()
    assert _registry is not None
    b = _registry.get(backend_id)
    if b is None:
        return {"success": False, "error": f"Backend {backend_id!r} not found"}
    if b.is_open() or b.health == "dead":
        b.reset_breaker()
        b.health = "alive"
        b.consecutive_failures = 0
        return {"success": True, "message": f"Backend {backend_id!r} enabled"}
    b.trip(60_000_000.0)  # ~1.9 years; effectively permanent until re-enabled
    return {"success": True, "message": f"Backend {backend_id!r} disabled"}


# ────────────── probe loop ──────────────

async def _probe_loop() -> None:
    """Run chat_probe against every backend every interval."""
    _ensure_initialized()
    assert _registry is not None and _transport is not None
    while True:
        try:
            client = _transport._client  # noqa: SLF001
            for b in _registry.all():
                try:
                    await chat_probe(
                        b, client,
                        timeout_s=_PROBE_TIMEOUT_S,
                        cooldown_s=_PROBE_COOLDOWN_S,
                        failure_threshold=_PROBE_FAILURE_THRESHOLD,
                    )
                except Exception:  # noqa: BLE001 - probe MUST NOT crash the loop
                    pass
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(_PROBE_INTERVAL_S)


def start_probe() -> None:
    """Idempotent. Spawns the background probe task if not already running."""
    global _probe_task
    _ensure_initialized()
    if _probe_task is not None and not _probe_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # not in an event loop; FastAPI startup will call us
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
