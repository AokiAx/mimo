"""Gateway runtime — wires the handler pipeline into app.py.

Backends live in ``data/backends.json`` (managed by ``backend_store``).
Credentials come from ``data/secrets.json`` (managed by ``secrets_store``).

The runtime owns backend lifecycle for account rotation:
new/reloaded backends are warmed with non-stream, stream, and tool-call probes;
when ready, each healthy backend joins the active load-balancing pool. Backends
only enter draining when they are removed, disabled, or being redeployed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default


_PROBE_INTERVAL_S = 30.0
_PROBE_TIMEOUT_S = 10.0
_PROBE_FAILURE_THRESHOLD = 4
_PROBE_COOLDOWN_S = 20.0
_PROBE_CONCURRENCY = 10
_DEFAULT_REQUEST_TIMEOUT_S = 600.0
_CHARSET_RE = re.compile(r"charset=([^;]+)", re.IGNORECASE)

# Account-rotation defaults. Cloud deployments are expected to live for about
# one hour; rotate at 50 minutes to leave room for deploy + readiness checks.
_ROTATION_INTERVAL_S = 50 * 60.0
_READINESS_INTERVAL_S = 5.0
_READINESS_REQUIRED_SUCCESSES = 1
_DRAIN_TIMEOUT_S = _env_float("GATEWAY_DRAIN_TIMEOUT_S", 10 * 60.0)
_DEPLOY_DRAIN_GRACE_S = _env_float("GATEWAY_DEPLOY_DRAIN_GRACE_S", 20.0)
_DETECTION_ZONE_FAILURES = 3           # consecutive failures to enter detection
_DETECTION_PROBE_INTERVAL_S = 10.0      # fast probe interval in detection zone
_ROTATION_LOOP_INTERVAL_S = _env_float("GATEWAY_ROTATION_LOOP_INTERVAL_S", 15.0)

_READINESS_MAX_STREAM_CHUNKS = 32
_READINESS_MAX_STREAM_SECONDS = 20.0
_READINESS_PROMPT = "ping"

# ────────────── singleton state ──────────────

_registry: BackendRegistry | None = None
_router: Router | None = None
_transport: HttpxTransport | None = None
_handler: GatewayHandler | None = None
_decision_log: InMemoryDecisionLog | None = None
_adapters: dict[str, ProtocolAdapter] = {}
_probe_task: asyncio.Task | None = None
_rotation_task: asyncio.Task | None = None
_started_at: float = time.time()
_total_requests: int = 0


# ────────────── backend loading / reconcile ──────────────


def _build_backend_from_entry(entry: dict[str, Any]) -> Backend:
    meta: dict[str, str] = {}
    name = entry.get("name") or ""
    if name:
        meta["name"] = name
    api_key = entry.get("api_key") or secrets.upstream_api_key
    models = entry.get("models") or []
    if not isinstance(models, list):
        models = []
    lifecycle = entry.get("lifecycle") or "active"
    if lifecycle not in {"standby", "warming", "active", "draining", "failed", "disabled"}:
        lifecycle = "active"
    return Backend(
        backend_id=entry["id"],
        base_url=entry["base_url"],
        models=[m for m in models if isinstance(m, str) and m],
        account_id=entry.get("account_id") or "",
        api_key=api_key,
        weight=max(1, int(entry.get("weight") or 1)),
        enabled=bool(entry.get("enabled", True)),
        metadata=meta,
        lifecycle=lifecycle,
        generation_id=entry.get("generation_id") or entry.get("id") or "",
        rotation_failures=max(0, int(entry.get("rotation_failures") or 0)),
        disabled_until=float(entry.get("disabled_until") or 0.0),
        in_detection=bool(entry.get("in_detection", False)),
        detection_entered_at=float(entry.get("detection_entered_at") or 0.0),
    )


def _build_backends_from_store() -> list[Backend]:
    """Read ``data/backends.json`` and produce Backend objects."""
    return [_build_backend_from_entry(entry) for entry in _list_persisted()]


def _ensure_initialized() -> None:
    global _registry, _router, _transport, _handler, _decision_log, _adapters
    if _registry is not None:
        return
    _registry = BackendRegistry(_build_backends_from_store())
    _bootstrap_active_lifecycles()
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



def _bootstrap_active_lifecycles() -> None:
    """Seed active timers while preserving all active backends for load balancing."""
    assert _registry is not None
    now = time.time()
    for b in _registry.all():
        if b.lifecycle == "active" and not b.active_since:
            b.mark_active(now=now)
        elif b.lifecycle == "draining" and not b.drain_deadline:
            b.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S, now=now)

def _make_metrics_recorder():
    try:
        from gateway.metrics import QueuedSQLiteMetricsRecorder
        return QueuedSQLiteMetricsRecorder()
    except Exception:
        return None


def reload_backends() -> int:
    """Re-read ``backends.json`` and reconcile the in-memory registry.

    Existing Backend objects keep EWMA, breaker, and in-flight state. Removed
    backends are marked draining instead of being dropped while requests are in
    flight. New backends start warming when an active peer for the same model
    exists; otherwise they are activated to avoid bootstrapping into outage.
    """
    _ensure_initialized()
    assert _registry is not None
    new_entries = _build_backends_from_store()
    now = time.time()
    seen: set[str] = set()

    for fresh in new_entries:
        seen.add(fresh.backend_id)
        existing = _registry.get(fresh.backend_id)
        if existing is None:
            if fresh.lifecycle == "warming" and not _has_active_peer(fresh):
                fresh.mark_active(now=now)
            elif fresh.lifecycle == "active":
                fresh.mark_active(now=now)
            _registry.add(fresh)
            continue

        existing.base_url = fresh.base_url
        existing.models = fresh.models
        existing.account_id = fresh.account_id
        existing.api_key = fresh.api_key
        existing.weight = fresh.weight
        existing.enabled = fresh.enabled
        existing.metadata = fresh.metadata
        existing.generation_id = fresh.generation_id
        existing.rotation_failures = fresh.rotation_failures
        existing.disabled_until = fresh.disabled_until
        existing.in_detection = fresh.in_detection
        existing.detection_entered_at = fresh.detection_entered_at
        if fresh.lifecycle != existing.lifecycle:
            if fresh.lifecycle == "warming":
                existing.mark_warming(now=now)
            elif fresh.lifecycle == "active":
                existing.mark_active(now=now)
            elif fresh.lifecycle == "draining":
                existing.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S, now=now)
            else:
                existing.lifecycle = fresh.lifecycle

    for old in _registry.all():
        if old.backend_id in seen:
            continue
        if old.in_flight > 0:
            if old.lifecycle != "draining":
                old.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S, now=now)
        else:
            _registry.remove(old.backend_id)
    _reap_drained(now=now)
    return len(_registry.all())


def _has_active_peer(candidate: Backend) -> bool:
    assert _registry is not None
    c_models = set(candidate.models)
    return any(
        b.backend_id != candidate.backend_id
        and b.enabled
        and b.lifecycle == "active"
        and bool(c_models.intersection(b.models))
        for b in _registry.all()
    )


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

    from gateway.probe_dump import dump_inbound, dump_outbound, tee_stream
    dump_inbound(adapter_name, body)

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
        return StreamingResponse(
            tee_stream(adapter_name, stream_iter),
            media_type=content_type, headers=headers,
        )
    dump_outbound(adapter_name, body_bytes)
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
        "backends_active": sum(1 for b in backends if b.lifecycle == "active"),
        "backends_standby": sum(1 for b in backends if b.lifecycle == "standby"),
        "backends_warming": sum(1 for b in backends if b.lifecycle == "warming"),
        "backends_draining": sum(1 for b in backends if b.lifecycle == "draining"),
        "rotation_interval_s": int(_ROTATION_INTERVAL_S),
        "pool_idle": 0,
        "pool_active": 0,
        "pool_reuse_rate": 0,
    }


def get_all_backends() -> list[dict[str, Any]]:
    _ensure_initialized()
    assert _registry is not None
    now = time.time()
    out: list[dict[str, Any]] = []
    for b in _registry.all():
        out.append({
            "id": b.backend_id,
            "name": b.metadata.get("name", b.backend_id),
            "url": b.base_url,
            "models": list(b.models),
            "healthy": b.is_selectable(now),
            "weight": b.weight,
            "avg_latency_ms": round(b.ewma_latency_ms, 1),
            "p95_latency_ms": round(b.ewma_latency_ms, 1),
            "circuit": "open" if b.is_open(now) else b.health,
            "total_requests": b.total_requests,
            "enabled": b.enabled,
            "account": b.account_id,
            "lifecycle": b.lifecycle,
            "generation_id": b.generation_id,
            "in_flight": b.in_flight,
            "active_for_s": round(now - b.active_since, 1) if b.active_since else 0,
            "draining_for_s": round(now - b.draining_since, 1) if b.draining_since else 0,
            "drain_deadline_s": round(b.drain_deadline - now, 1) if b.drain_deadline else 0,
            "readiness_successes": b.readiness_successes,
            "readiness_failures": b.readiness_failures,
            "rotation_failures": b.rotation_failures,
            "disabled_until": b.disabled_until,
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
        b.disabled_until = 0.0
        b.in_detection = False
        b.detection_entered_at = 0.0
        if b.lifecycle in ("disabled", "failed"):
            b.mark_warming()
    else:
        b.lifecycle = "disabled"

    from gateway.backend_store import update_backend
    update_backend(backend_id, enabled=new_enabled, lifecycle=b.lifecycle, disabled_until=b.disabled_until)

    label = "启用" if new_enabled else "禁用"
    return {"success": True, "message": f"Backend {backend_id!r} {label}"}


def activate_backend(backend_id: str) -> dict[str, Any]:
    """Hard-switch traffic to one ready backend and drain peers for shared models."""
    _ensure_initialized()
    assert _registry is not None
    b = _registry.get(backend_id)
    if b is None:
        return {"success": False, "error": f"Backend {backend_id!r} not found"}
    _activate_backend(b, reason="manual")
    return {"success": True, "backend": backend_id}


def prepare_account_deploy(account_id: str, *, api_port: int | None = None) -> dict[str, Any]:
    """Move traffic away from backends that are about to have their Claw destroyed.

    Auto-deploy recreates a Claw in-place behind the same jump-server port. If
    the gateway keeps routing new requests to that active backend while Step 0
    destroys the old Claw/tunnel, clients see avoidable 5xxs until the next
    manual reload or health-probe cycle. This helper is intentionally sync so
    the deploy thread can call it before touching the old Claw: active matching
    backends are drained only when another active peer can serve their models.
    """
    _ensure_initialized()
    assert _registry is not None
    targets = _matching_account_backends(account_id, api_port=api_port)
    drained: list[str] = []
    blocked: list[str] = []
    for backend in targets:
        if backend.lifecycle != "active":
            continue
        if not _has_active_peer(backend):
            blocked.append(backend.backend_id)
            continue
        backend.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S)
        _persist_backend_runtime_state(backend)
        drained.append(backend.backend_id)
    _reap_drained()
    return {
        "success": True,
        "account": account_id,
        "matched": [b.backend_id for b in targets],
        "drained": drained,
        "blocked": blocked,
    }


def wait_for_account_drain(
    account_id: str,
    *,
    api_port: int | None = None,
    timeout_s: float | None = None,
    poll_s: float = 0.25,
) -> dict[str, Any]:
    """Block briefly until matching draining backends have no in-flight requests."""
    _ensure_initialized()
    deadline = time.monotonic() + (timeout_s if timeout_s is not None else _DEPLOY_DRAIN_GRACE_S)
    pending: list[str] = []
    while True:
        targets = [b for b in _matching_account_backends(account_id, api_port=api_port) if b.lifecycle == "draining"]
        pending = [f"{b.backend_id}:{b.in_flight}" for b in targets if b.in_flight > 0]
        if not pending or time.monotonic() >= deadline:
            break
        time.sleep(max(0.05, poll_s))
    return {"success": not pending, "pending": pending}


def complete_account_deploy(account_id: str, *, api_port: int | None = None) -> dict[str, Any]:
    """Reload and warm/activate backends that belong to a freshly deployed Claw."""
    reload_backends()
    assert _registry is not None
    now = time.time()
    targets = _matching_account_backends(account_id, api_port=api_port)
    warmed: list[str] = []
    activated: list[str] = []
    for backend in targets:
        backend.enabled = True
        backend.disabled_until = 0.0
        backend.reset_breaker()
        backend.health = "alive"
        backend.consecutive_failures = 0
        backend.last_error = ""
        if _has_active_peer(backend):
            backend.mark_warming(now=now)
            backend.last_probe_at = 0.0
            warmed.append(backend.backend_id)
        else:
            # The deploy flow has already verified the endpoint. If no peer is
            # carrying traffic, restore service immediately instead of waiting
            # for the background rotation loop.
            backend.mark_active(now=now)
            activated.append(backend.backend_id)
        _persist_backend_runtime_state(backend)
    return {
        "success": True,
        "account": account_id,
        "matched": [b.backend_id for b in targets],
        "warmed": warmed,
        "activated": activated,
    }


def fail_account_deploy(account_id: str, *, api_port: int | None = None, error: str = "deploy failed") -> dict[str, Any]:
    """Keep failed redeploy targets out of routing until an operator fixes them."""
    _ensure_initialized()
    assert _registry is not None
    targets = _matching_account_backends(account_id, api_port=api_port)
    failed: list[str] = []
    for backend in targets:
        if backend.lifecycle == "active" and not _has_active_peer(backend):
            # Single-backend installs have no failover target; leave the backend
            # visible so the router can recover if the old tunnel is still alive.
            continue
        backend.mark_failed_rotation(error)
        _persist_backend_runtime_state(backend)
        failed.append(backend.backend_id)
    return {"success": True, "account": account_id, "matched": [b.backend_id for b in targets], "failed": failed}


def _matching_account_backends(account_id: str, *, api_port: int | None = None) -> list[Backend]:
    assert _registry is not None
    keys = _account_match_keys(account_id)
    return [
        b for b in _registry.all()
        if (b.account_id and b.account_id in keys)
        or (api_port is not None and _base_url_port(b.base_url) == api_port)
    ]


def _account_match_keys(account_id: str) -> set[str]:
    raw = (account_id or "").strip()
    keys = {raw} if raw else set()
    if raw.endswith(".json"):
        keys.add(raw[:-5])
    elif raw:
        keys.add(f"{raw}.json")
    return keys


def _base_url_port(base_url: str) -> int | None:
    match = re.search(r":(\d+)(?:/)?$", (base_url or "").rstrip("/"))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


# ────────────── probe / readiness / rotation loops ──────────────


async def _probe_loop() -> None:
    """Lightweight liveness probe with detection-zone quarantine.

    Normal backends are probed every ``_PROBE_INTERVAL_S`` (30 s).
    After ``_DETECTION_ZONE_FAILURES`` (2) consecutive failures a backend
    enters the **detection zone** where it is probed every
    ``_DETECTION_PROBE_INTERVAL_S`` (10 s).  One success exits the zone.
    """
    _ensure_initialized()
    assert _registry is not None and _transport is not None
    client = _transport._client  # noqa: SLF001

    async def _probe_one(backend: Backend, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            if not backend.enabled:
                return
            try:
                started = time.monotonic()
                resp = await client.get(
                    backend.base_url.rstrip("/") + "/v1/models",
                    headers={"Authorization": f"Bearer {backend.api_key}"} if backend.api_key else {},
                    timeout=_PROBE_TIMEOUT_S,
                )
                latency = (time.monotonic() - started) * 1000
                if 200 <= resp.status_code < 500:
                    backend.record_success()
                    backend.record_latency(latency)
                    # Exit detection zone on success
                    if backend.in_detection:
                        backend.exit_detection()
                        logger.info(
                            "Backend %s exited detection zone (healthy)",
                            backend.backend_id,
                        )
                    # Recovery: a failed backend that passes liveness check
                    # goes back to standby so the rotation loop can re-warm it.
                    if backend.lifecycle == "failed":
                        backend.mark_standby()
                        logger.info(
                            "Backend %s recovered from failed → standby",
                            backend.backend_id,
                        )
                else:
                    backend.record_probe_failure(
                        f"http {resp.status_code}",
                        cooldown_s=_PROBE_COOLDOWN_S,
                        threshold=_PROBE_FAILURE_THRESHOLD,
                    )
                    # Enter detection zone after N consecutive probe failures
                    if (backend.probe_consecutive_failures >= _DETECTION_ZONE_FAILURES
                            and not backend.in_detection):
                        backend.mark_detection()
                        logger.warning(
                            "Backend %s entered detection zone (%d consecutive probe failures)",
                            backend.backend_id, backend.probe_consecutive_failures,
                        )
            except Exception as e:
                backend.record_probe_failure(
                    f"{type(e).__name__}: {e}",
                    cooldown_s=_PROBE_COOLDOWN_S,
                    threshold=_PROBE_FAILURE_THRESHOLD,
                )
                if (backend.probe_consecutive_failures >= _DETECTION_ZONE_FAILURES
                        and not backend.in_detection):
                    backend.mark_detection()
                    logger.warning(
                        "Backend %s entered detection zone (%d consecutive probe failures)",
                        backend.backend_id, backend.probe_consecutive_failures,
                    )

    while True:
        try:
            enabled_backends = [b for b in _registry.all() if b.enabled]
            semaphore = asyncio.Semaphore(_PROBE_CONCURRENCY)
            await asyncio.gather(
                *(_probe_one(b, semaphore) for b in enabled_backends),
                return_exceptions=True,
            )
            _reap_drained()
        except Exception:  # noqa: BLE001
            logger.exception("Gateway probe loop failed")
        # Use shorter interval if any backend is in detection zone
        any_in_detection = any(
            b.in_detection for b in _registry.all() if b.enabled
        )
        interval = _DETECTION_PROBE_INTERVAL_S if any_in_detection else _PROBE_INTERVAL_S
        await asyncio.sleep(interval)


async def _rotation_loop() -> None:
    """Warm standby backends periodically so they can join the active pool."""
    _ensure_initialized()
    assert _registry is not None
    while True:
        try:
            _promote_standby_backends_to_warming()
            await _warm_ready_backends()
            _rotate_expired_active_backends()
            _reap_drained()
        except Exception:  # noqa: BLE001
            logger.exception("Gateway rotation loop failed")
        await asyncio.sleep(_ROTATION_LOOP_INTERVAL_S)


def _promote_standby_backends_to_warming() -> None:
    """Continuously fill the load-balancing pool from enabled standby backends."""
    assert _registry is not None
    now = time.time()
    for b in _registry.all():
        if not b.enabled or b.lifecycle != "standby" or b.is_temporarily_disabled(now):
            continue
        if _has_active_peer(b):
            b.mark_warming(now=now)
        else:
            # If this model currently has no active capacity, avoid leaving the
            # gateway unavailable while waiting for the next readiness cycle.
            b.mark_active(now=now)
        _persist_backend_runtime_state(b)


async def _warm_ready_backends() -> None:
    assert _registry is not None
    now = time.time()
    for b in _registry.all():
        if not b.enabled or b.lifecycle != "warming" or b.is_temporarily_disabled(now):
            continue
        if b.last_probe_at and now - b.last_probe_at < _READINESS_INTERVAL_S:
            continue
        ok, reason, latency_ms = await _run_readiness_checks(b)
        if ok:
            b.record_success(now=now)
            b.record_latency(latency_ms)
            b.readiness_successes += 1
            b.ready_at = now
            if b.readiness_successes >= _READINESS_REQUIRED_SUCCESSES:
                _activate_backend(b, reason="readiness")
        else:
            b.readiness_failures += 1
            b.record_failure(reason, now=now, threshold=_PROBE_FAILURE_THRESHOLD)
            if b.readiness_failures >= _PROBE_FAILURE_THRESHOLD:
                b.mark_failed_rotation(reason, now=now)
                _persist_backend_runtime_state(b)


def _rotate_expired_active_backends() -> None:
    assert _registry is not None
    now = time.time()
    for active in list(_registry.all()):
        if not active.enabled or active.lifecycle != "active" or not active.active_since:
            continue
        if now - active.active_since < _ROTATION_INTERVAL_S:
            continue
        nxt = _next_ready_or_warm_peer(active, now=now)
        if nxt is None:
            continue
        if nxt.lifecycle == "active":
            continue
        if nxt.lifecycle != "warming":
            nxt.mark_warming(now=now)
        # The readiness loop will activate it after the three probe modes pass.


def _next_ready_or_warm_peer(active: Backend, *, now: float) -> Backend | None:
    assert _registry is not None
    models = set(active.models)
    peers = [
        b for b in _registry.all()
        if b.backend_id != active.backend_id
        and b.enabled
        and not b.is_temporarily_disabled(now)
        and b.lifecycle in ("standby", "warming", "active")
        and bool(models.intersection(b.models))
    ]
    if not peers:
        return None
    peers.sort(key=lambda b: (b.active_since or 0.0, b.backend_id))
    return peers[0]


def _activate_backend(backend: Backend, *, reason: str) -> None:
    """Add a warmed backend to the active load-balancing pool."""
    now = time.time()
    backend.mark_active(now=now)
    shared_models = set(backend.models)
    _persist_backend_runtime_state(backend)
    logger.info("Activated backend %s by %s; load-balancing models=%s", backend.backend_id, reason, sorted(shared_models))


def _reap_drained(*, now: float | None = None) -> None:
    assert _registry is not None
    n = now or time.time()
    persisted_ids = _persisted_backend_ids()
    for b in list(_registry.all()):
        if b.lifecycle != "draining":
            continue
        if b.in_flight > 0 and not (b.drain_deadline and n >= b.drain_deadline):
            continue
        if b.in_flight > 0:
            logger.warning(
                "Drain deadline reached for backend %s with %s in-flight request(s); "
                "removing it from new routing while existing handlers continue draining",
                b.backend_id,
                b.in_flight,
            )
        if b.backend_id in persisted_ids:
            b.mark_standby()
            _persist_backend_runtime_state(b)
        else:
            _registry.remove(b.backend_id)


def _persisted_backend_ids() -> set[str]:
    try:
        return {str(entry.get("id")) for entry in _list_persisted() if entry.get("id")}
    except Exception:
        return set()


async def _run_readiness_checks(backend: Backend) -> tuple[bool, str, float]:
    started = time.monotonic()
    checks = (
        ("non_stream", _readiness_non_stream_body),
        ("stream", _readiness_stream_body),
        ("tool_call", _readiness_tool_call_body),
    )
    for name, body_factory in checks:
        try:
            body = body_factory(backend)
            ok, reason = await _run_one_readiness_check(backend, name, body)
        except Exception as e:  # noqa: BLE001
            ok = False
            reason = f"{type(e).__name__}: {e}"
        if not ok:
            return False, f"{name}: {reason}", (time.monotonic() - started) * 1000
    return True, "ready", (time.monotonic() - started) * 1000


async def _run_one_readiness_check(backend: Backend, name: str, body: dict[str, Any]) -> tuple[bool, str]:
    assert _transport is not None
    url = backend.base_url.rstrip("/") + "/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    if backend.api_key:
        headers["Authorization"] = f"Bearer {backend.api_key}"
    try:
        if body.get("stream"):
            status, raw_iter = await _transport.post_stream(
                url, body, headers=headers, timeout_s=_READINESS_MAX_STREAM_SECONDS,
            )
            if status >= 400:
                return False, f"http {status}"
            chunks = 0
            deadline = time.monotonic() + _READINESS_MAX_STREAM_SECONDS
            async for _chunk in raw_iter:
                chunks += 1
                if chunks >= _READINESS_MAX_STREAM_CHUNKS or time.monotonic() >= deadline:
                    break
            return (chunks > 0), "empty stream" if chunks <= 0 else "ok"

        status, raw = await _transport.post_json(
            url, body, headers=headers, timeout_s=_PROBE_TIMEOUT_S,
        )
        if status >= 400:
            return False, f"http {status}: {raw[:200]!r}"
        if name == "tool_call":
            ok, reason = _raw_response_is_valid(raw)
            if not ok:
                return False, reason
        return True, "ok"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def _raw_response_is_valid(raw: bytes) -> tuple[bool, str]:
    """Check that the tool-call readiness response is structurally valid.

    We no longer require tool_calls in the response — models may choose to
    respond with text instead of calling the tool.  A valid JSON response
    with at least one choice is sufficient.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return False, f"invalid JSON: {e}"
    choices = data.get("choices") if isinstance(data, dict) else None
    if not isinstance(choices, list) or not choices:
        return False, "response has no choices"
    return True, "ok"


def _readiness_model(backend: Backend) -> str:
    if not backend.models:
        raise ValueError("backend has no configured models for readiness checks")
    return backend.models[0]


def _readiness_non_stream_body(backend: Backend) -> dict[str, Any]:
    return {
        "model": _readiness_model(backend),
        "messages": [{"role": "user", "content": _READINESS_PROMPT}],
        "max_tokens": 256,
        "stream": False,
    }


def _readiness_stream_body(backend: Backend) -> dict[str, Any]:
    body = _readiness_non_stream_body(backend)
    body["stream"] = True
    body["stream_options"] = {"include_usage": True}
    return body


def _readiness_tool_call_body(backend: Backend) -> dict[str, Any]:
    body = _readiness_non_stream_body(backend)
    body["messages"] = [{"role": "user", "content": "Use the gateway_readiness_ping tool."}]
    body["tools"] = [{
        "type": "function",
        "function": {
            "name": "gateway_readiness_ping",
            "description": "Return a small readiness acknowledgement.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ok": {"type": "boolean"},
                },
                "required": ["ok"],
            },
        },
    }]
    return body


def _persist_backend_runtime_state(backend: Backend) -> None:
    try:
        from gateway.backend_store import update_backend
        update_backend(
            backend.backend_id,
            enabled=backend.enabled,
            lifecycle=backend.lifecycle,
            generation_id=backend.generation_id,
            rotation_failures=backend.rotation_failures,
            disabled_until=backend.disabled_until,
            in_detection=backend.in_detection,
            detection_entered_at=backend.detection_entered_at,
        )
    except Exception:
        logger.exception("Failed to persist backend state for %s", backend.backend_id)


def start_probe() -> None:
    """Idempotent. Spawns background liveness and rotation tasks."""
    global _probe_task, _rotation_task
    _ensure_initialized()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if _probe_task is None or _probe_task.done():
        _probe_task = loop.create_task(_probe_loop())
    if _rotation_task is None or _rotation_task.done():
        _rotation_task = loop.create_task(_rotation_loop())


async def shutdown() -> None:
    """Close runtime-owned background resources."""
    global _probe_task, _rotation_task
    tasks = (_probe_task, _rotation_task)
    _probe_task = None
    _rotation_task = None
    for task in tasks:
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if _transport is not None:
        await _transport.close()
    if _handler is not None:
        metrics = getattr(_handler, "_metrics", None)
        close = getattr(metrics, "close", None)
        if close is not None:
            close()


# ────────────── helpers ──────────────


def _ctx_from_request(request: Request, adapter: ProtocolAdapter) -> RequestContext:
    headers = {k.lower(): v for k, v in request.headers.items()}
    principal = getattr(request.state, "gateway_principal", None)
    return RequestContext(
        client_ip=request.client.host if request.client else "",
        user_agent=headers.get("user-agent", ""),
        headers=headers,
        api_key_id=getattr(principal, "key_id", None),
        principal=principal,
        src_protocol=adapter.name,
        src_path=str(request.url.path),
        src_method=request.method,
    )


async def _read_json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        raise BadRequestError("Empty request body")

    text, encoding = _decode_json_body(raw, request.headers.get("content-type", ""))
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise BadRequestError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise BadRequestError("Request body must be a JSON object")
    if encoding not in {"utf-8", "utf-8-sig"}:
        logger.info("Decoded JSON request body using %s", encoding)
    return data


def _decode_json_body(raw: bytes, content_type: str = "") -> tuple[str, str]:
    """Decode a JSON request body without leaking ``UnicodeDecodeError``.

    JSON clients should send UTF-8, but some callers incorrectly post bodies
    encoded as GBK/GB18030 (common with Chinese text). Try the declared
    charset first, then UTF-8 variants, and finally GB18030 as a compatibility
    fallback. If all decoders fail, surface a protocol-level 400 instead of an
    unhandled server exception.
    """
    encodings = _candidate_json_encodings(content_type)
    failures: list[str] = []
    for encoding in encodings:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError as e:
            failures.append(f"{encoding}: byte 0x{raw[e.start]:02x} at position {e.start}")
        except LookupError:
            failures.append(f"{encoding}: unknown encoding")

    detail = "; ".join(failures[:3])
    raise BadRequestError(
        "Invalid request body encoding: expected UTF-8 JSON"
        + (f" ({detail})" if detail else "")
    )


def _candidate_json_encodings(content_type: str) -> list[str]:
    declared = _declared_charset(content_type)
    candidates: list[str] = []
    if declared:
        candidates.append(declared)
    candidates.extend(["utf-8-sig", "utf-8", "gb18030"])

    out: list[str] = []
    seen: set[str] = set()
    for encoding in candidates:
        normalized = encoding.strip().strip('"').lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _declared_charset(content_type: str) -> str:
    match = _CHARSET_RE.search(content_type or "")
    return match.group(1).strip() if match else ""


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
    "activate_backend",
    "prepare_account_deploy",
    "wait_for_account_drain",
    "complete_account_deploy",
    "fail_account_deploy",
    "start_probe",
    "shutdown",
]