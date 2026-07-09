"""Gateway runtime — wires the handler pipeline into app.py.

Backends live in the ``backends`` section of ``data/config.json`` (managed by
``backend_store``).
Credentials come from ``data/secrets.json`` (managed by ``secrets_store``).

Claw fleet policy (free-tier):
  * Hard TTL ≈ 4h per Claw
  * Open a new account Claw every 2h (overlap so handoff is never empty)
  * Last 30 minutes of each Claw: lifecycle=draining (no new requests)
  * Multiple backends may be ``active`` at once; the router prefers the newest

Age-based drain/disable is applied in the probe loop via
``reconcile_backend_ages``. Deploy completion activates the new backend
*without* forcing peers offline.
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
_PROBE_TIMEOUT_S = _env_float("GATEWAY_PROBE_TIMEOUT_S", 20.0)
_PROBE_FAILURE_THRESHOLD = 3
_PROBE_COOLDOWN_S = 30.0
_DEFAULT_REQUEST_TIMEOUT_S = 600.0
_CHARSET_RE = re.compile(r"charset=([^;]+)", re.IGNORECASE)

# Free-tier Claw lifetime + fleet drain window (also used by claw_activity).
_CLAW_HARD_TTL_S = _env_float("MIMO_CLAW_TTL_S", 4 * 60 * 60.0)
_CLAW_DRAIN_BEFORE_EXPIRY_S = _env_float("MIMO_CLAW_DRAIN_BEFORE_S", 30 * 60.0)
# How long a draining backend may keep in-flight requests after operator drain.
_DRAIN_TIMEOUT_S = _env_float("GATEWAY_DRAIN_TIMEOUT_S", 30 * 60.0)
_DEPLOY_DRAIN_GRACE_S = _env_float("GATEWAY_DEPLOY_DRAIN_GRACE_S", 20.0)

_PROBE_PROMPT = "ping"

# Models that cannot answer a text /v1/chat/completions probe (text-to-speech,
# speech recognition). Probing them would falsely mark a healthy backend dead,
# so they are excluded from liveness probing.
_NON_CHAT_MODEL_RE = re.compile(r"(tts|asr)", re.IGNORECASE)

# Liveness probing is restricted to the v2.5 family (e.g. mimo-v2.5-pro). The
# older v2 models (v2-flash/omni/pro) are delisted upstream, so they are never
# probed and there is no fallback to them.
_PROBE_MODEL_RE = re.compile(r"v2\.5", re.IGNORECASE)


def _probeable_models(backend: Backend) -> list[str]:
    """v2.5-family chat models to liveness-probe.

    Excludes non-chat (tts/asr) models and keeps only the v2.5 family. The older
    v2 models are delisted upstream, so there is no fallback to them — a backend
    exposing no v2.5 chat model simply has nothing to probe.

    Prefer pro / ultraspeed over bare ``mimo-v2.5``: free-tier Claw lists the
    base id in /v1/models but rejects plain chat with HTTP 400 Param Incorrect,
    which used to flood error logs every probe tick before falling through to pro.
    """
    models = [
        m for m in backend.models
        if m and _PROBE_MODEL_RE.search(m) and not _NON_CHAT_MODEL_RE.search(m)
    ]

    def _rank(name: str) -> tuple[int, str]:
        n = name.lower()
        if "ultraspeed" in n:
            return (0, n)
        if n.endswith("-pro") or "-pro-" in n:
            return (1, n)
        # bare base model last (often multi-modal / free-tier rejects text-only)
        if n == "mimo-v2.5" or n.endswith("/mimo-v2.5"):
            return (3, n)
        return (2, n)

    return sorted(models, key=_rank)

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


# ────────────── backend loading / reconcile ──────────────


def _build_backend_from_entry(entry: dict[str, Any]) -> Backend:
    meta: dict[str, Any] = dict(entry.get("metadata") or {})
    name = entry.get("name") or ""
    if name and "name" not in meta:
        meta["name"] = name
    api_key = entry.get("api_key") or secrets.upstream_api_key
    models = entry.get("models") or []
    if not isinstance(models, list):
        models = []
    lifecycle = str(entry.get("lifecycle") or "active")
    if lifecycle in {"standby", "warming"}:
        lifecycle = "inactive"
    if lifecycle not in {"inactive", "active", "draining", "failed", "disabled"}:
        lifecycle = "active"
    backend = Backend(
        backend_id=entry["id"],
        base_url=entry["base_url"],
        models=[m for m in models if isinstance(m, str) and m],
        account_id=entry.get("account_id") or "",
        api_key=api_key,
        enabled=bool(entry.get("enabled", True)),
        metadata=meta,
        lifecycle=lifecycle,
        generation_id=entry.get("generation_id") or entry.get("id") or "",
        expire_at=float(entry.get("expire_at") or 0) or 0.0,
    )
    return backend


def _build_backends_from_store() -> list[Backend]:
    """Read the persisted backends config and produce Backend objects."""
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
    """Seed active timers; multi-active is allowed (2h open / 4h TTL fleet)."""
    assert _registry is not None
    now = time.time()
    for b in _registry.all():
        if b.lifecycle == "active" and not b.active_since:
            b.mark_active(now=now)
        elif b.lifecycle == "draining" and not b.drain_deadline:
            b.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S, now=now)
    reconcile_backend_ages(now=now)


def _make_metrics_recorder():
    try:
        from gateway.metrics import QueuedSQLiteMetricsRecorder
        return QueuedSQLiteMetricsRecorder()
    except Exception:
        return None


def reload_backends() -> int:
    """Re-read persisted backend config and reconcile the in-memory registry.

    Existing Backend objects keep EWMA, breaker, and in-flight state. Removed
    backends are marked draining instead of being dropped while requests are in
    flight. Multiple ``active`` backends are allowed (fleet overlap).
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
            if fresh.lifecycle == "active":
                fresh.mark_active(now=now)
            _registry.add(fresh)
            continue

        existing.base_url = fresh.base_url
        existing.models = fresh.models
        existing.account_id = fresh.account_id
        existing.api_key = fresh.api_key
        existing.enabled = fresh.enabled
        existing.metadata = fresh.metadata
        existing.generation_id = fresh.generation_id
        if getattr(fresh, "expire_at", 0):
            existing.expire_at = float(fresh.expire_at)
        if fresh.lifecycle != existing.lifecycle:
            if fresh.lifecycle == "inactive":
                existing.mark_inactive()
            elif fresh.lifecycle == "active":
                # Preserve active_since if already active (age-based drain)
                if existing.lifecycle != "active" or not existing.active_since:
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
    reconcile_backend_ages(now=now)
    _reap_drained(now=now)
    return len(_registry.all())


def reconcile_backend_ages(*, now: float | None = None) -> dict[str, list[str]]:
    """Age-based traffic control for free-tier Claws.

    Prefer official MiMo ``expireTime`` (stored as ``Backend.expire_at``) when
    known. Fall back to ``active_since + hard TTL`` only when expire_at is unset.

    - remain <= 30m  → lifecycle=draining (no new requests)
    - remain <= 0    → inactive + disabled (Claw effectively expired)
    """
    _ensure_initialized()
    assert _registry is not None
    n = now or time.time()
    drained: list[str] = []
    expired: list[str] = []
    ttl = max(60.0, float(_CLAW_HARD_TTL_S))
    drain_before = max(0.0, min(float(_CLAW_DRAIN_BEFORE_EXPIRY_S), ttl - 60.0))

    for b in _registry.all():
        if getattr(b, "expire_at", 0) and b.expire_at > 0:
            remain = float(b.expire_at) - n
            age = max(0.0, ttl - remain)
        elif b.active_since:
            age = n - b.active_since
            remain = ttl - age
        else:
            continue
        if remain <= 0 or age >= ttl:
            if b.lifecycle != "inactive" or b.enabled:
                b.mark_inactive()
                b.enabled = False
                _persist_backend_runtime_state(b)
                try:
                    from gateway.backend_store import update_backend
                    update_backend(b.backend_id, enabled=False, lifecycle="inactive")
                except Exception:
                    pass
                expired.append(b.backend_id)
                logger.info(
                    "Backend %s expired (age=%.0fs remain=%.0fs ttl=%.0fs expire_at=%.0f) → disabled",
                    b.backend_id, age, remain, ttl, float(getattr(b, "expire_at", 0) or 0),
                )
            continue
        if b.lifecycle == "active" and remain <= drain_before:
            remaining = max(30.0, remain)
            b.mark_draining(drain_timeout_s=remaining, now=n)
            _persist_backend_runtime_state(b)
            try:
                from gateway.backend_store import update_backend
                update_backend(b.backend_id, lifecycle="draining")
            except Exception:
                pass
            drained.append(b.backend_id)
            logger.info(
                "Backend %s entering pre-expiry drain (age=%.0fs, remain=%.0fs)",
                b.backend_id, age, remaining,
            )
    return {"drained": drained, "expired": expired}


def _retire_other_backends(active_backend: Backend) -> list[str]:
    """Optional exclusive activation: drain every other active backend.

    Used only for manual ``activate_backend`` (operator hard-switch). Normal
    fleet deploys do **not** call this so overlap stays up.
    """
    assert _registry is not None
    retired: list[str] = []
    now = time.time()
    for backend in _registry.all():
        if backend.backend_id == active_backend.backend_id:
            continue
        if backend.lifecycle == "active" or backend.in_flight > 0:
            backend.mark_draining(drain_timeout_s=_DRAIN_TIMEOUT_S, now=now)
        else:
            continue
        _persist_backend_runtime_state(backend)
        retired.append(backend.backend_id)
    return retired


# ────────────── dispatch (used by /v1/* routes) ──────────────


async def _dispatch_preparsed(adapter_name: str, request: Request, body: dict[str, Any]) -> Response:
    """Run a request through the pipeline using a caller-supplied JSON body."""
    global _total_requests
    _ensure_initialized()
    assert _handler is not None
    adapter = _adapters[adapter_name]

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
            stream_iter,
            media_type=content_type, headers=headers,
        )
    return Response(content=body_bytes, media_type=content_type, status_code=200, headers=headers)


async def dispatch(adapter_name: str, request: Request) -> Response:
    """Run a single request through the pipeline and return a FastAPI Response."""
    _ensure_initialized()
    adapter = _adapters[adapter_name]
    try:
        body = await _read_json_body(request)
    except BadRequestError as e:
        return _error_response(adapter, e)
    return await _dispatch_preparsed(adapter_name, request, body)


async def dispatch_with_body_override(adapter_name: str, request: Request, body: dict[str, Any]) -> Response:
    """Variant of dispatch() for routes that translate one protocol into another first."""
    _ensure_initialized()
    adapter = _adapters[adapter_name]
    if not isinstance(body, dict):
        return _error_response(adapter, BadRequestError("Request body must be a JSON object"))
    return await _dispatch_preparsed(adapter_name, request, body)


# ────────────── status helpers (used by panel) ──────────────


def get_router_status() -> dict[str, Any]:
    _ensure_initialized()
    assert _registry is not None
    backends = _registry.all()
    healthy = sum(1 for b in backends if b.is_selectable())
    latencies = [b.ewma_latency_ms for b in backends if b.ewma_latency_ms > 0]
    avg_lat = round(sum(latencies) / len(latencies), 1) if latencies else 0
    uptime = int(time.time() - _started_at)
    actives = [b for b in backends if b.lifecycle == "active" and b.enabled]
    # Prefer newest active for the "primary" status field
    primary = ""
    if actives:
        primary = max(actives, key=lambda b: b.active_since or 0).backend_id
    return {
        "uptime": uptime,
        "total_requests": _total_requests,
        "qps": round(_total_requests / max(uptime, 1), 2),
        "avg_latency_ms": avg_lat,
        "active_backend": primary,
        "active_backends": [b.backend_id for b in actives],
        "backends_total": len(backends),
        "backends_healthy": healthy,
        "backends_active": len(actives),
        "backends_inactive": sum(1 for b in backends if b.lifecycle == "inactive"),
        "backends_draining": sum(1 for b in backends if b.lifecycle == "draining"),
        "backends_failed": sum(1 for b in backends if b.lifecycle == "failed"),
        "backends_degraded": sum(1 for b in backends if b.lifecycle == "active" and b.health == "degraded"),
        "claw_ttl_s": int(_CLAW_HARD_TTL_S),
        "claw_drain_before_s": int(_CLAW_DRAIN_BEFORE_EXPIRY_S),
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
            "status": b.status_label(now),
            "avg_latency_ms": round(b.ewma_latency_ms, 1),
            "p95_latency_ms": round(b.ewma_latency_ms, 1),
            "circuit": "open" if b.is_open(now) else b.health,
            "total_requests": b.total_requests,
            "enabled": b.enabled,
            "account": b.account_id,
            "lifecycle": b.lifecycle,
            "generation_id": b.generation_id,
            "in_flight": b.in_flight,
            "active_for_s": (
                round(max(0.0, float(_CLAW_HARD_TTL_S) - (float(b.expire_at) - now)), 1)
                if getattr(b, "expire_at", 0) and b.expire_at > 0
                else (round(now - b.active_since, 1) if b.active_since else 0)
            ),
            "expire_at": float(getattr(b, "expire_at", 0) or 0),
            "remain_s": (
                round(float(b.expire_at) - now, 1)
                if getattr(b, "expire_at", 0) and b.expire_at > 0
                else (
                    round(float(_CLAW_HARD_TTL_S) - (now - b.active_since), 1)
                    if b.active_since else 0
                )
            ),
            "draining_for_s": round(now - b.draining_since, 1) if b.draining_since else 0,
            "drain_deadline_s": round(b.drain_deadline - now, 1) if b.drain_deadline else 0,
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
        retired = _activate_backend(b, reason="toggle")
    else:
        retired = []
        b.lifecycle = "disabled"
        _persist_backend_runtime_state(b)

    from gateway.backend_store import update_backend
    update_backend(backend_id, enabled=new_enabled, lifecycle=b.lifecycle)

    label = "启用" if new_enabled else "禁用"
    return {"success": True, "message": f"Backend {backend_id!r} {label}", "retired": retired}


def activate_backend(backend_id: str) -> dict[str, Any]:
    """Operator hard-switch: make one backend active and drain peers."""
    _ensure_initialized()
    assert _registry is not None
    b = _registry.get(backend_id)
    if b is None:
        return {"success": False, "error": f"Backend {backend_id!r} not found"}
    retired = _activate_backend(b, reason="manual", exclusive=True)
    return {"success": True, "backend": backend_id, "retired": retired}


def prepare_account_deploy(account_id: str, *, api_port: int | None = None) -> dict[str, Any]:
    """Move traffic away from backends that are about to have their Claw destroyed.

    Auto-deploy recreates a Claw in-place behind the same jump-server port. If
    the gateway keeps routing new requests to that active backend while Step 0
    destroys the old Claw/tunnel, clients see avoidable 5xxs until the next
    manual reload or health-probe cycle. This helper is intentionally sync so
    the deploy thread can call it before touching the old Claw: active matching
    backends are drained even if this temporarily leaves no route.
    """
    _ensure_initialized()
    assert _registry is not None
    targets = _matching_account_backends(account_id, api_port=api_port)
    drained: list[str] = []
    blocked: list[str] = []
    for backend in targets:
        if backend.lifecycle != "active":
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


def complete_account_deploy(
    account_id: str,
    *,
    api_port: int | None = None,
    expire_at: float | None = None,
) -> dict[str, Any]:
    """Reload and activate backends for a freshly deployed Claw.

    Does **not** force-drain peer actives — fleet keeps overlap (2h open / 4h TTL).
    Age-based drain still retires Claws in their last 30 minutes.

    ``expire_at`` is the official MiMo status.expireTime converted to epoch
    seconds. When provided it becomes the source of truth for remain/drain/open.
    """
    reload_backends()
    assert _registry is not None
    now = time.time()
    targets = _matching_account_backends(account_id, api_port=api_port)
    activated: list[str] = []
    for backend in targets:
        backend.enabled = True
        backend.reset_breaker()
        backend.health = "alive"
        backend.consecutive_failures = 0
        backend.last_error = ""
        backend.mark_active(now=now)
        if expire_at and float(expire_at) > 0:
            backend.expire_at = float(expire_at)
        activated.append(backend.backend_id)
        _persist_backend_runtime_state(backend)
        try:
            from gateway.backend_store import update_backend
            fields: dict[str, Any] = {"enabled": True, "lifecycle": "active"}
            if backend.expire_at > 0:
                fields["expire_at"] = backend.expire_at
            update_backend(backend.backend_id, **fields)
        except Exception:
            pass
    reconcile_backend_ages(now=now)
    return {
        "success": True,
        "account": account_id,
        "matched": [b.backend_id for b in targets],
        "activated": activated,
        "retired": [],
    }


def fail_account_deploy(account_id: str, *, api_port: int | None = None, error: str = "deploy failed") -> dict[str, Any]:
    """Keep failed redeploy targets out of routing until an operator fixes them."""
    _ensure_initialized()
    assert _registry is not None
    targets = _matching_account_backends(account_id, api_port=api_port)
    failed: list[str] = []
    for backend in targets:
        backend.mark_failed_deploy(error)
        _persist_backend_runtime_state(backend)
        failed.append(backend.backend_id)
    return {"success": True, "account": account_id, "matched": [b.backend_id for b in targets], "failed": failed}


def abort_account_deploy(
    account_id: str,
    *,
    api_port: int | None = None,
    restore: bool,
    error: str = "deploy aborted",
) -> dict[str, Any]:
    """Resolve a deploy that stopped after prepare but before completion.

    ``restore=True`` is used only while the old Claw/tunnel is known untouched;
    otherwise the matched backend is marked failed so the gateway does not route
    to an uncertain upstream.
    """
    _ensure_initialized()
    assert _registry is not None
    targets = _matching_account_backends(account_id, api_port=api_port)
    restored: list[str] = []
    failed: list[str] = []
    for backend in targets:
        if restore and backend.lifecycle in {"draining", "inactive"}:
            backend.enabled = True
            backend.reset_breaker()
            backend.health = "alive"
            backend.consecutive_failures = 0
            backend.last_error = ""
            _activate_backend(backend, reason="deploy_abort")
            restored.append(backend.backend_id)
        else:
            backend.mark_failed_deploy(error)
            _persist_backend_runtime_state(backend)
            failed.append(backend.backend_id)
    return {
        "success": True,
        "account": account_id,
        "matched": [b.backend_id for b in targets],
        "restored": restored,
        "failed": failed,
    }


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


# ────────────── probe loop ──────────────


async def _probe_loop() -> None:
    """Probe the current active backend with a non-stream chat completion."""
    _ensure_initialized()
    assert _registry is not None

    async def _probe_one(backend: Backend) -> None:
        if not backend.enabled or backend.lifecycle != "active":
            return
        started = time.monotonic()
        try:
            # Probe the v2.5 chat model(s); stop at the first that answers. The
            # backend may list several v2.5 variants, so try each until one is OK.
            ok, reason = False, "no probeable models"
            for _m in _probeable_models(backend):
                ok, reason = await _run_one_probe_check(backend, "probe", _probe_body_for_model(_m))
                if ok:
                    break
        except Exception as e:  # noqa: BLE001
            ok = False
            reason = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - started) * 1000
        if ok:
            backend.record_success()
            backend.record_latency(latency)
            return
        backend.record_failure(
            reason,
            cooldown_s=_PROBE_COOLDOWN_S,
            threshold=_PROBE_FAILURE_THRESHOLD,
        )

    while True:
        try:
            reconcile_backend_ages()
            actives = [
                b for b in _registry.all()
                if b.enabled and b.lifecycle == "active"
            ]
            for active in actives:
                await _probe_one(active)
            _reap_drained()
        except Exception:  # noqa: BLE001
            logger.exception("Gateway probe loop failed")
        await asyncio.sleep(_PROBE_INTERVAL_S)


def _activate_backend(backend: Backend, *, reason: str, exclusive: bool = False) -> list[str]:
    """Activate ``backend``. If ``exclusive``, drain all other actives (manual switch)."""
    now = time.time()
    backend.enabled = True
    backend.mark_active(now=now)
    shared_models = set(backend.models)
    _persist_backend_runtime_state(backend)
    retired = _retire_other_backends(backend) if exclusive else []
    logger.info(
        "Activated backend %s by %s exclusive=%s models=%s retired=%s",
        backend.backend_id,
        reason,
        exclusive,
        sorted(shared_models),
        retired,
    )
    return retired


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
            b.mark_inactive()
            _persist_backend_runtime_state(b)
        else:
            _registry.remove(b.backend_id)


def _persisted_backend_ids() -> set[str]:
    try:
        return {str(entry.get("id")) for entry in _list_persisted() if entry.get("id")}
    except Exception:
        return set()


async def _run_one_probe_check(backend: Backend, name: str, body: dict[str, Any]) -> tuple[bool, str]:
    assert _transport is not None
    proxy_url = None
    url = backend.base_url.rstrip("/") + "/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    if backend.api_key:
        headers["Authorization"] = f"Bearer {backend.api_key}"
    try:
        status, raw = await _transport.post_json(
            url, body, headers=headers, timeout_s=_PROBE_TIMEOUT_S,
            proxy=proxy_url,
        )
        if status >= 400:
            return False, f"http {status}: {raw[:200]!r}"
        return True, "ok"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def _probe_model(backend: Backend) -> str:
    if not backend.models:
        raise ValueError("backend has no configured models for probe checks")
    probeable = _probeable_models(backend)
    return probeable[0] if probeable else backend.models[0]


def _probe_non_stream_body(backend: Backend) -> dict[str, Any]:
    return _probe_body_for_model(_probe_model(backend))


def _probe_body_for_model(model: str) -> dict[str, Any]:
    return {
        "model": model,
        "messages": [{"role": "user", "content": _PROBE_PROMPT}],
        "max_tokens": 256,
        "stream": False,
    }


def _persist_backend_runtime_state(backend: Backend) -> None:
    try:
        from gateway.backend_store import update_backend
        fields: dict[str, Any] = {
            "enabled": backend.enabled,
            "lifecycle": backend.lifecycle,
            "generation_id": backend.generation_id,
        }
        if getattr(backend, "expire_at", 0):
            fields["expire_at"] = float(backend.expire_at)
        update_backend(backend.backend_id, **fields)
    except Exception:
        logger.exception("Failed to persist backend state for %s", backend.backend_id)


def start_probe() -> None:
    """Idempotent. Spawns the background liveness probe."""
    global _probe_task
    _ensure_initialized()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if _probe_task is None or _probe_task.done():
        _probe_task = loop.create_task(_probe_loop())


async def shutdown() -> None:
    """Close runtime-owned background resources."""
    global _probe_task
    tasks = (_probe_task,)
    _probe_task = None
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
    "dispatch_with_body_override",
    "reload_backends",
    "get_router_status",
    "get_all_backends",
    "toggle_backend",
    "activate_backend",
    "prepare_account_deploy",
    "wait_for_account_drain",
    "complete_account_deploy",
    "fail_account_deploy",
    "abort_account_deploy",
    "start_probe",
    "shutdown",
]
