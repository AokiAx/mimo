"""
Backend selection + decision logging.

The Router's only job is: given a RequestContext, pick the active backend
that can serve the requested model. Retry still lives in the handler, which can
call ``choose`` again after excluding a backend. The gateway now enforces one
active backend at lifecycle boundaries; scoring remains only as a compatibility
fallback for legacy multi-active configs.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

from gateway.core import BackendUnavailableError

from .backend import Backend
from .registry import BackendRegistry


@dataclass
class RoutingDecision:
    """One routing decision, suitable for the audit log."""

    request_id: str
    model_requested: str
    chosen_backend: str | None
    reason: str
    candidates_considered: list[str] = field(default_factory=list)
    excluded: dict[str, str] = field(default_factory=dict)   # backend_id → reason
    timestamp: float = field(default_factory=time.time)
    chosen_score: float = 0.0
    chosen_in_flight: int = 0
    chosen_latency_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Router:
    """Stateless given a registry — every call is a fresh selection."""

    def __init__(self, registry: BackendRegistry):
        self._registry = registry

    def choose(
        self,
        *,
        request_id: str,
        model: str,
        exclude: set[str] | None = None,
    ) -> tuple[Backend, RoutingDecision]:
        """Pick a backend serving ``model``. Raises if none is available.

        ``exclude`` lets the caller skip backends already tried in this
        request (e.g. after an upstream 5xx). Decisions are returned
        alongside the backend so the handler can log/persist them.
        """
        exclude = exclude or set()
        now = time.time()

        considered: list[str] = []
        excluded: dict[str, str] = {}
        candidates: list[Backend] = []

        for b in self._registry.all():
            considered.append(b.backend_id)
            if b.backend_id in exclude:
                excluded[b.backend_id] = "excluded by caller"
                continue
            if not b.serves(model):
                excluded[b.backend_id] = f"serves {b.models!r}, not {model!r}"
                continue
            if not b.is_selectable(now):
                if not b.enabled:
                    excluded[b.backend_id] = "disabled"
                elif getattr(b, "lifecycle", "active") != "active":
                    excluded[b.backend_id] = f"lifecycle={b.lifecycle}"
                elif b.is_temporarily_disabled(now):
                    excluded[b.backend_id] = (
                        f"temporarily disabled for {b.disabled_until - now:.1f}s"
                    )
                elif b.is_open(now):
                    excluded[b.backend_id] = (
                        f"breaker open until {b.open_until - now:.1f}s"
                    )
                elif b.max_in_flight and b.in_flight >= b.max_in_flight:
                    excluded[b.backend_id] = (
                        f"saturated: in_flight={b.in_flight} >= max={b.max_in_flight}"
                    )
                else:
                    excluded[b.backend_id] = f"health={b.health}"
                continue
            candidates.append(b)

        if not candidates:
            decision = RoutingDecision(
                request_id=request_id,
                model_requested=model,
                chosen_backend=None,
                reason="no selectable backend",
                candidates_considered=considered,
                excluded=excluded,
            )
            raise BackendUnavailableError(
                f"No backend available for model {model!r}",
                details={"decision": decision.to_dict()},
            )

        # Compatibility fallback for legacy multi-active configs: lower score
        # wins, then older/less-used backends. Normal operation has one active
        # backend, so this path is effectively deterministic.
        chosen = min(
            candidates,
            key=lambda b: (
                b.routing_score(),
                b.total_requests / max(b.weight, 1),
                b.last_failure_at,
                b.backend_id,
            ),
        )

        decision = RoutingDecision(
            request_id=request_id,
            model_requested=model,
            chosen_backend=chosen.backend_id,
            reason="score",
            candidates_considered=considered,
            excluded=excluded,
            chosen_score=round(chosen.routing_score(), 3),
            chosen_in_flight=chosen.in_flight,
            chosen_latency_ms=round(chosen.ewma_latency_ms, 1),
        )
        return chosen, decision
