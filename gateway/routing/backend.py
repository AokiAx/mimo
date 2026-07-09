"""Backend abstraction — one upstream MiMo instance plus health/breaker state."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


HealthState = Literal["alive", "degraded", "dead", "unknown"]
LifecycleState = Literal["inactive", "active", "draining", "failed", "disabled"]


@dataclass
class Backend:
    """One upstream OpenAI-compatible endpoint backed by a MiMo Claw.

    Identity comes from ``backend_id``; ``base_url`` is the URL the gateway
    POSTs ``/v1/chat/completions`` to. ``account_id`` lets the auto-deploy
    pipeline tie a backend back to the Studio account that produced it.

    ``lifecycle`` controls traffic during account handoff: only ``active``
    backends can receive new requests. ``inactive`` keeps a configured backend
    available for manual/deploy activation without joining routing, and
    ``draining`` keeps existing requests alive while new traffic stops.
    """

    backend_id: str
    base_url: str
    models: list[str] = field(default_factory=list)
    account_id: str = ""
    api_key: str = ""                       # bearer token if upstream needs one

    # Health
    health: HealthState = "unknown"
    last_probe_at: float = 0.0
    last_success_at: float = 0.0
    last_failure_at: float = 0.0
    consecutive_failures: int = 0
    last_error: str = ""

    # Enable/disable (user-controlled, persisted in data/config.json)
    enabled: bool = True

    # Lifecycle / deploy handoff
    lifecycle: LifecycleState = "active"
    generation_id: str = ""
    ready_at: float = 0.0
    active_since: float = 0.0
    # Official MiMo claw expiry (epoch seconds from status.expireTime).
    # 0 = unknown → fall back to active_since + hard TTL.
    expire_at: float = 0.0
    draining_since: float = 0.0
    drain_deadline: float = 0.0

    # Breaker
    open_until: float = 0.0                  # epoch sec; 0 = closed
    metadata: dict[str, str] = field(default_factory=dict)

    # Routing / request accounting
    in_flight: int = 0                       # current concurrent requests
    max_in_flight: int = 50                  # reject new routing when saturated
    ewma_latency_ms: float = 0.0             # exponential moving average
    latency_alpha: float = 0.3               # EWMA smoothing factor
    total_requests: int = 0                  # lifetime success counter
    total_failures: int = 0                  # lifetime failure counter

    def serves(self, model: str) -> bool:
        return model in self.models

    # ───── breaker helpers ─────

    def is_open(self, now: float | None = None) -> bool:
        return (now or time.time()) < self.open_until

    def trip(self, cooldown_s: float, *, now: float | None = None) -> None:
        self.open_until = (now or time.time()) + cooldown_s

    def reset_breaker(self) -> None:
        self.open_until = 0.0

    # ───── lifecycle helpers ─────

    def mark_inactive(self) -> None:
        self.lifecycle = "inactive"
        self.draining_since = 0.0
        self.drain_deadline = 0.0
        self.ready_at = 0.0

    def mark_active(self, *, now: float | None = None) -> None:
        n = now or time.time()
        self.lifecycle = "active"
        self.ready_at = self.ready_at or n
        self.active_since = n
        self.draining_since = 0.0
        self.drain_deadline = 0.0
        self.reset_breaker()
        if self.health in ("unknown", "dead"):
            self.health = "alive"

    def mark_draining(self, *, drain_timeout_s: float, now: float | None = None) -> None:
        n = now or time.time()
        self.lifecycle = "draining"
        self.draining_since = n
        self.drain_deadline = n + drain_timeout_s

    def mark_failed_deploy(
        self,
        error: str,
        *,
        now: float | None = None,
    ) -> None:
        n = now or time.time()
        self.lifecycle = "failed"
        self.record_failure(error, now=n)

    # ───── health helpers ─────

    def record_success(self, *, now: float | None = None) -> None:
        n = now or time.time()
        self.health = "alive"
        self.last_probe_at = n
        self.last_success_at = n
        self.consecutive_failures = 0
        self.last_error = ""
        self.reset_breaker()

    def record_failure(
        self,
        error: str,
        *,
        now: float | None = None,
        cooldown_s: float = 30.0,
        threshold: int = 3,
    ) -> None:
        n = now or time.time()
        self.last_probe_at = n
        self.last_failure_at = n
        self.consecutive_failures += 1
        self.total_failures += 1
        self.last_error = error
        if self.consecutive_failures >= threshold:
            self.health = "dead"
            self.trip(cooldown_s, now=n)
        else:
            self.health = "degraded"

    def status_label(self, now: float | None = None) -> str:
        """A single human-facing status richer than online/offline. Priority
        order: user disable > draining > deploy-failed > breaker > inactive >
        active health."""
        n = now or time.time()
        if not self.enabled:
            return "disabled"
        if self.lifecycle == "draining":
            return "draining"
        if self.lifecycle == "failed":
            return "failed"
        if self.is_open(n):
            return "circuit_open"
        if self.lifecycle == "inactive":
            return "inactive"
        if self.lifecycle == "active":
            if self.health == "dead":
                return "dead"
            if self.health == "degraded":
                return "degraded"
            return "online"
        return self.health or "unknown"

    def is_selectable(self, now: float | None = None) -> bool:
        if not self.enabled:
            return False
        if self.lifecycle != "active":
            return False
        if self.is_open(now):
            return False
        if self.health == "dead":
            return False
        if self.max_in_flight and self.in_flight >= self.max_in_flight:
            return False
        return True

    # ───── request accounting helpers ─────

    def inc_in_flight(self) -> None:
        self.in_flight += 1

    def dec_in_flight(self) -> None:
        if self.in_flight > 0:
            self.in_flight -= 1

    def record_latency(self, latency_ms: float) -> None:
        """Update the EWMA latency. Call on each successful response."""
        if self.ewma_latency_ms <= 0:
            self.ewma_latency_ms = max(latency_ms, 0.0)
        else:
            a = self.latency_alpha
            self.ewma_latency_ms = a * latency_ms + (1 - a) * self.ewma_latency_ms
        self.total_requests += 1
