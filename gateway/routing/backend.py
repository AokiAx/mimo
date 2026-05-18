"""Backend abstraction — one upstream MiMo instance plus health/breaker state."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


HealthState = Literal["alive", "degraded", "dead", "unknown"]
LifecycleState = Literal["standby", "warming", "active", "draining", "failed", "disabled"]


@dataclass
class Backend:
    """One upstream OpenAI-compatible endpoint backed by a MiMo Claw.

    Identity comes from ``backend_id``; ``base_url`` is the URL the gateway
    POSTs ``/v1/chat/completions`` to. ``account_id`` lets the auto-deploy
    pipeline tie a backend back to the Studio account that produced it.

    ``lifecycle`` controls traffic during account rotation:
    ``standby`` backends are eligible for the next rotation, ``warming``
    backends are being readiness-checked, ``active`` backends can receive new
    requests, and ``draining`` backends keep existing requests alive while the
    router stops assigning fresh traffic to them.
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

    # Enable/disable (user-controlled, persisted in backends.json)
    enabled: bool = True

    # Lifecycle / rotation
    lifecycle: LifecycleState = "active"
    generation_id: str = ""
    ready_at: float = 0.0
    active_since: float = 0.0
    draining_since: float = 0.0
    drain_deadline: float = 0.0
    readiness_successes: int = 0
    readiness_failures: int = 0
    rotation_failures: int = 0
    disabled_until: float = 0.0

    # Detection zone — fast-probe quarantine for flaky backends
    in_detection: bool = False
    detection_entered_at: float = 0.0

    # Probe-path failure tracking (separate from request-path failures)
    probe_consecutive_failures: int = 0

    # Breaker
    open_until: float = 0.0                  # epoch sec; 0 = closed
    weight: int = 1                          # static weight for selection
    metadata: dict[str, str] = field(default_factory=dict)

    # Routing / load balancing
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

    def is_temporarily_disabled(self, now: float | None = None) -> bool:
        return (now or time.time()) < self.disabled_until

    def mark_standby(self) -> None:
        self.lifecycle = "standby"
        self.draining_since = 0.0
        self.drain_deadline = 0.0
        self.readiness_successes = 0
        self.readiness_failures = 0

    def mark_warming(self, *, now: float | None = None) -> None:
        self.lifecycle = "warming"
        self.draining_since = 0.0
        self.drain_deadline = 0.0
        self.ready_at = 0.0
        self.readiness_successes = 0
        self.readiness_failures = 0
        self.last_probe_at = now or time.time()

    def mark_active(self, *, now: float | None = None) -> None:
        n = now or time.time()
        self.lifecycle = "active"
        self.ready_at = self.ready_at or n
        self.active_since = n
        self.draining_since = 0.0
        self.drain_deadline = 0.0
        self.disabled_until = 0.0
        self.readiness_successes = 0
        self.readiness_failures = 0
        self.reset_breaker()
        if self.health in ("unknown", "dead"):
            self.health = "alive"

    def mark_draining(self, *, drain_timeout_s: float, now: float | None = None) -> None:
        n = now or time.time()
        self.lifecycle = "draining"
        self.draining_since = n
        self.drain_deadline = n + drain_timeout_s

    def mark_failed_rotation(
        self,
        error: str,
        *,
        now: float | None = None,
    ) -> None:
        n = now or time.time()
        self.lifecycle = "failed"
        self.readiness_failures += 1
        self.rotation_failures += 1
        self.record_failure(error, now=n)
        # No longer disable — just mark failed. The probe loop
        # handles detection-zone quarantine separately.

    def mark_detection(self, *, now: float | None = None) -> None:
        """Enter detection zone: fast probing (10s) until one success."""
        self.in_detection = True
        self.detection_entered_at = now or time.time()

    def exit_detection(self) -> None:
        """Leave detection zone after a successful probe."""
        self.in_detection = False
        self.detection_entered_at = 0.0
        self.consecutive_failures = 0
        self.probe_consecutive_failures = 0

    # ───── health helpers ─────

    def record_success(self, *, now: float | None = None) -> None:
        n = now or time.time()
        self.health = "alive"
        self.last_probe_at = n
        self.last_success_at = n
        self.consecutive_failures = 0
        self.probe_consecutive_failures = 0
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

    def record_probe_failure(
        self,
        error: str,
        *,
        now: float | None = None,
        cooldown_s: float = 30.0,
        threshold: int = 3,
    ) -> None:
        """Record a failure from the liveness probe path only."""
        self.probe_consecutive_failures += 1
        self.record_failure(error, now=now, cooldown_s=cooldown_s, threshold=threshold)

    def is_selectable(self, now: float | None = None) -> bool:
        if not self.enabled:
            return False
        if self.lifecycle not in ("active", "warming"):
            return False
        if self.lifecycle == "warming" and self.readiness_successes < 1:
            return False
        if self.in_detection:
            return False
        if self.is_open(now):
            return False
        if self.health == "dead":
            return False
        if self.max_in_flight and self.in_flight >= self.max_in_flight:
            return False
        return True

    # ───── routing/load balancing helpers ─────

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

    def _failure_rate(self) -> float:
        """Recent failure rate as a 0.0-1.0 fraction. 0.0 if insufficient data."""
        total = self.total_requests + self.total_failures
        if total < 5:
            return 0.0
        return self.total_failures / total

    def routing_score(self) -> float:
        """Lower is better. Combines latency, current load, weight, and reliability.

        Unobserved latency is deliberately conservative so a newly promoted
        backend does not steal all traffic before readiness probes seed EWMA.
        """
        base = self.ewma_latency_ms if self.ewma_latency_ms > 0 else 100.0
        penalty = 1.0 + self._failure_rate() * 4.0
        return base * (1 + self.in_flight) / max(self.weight, 1) * penalty