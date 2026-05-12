"""Backend abstraction — one upstream MiMo instance plus health/breaker state."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


HealthState = Literal["alive", "degraded", "dead", "unknown"]


@dataclass
class Backend:
    """One upstream OpenAI-compatible endpoint backed by a MiMo Claw.

    Identity comes from ``backend_id``; ``base_url`` is the URL the gateway
    POSTs ``/v1/chat/completions`` to. ``account_id`` lets the auto-deploy
    pipeline (untouched by this refactor) tie a backend back to the Studio
    account that produced it.

    ``models`` lists every native model name this Claw serves (a single MiMo
    Claw usually exposes ~9 native models). The router matches a request if
    ``req.model in b.models`` — no notion of "primary".

    Health/breaker fields are intentionally simple — no rolling windows,
    no failure-rate math. We trust the chat probe to be the source of
    truth and use consecutive_failures as the only suppression signal.

    Routing fields (``in_flight``, ``ewma_latency_ms``) feed the router's
    score = latency * (1 + in_flight) / weight; mutations are non-atomic
    but safe under asyncio (cooperative single-thread).
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

    # Breaker
    open_until: float = 0.0                  # epoch sec; 0 = closed
    weight: int = 1                          # static weight for selection
    metadata: dict[str, str] = field(default_factory=dict)

    # Routing / load balancing
    in_flight: int = 0                       # current concurrent requests
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

    def is_selectable(self, now: float | None = None) -> bool:
        if not self.enabled:
            return False
        if self.is_open(now):
            return False
        if self.health == "dead":
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

    def routing_score(self) -> float:
        """Lower is better. Combines latency, current load, and weight.

        Unobserved latency is treated as 1ms so fresh backends compete
        fairly with established ones (and any in-flight load still
        differentiates them once requests start landing).
        """
        base = self.ewma_latency_ms if self.ewma_latency_ms > 0 else 1.0
        return base * (1 + self.in_flight) / max(self.weight, 1)
