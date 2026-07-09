"""Unit tests for gateway.routing."""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from gateway.core import BackendUnavailableError
from gateway.routing import (
    Backend,
    BackendRegistry,
    InMemoryDecisionLog,
    JSONLDecisionLog,
    Router,
    TeeDecisionLog,
)


# ───────── helpers ─────────


def _backend(**overrides) -> Backend:
    base = {
        "backend_id": "b1",
        "base_url": "http://upstream.example",
        "models": ["MiMo-VL-7B-RL-2508"],
        "account_id": "acct1",
        "api_key": "sk-test",
    }
    # Accept legacy {model: "X"} from test callers and fold into models list.
    if "model" in overrides and "models" not in overrides:
        overrides["models"] = [overrides.pop("model")]
    base.update(overrides)
    return Backend(**base)


# ───────── Backend health/breaker ─────────


def test_backend_record_success_resets_state():
    b = _backend()
    b.consecutive_failures = 2
    b.health = "degraded"
    b.open_until = 9_999_999.0
    b.record_success()
    assert b.health == "alive"
    assert b.consecutive_failures == 0
    assert b.open_until == 0.0
    assert b.last_success_at > 0


def test_backend_record_failure_below_threshold_marks_degraded():
    b = _backend()
    b.record_failure("boom", threshold=3)
    assert b.health == "degraded"
    assert b.is_open() is False
    assert b.consecutive_failures == 1
    assert b.is_selectable() is True


def test_backend_record_failure_at_threshold_trips_breaker():
    b = _backend()
    b.record_failure("boom", threshold=3, cooldown_s=30)
    b.record_failure("boom", threshold=3, cooldown_s=30)
    b.record_failure("boom", threshold=3, cooldown_s=30)
    assert b.health == "dead"
    assert b.is_open()
    assert b.is_selectable() is False


# ───────── BackendRegistry ─────────


def test_registry_basic_crud():
    r = BackendRegistry([_backend(backend_id="a"), _backend(backend_id="b")])
    assert len(r) == 2
    assert "a" in r and "b" in r
    assert r.get("a").backend_id == "a"
    assert r.remove("a") is True
    assert "a" not in r
    r.add(_backend(backend_id="c"))
    assert "c" in r


def test_registry_replace_all():
    r = BackendRegistry([_backend(backend_id="x")])
    r.replace_all([_backend(backend_id="y"), _backend(backend_id="z")])
    ids = {b.backend_id for b in r.all()}
    assert ids == {"y", "z"}


def test_registry_edit_yields_locked_backend():
    r = BackendRegistry([_backend(backend_id="a")])
    with r.edit("a") as b:
        assert b is not None
        b.health = "alive"
    assert r.get("a").health == "alive"


# ───────── Router.choose ─────────


def test_router_choose_picks_only_selectable():
    alive = _backend(backend_id="alive")
    alive.record_success()
    dead = _backend(backend_id="dead")
    for _ in range(3):
        dead.record_failure("x", threshold=3)
    r = Router(BackendRegistry([alive, dead]))
    chosen, decision = r.choose(request_id="r1", model=alive.models[0])
    assert chosen.backend_id == "alive"
    assert decision.chosen_backend == "alive"
    assert "dead" in decision.excluded


def test_router_choose_filters_by_model():
    a = _backend(backend_id="a", model="model-A")
    b = _backend(backend_id="b", model="model-B")
    a.record_success()
    b.record_success()
    r = Router(BackendRegistry([a, b]))
    chosen, decision = r.choose(request_id="r1", model="model-B")
    assert chosen.backend_id == "b"
    assert "a" in decision.excluded


def test_router_choose_excludes_caller_specified():
    a = _backend(backend_id="a")
    b = _backend(backend_id="b")
    a.record_success()
    b.record_success()
    r = Router(BackendRegistry([a, b]))
    chosen, decision = r.choose(request_id="r1", model=a.models[0], exclude={"a"})
    assert chosen.backend_id == "b"
    assert decision.excluded["a"] == "excluded by caller"


def test_router_choose_raises_when_none_available():
    dead = _backend(backend_id="dead")
    for _ in range(3):
        dead.record_failure("x", threshold=3)
    r = Router(BackendRegistry([dead]))
    with pytest.raises(BackendUnavailableError) as exc:
        r.choose(request_id="r1", model=dead.models[0])
    assert "decision" in exc.value.details


def test_router_choose_prefers_newest_active():
    """Among multi-active candidates, prefer highest active_since (freshest Claw)."""
    import time
    now = time.time()
    a = _backend(backend_id="a")
    a.record_success()
    a.active_since = now - 3600
    b = _backend(backend_id="b")
    b.record_success()
    b.active_since = now  # newer
    r = Router(BackendRegistry([a, b]))
    chosen, decision = r.choose(request_id="r1", model=a.models[0])
    assert chosen.backend_id == "b"
    assert decision.reason == "newest_active"


def test_router_decision_records_candidates_considered():
    a = _backend(backend_id="a")
    b = _backend(backend_id="b")
    a.record_success()
    b.record_success()
    a.active_since = 100.0
    b.active_since = 50.0
    r = Router(BackendRegistry([a, b]))
    _, decision = r.choose(request_id="rid-xyz", model=a.models[0])
    assert set(decision.candidates_considered) == {"a", "b"}
    assert decision.request_id == "rid-xyz"
    assert decision.reason in ("active", "newest_active")


# ───────── Backend request counters ─────────


def test_backend_record_latency_seeds_then_smooths():
    b = _backend()
    b.record_latency(100.0)
    assert b.ewma_latency_ms == 100.0
    assert b.total_requests == 1
    # Next sample blends at alpha=0.3: 0.3*200 + 0.7*100 = 130
    b.record_latency(200.0)
    assert b.ewma_latency_ms == pytest.approx(130.0, rel=1e-6)
    assert b.total_requests == 2


def test_backend_in_flight_counters_dont_go_negative():
    b = _backend()
    b.inc_in_flight()
    b.inc_in_flight()
    assert b.in_flight == 2
    b.dec_in_flight()
    b.dec_in_flight()
    b.dec_in_flight()  # extra dec is a no-op
    assert b.in_flight == 0


# ───────── Router single-active fallback ─────────


def test_router_prefers_newest_among_multi_active_candidates():
    a = _backend(backend_id="a")
    b = _backend(backend_id="b")
    a.record_success()
    b.record_success()
    a.active_since = 10.0
    b.active_since = 20.0  # newer
    a.ewma_latency_ms = 500.0
    b.ewma_latency_ms = 50.0
    r = Router(BackendRegistry([a, b]))

    chosen, decision = r.choose(request_id="r1", model=a.models[0])

    assert chosen.backend_id == "b"
    assert decision.reason == "newest_active"
    assert decision.chosen_score == 0.0


# ───────── decision log ─────────


def _decision(**kw):
    from gateway.routing.router import RoutingDecision
    base = {
        "request_id": "r1", "model_requested": "m",
        "chosen_backend": "b1", "reason": "lru",
    }
    base.update(kw)
    return RoutingDecision(**base)


def test_inmemory_decision_log_buffer_eviction():
    log = InMemoryDecisionLog(capacity=3)
    for i in range(5):
        log.write(_decision(request_id=f"r{i}"))
    assert len(log) == 3
    ids = [d.request_id for d in log.recent(10)]
    assert ids == ["r2", "r3", "r4"]


def test_inmemory_decision_log_filter_by_request():
    log = InMemoryDecisionLog()
    log.write(_decision(request_id="r1", chosen_backend="a"))
    log.write(_decision(request_id="r1", chosen_backend="b"))
    log.write(_decision(request_id="r2"))
    matches = log.filter_by_request("r1")
    assert len(matches) == 2
    assert {d.chosen_backend for d in matches} == {"a", "b"}


def test_jsonl_decision_log_appends_lines():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "dec.jsonl")
        log = JSONLDecisionLog(path)
        log.write(_decision(request_id="r1"))
        log.write(_decision(request_id="r2", reason="fallback"))
        with open(path, encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2
        assert lines[0]["request_id"] == "r1"
        assert lines[1]["reason"] == "fallback"


def test_tee_decision_log_fans_out():
    a = InMemoryDecisionLog()
    b = InMemoryDecisionLog()
    tee = TeeDecisionLog([a, b])
    tee.write(_decision(request_id="r1"))
    assert len(a) == 1 and len(b) == 1


def test_tee_decision_log_swallows_per_writer_errors():
    class Boom:
        def write(self, decision):
            raise RuntimeError("nope")

    a = InMemoryDecisionLog()
    tee = TeeDecisionLog([Boom(), a])
    tee.write(_decision(request_id="r1"))   # must not raise
    assert len(a) == 1
