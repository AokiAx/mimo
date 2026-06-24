from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

import pytest

from gateway.routing import Backend, BackendRegistry, Router
from gateway.core import BackendUnavailableError
import gateway.config_store as config_store
import gateway.backend_store as backend_store
import gateway.runtime as runtime


@pytest.fixture(autouse=True)
def reset_runtime(monkeypatch, tmp_path):
    path = tmp_path / "config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"backends": {"backends": []}}), encoding="utf-8")
    monkeypatch.setattr(config_store, "CONFIG_PATH", path)
    for name in (
        "_registry", "_router", "_transport", "_handler",
        "_decision_log", "_probe_task", "_rotation_task",
    ):
        monkeypatch.setattr(runtime, name, None)
    monkeypatch.setattr(runtime, "_adapters", {})
    yield


def _backend(backend_id: str, *, lifecycle: str = "active") -> Backend:
    b = Backend(
        backend_id=backend_id,
        base_url=f"http://{backend_id}.example",
        models=["mimo-v2.5-pro", "mimo-v2-flash"],
        account_id=backend_id,
        lifecycle=lifecycle,
    )
    b.record_success()
    return b


def test_router_excludes_warming_and_draining_backends():
    active = _backend("active")
    warming = _backend("warming", lifecycle="warming")
    draining = _backend("draining", lifecycle="draining")
    router = Router(BackendRegistry([warming, draining, active]))

    chosen, decision = router.choose(request_id="r1", model="mimo-v2.5-pro")

    assert chosen.backend_id == "active"
    assert decision.excluded["warming"] == "lifecycle=warming"
    assert decision.excluded["draining"] == "lifecycle=draining"


def test_router_raises_when_only_warming_backend_exists():
    router = Router(BackendRegistry([_backend("warming", lifecycle="warming")]))

    with pytest.raises(BackendUnavailableError):
        router.choose(request_id="r1", model="mimo-v2.5-pro")


def test_router_excludes_warming_even_after_readiness_success():
    active = _backend("active")
    warming = _backend("warming", lifecycle="warming")
    warming.readiness_successes = 1
    router = Router(BackendRegistry([warming, active]))

    chosen, decision = router.choose(request_id="r1", model="mimo-v2.5-pro")

    assert chosen.backend_id == "active"
    assert decision.excluded["warming"] == "lifecycle=warming"


def test_activate_backend_drains_existing_active_peers(monkeypatch):
    old = _backend("old")
    old.active_since = 100.0
    new = _backend("new", lifecycle="warming")
    reg = BackendRegistry([old, new])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.activate_backend("new")

    assert result["success"] is True
    assert new.lifecycle == "active"
    assert old.lifecycle == "draining"
    assert old.drain_deadline > 0.0


def test_reap_drained_keeps_in_flight_until_deadline(monkeypatch, caplog):
    b = _backend("old", lifecycle="draining")
    b.in_flight = 1
    b.draining_since = 100.0
    b.drain_deadline = 200.0
    reg = BackendRegistry([b])
    monkeypatch.setattr(runtime, "_registry", reg)

    runtime._reap_drained(now=150.0)
    assert reg.get("old") is b

    runtime._reap_drained(now=250.0)
    assert reg.get("old") is None
    assert "Drain deadline reached for backend old" in caplog.text


class FakeTransport:
    def __init__(self):
        self.json_bodies = []
        self.stream_bodies = []
        self.json_timeouts = []
        self.stream_timeouts = []

    async def post_json(self, url, body, *, headers=None, timeout_s=60.0, proxy=None):
        self.json_bodies.append(body)
        self.json_timeouts.append(timeout_s)
        if body.get("tools"):
            return 200, b'{"choices":[{"message":{"tool_calls":[{"id":"call_1"}]}}]}'
        return 200, b'{"choices":[{"message":{"content":"ok"}}]}'

    async def post_stream(self, url, body, *, headers=None, timeout_s=600.0, proxy=None):
        self.stream_bodies.append(body)
        self.stream_timeouts.append(timeout_s)

        async def chunks() -> AsyncIterator[bytes]:
            yield b'data: {"choices":[{"delta":{"content":"o"}}]}\n\n'
            yield b'data: [DONE]\n\n'

        return 200, chunks()


def test_probe_one_uses_chat_completion_not_models(monkeypatch):
    fake = FakeTransport()
    monkeypatch.setattr(runtime, "_transport", fake)
    backend = _backend("candidate", lifecycle="active")

    ok, reason = asyncio.run(
        runtime._run_one_readiness_check(
            backend,
            "probe",
            runtime._readiness_non_stream_body(backend),
        )
    )

    assert ok is True
    assert reason == "ok"
    assert len(fake.json_bodies) == 1
    assert fake.json_bodies[0]["messages"][0]["content"] == runtime._READINESS_PROMPT
    assert fake.json_bodies[0]["model"] == "mimo-v2.5-pro"
    assert fake.json_bodies[0]["stream"] is False
    assert fake.json_timeouts == [runtime._PROBE_TIMEOUT_S]


def test_readiness_model_falls_back_when_flash_unavailable():
    backend = _backend("candidate")
    backend.models = ["mimo-v2.5-pro"]

    assert runtime._readiness_model(backend) == "mimo-v2.5-pro"


def test_probe_without_models_fails_explicitly(monkeypatch):
    fake = FakeTransport()
    monkeypatch.setattr(runtime, "_transport", fake)
    backend = _backend("candidate", lifecycle="active")
    backend.models = []

    with pytest.raises(ValueError, match="backend has no configured models"):
        runtime._readiness_non_stream_body(backend)
    assert fake.json_bodies == []
    assert fake.stream_bodies == []


def test_start_probe_does_not_attempt_removed_free_api_pool(monkeypatch, caplog):
    async def idle_loop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(runtime, "_probe_loop", idle_loop)
    monkeypatch.setattr(runtime, "_rotation_loop", idle_loop)

    async def scenario():
        with caplog.at_level(logging.ERROR):
            runtime.start_probe()
            await asyncio.sleep(0)

        tasks = [runtime._probe_task, runtime._rotation_task]
        monkeypatch.setattr(runtime, "_probe_task", None)
        monkeypatch.setattr(runtime, "_rotation_task", None)
        for task in tasks:
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(scenario())

    assert "free API pool" not in caplog.text


def test_shutdown_does_not_attempt_removed_free_api_pool(caplog):
    async def scenario():
        with caplog.at_level(logging.ERROR):
            await runtime.shutdown()

    asyncio.run(scenario())

    assert "free API pool" not in caplog.text


def test_persist_backend_runtime_state_logs_failures(monkeypatch, caplog):
    backend = _backend("candidate", lifecycle="warming")

    def fail_update(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(backend_store, "update_backend", fail_update)

    runtime._persist_backend_runtime_state(backend)

    assert "Failed to persist backend state for candidate" in caplog.text


def test_single_active_enforcement_drains_extra_active_backends(monkeypatch):
    chosen = _backend("chosen")
    extra_a = _backend("extra-a")
    extra_b = _backend("extra-b")
    reg = BackendRegistry([chosen, extra_a, extra_b])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    retired = runtime._retire_other_backends(chosen)

    active = {b.backend_id for b in reg.all() if b.lifecycle == "active"}
    draining = {b.backend_id for b in reg.all() if b.lifecycle == "draining"}
    assert active == {"chosen"}
    assert draining == {"extra-a", "extra-b"}
    assert retired == ["extra-a", "extra-b"]


def test_prepare_account_deploy_drains_matching_active_backend(monkeypatch):
    old = _backend("old")
    old.account_id = "alice"
    old.in_flight = 0
    peer = _backend("peer")
    peer.account_id = "bob"
    reg = BackendRegistry([old, peer])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.prepare_account_deploy("alice.json")

    assert result["drained"] == ["old"]
    assert result["blocked"] == []
    assert old.lifecycle == "draining"
    assert peer.lifecycle == "active"


def test_prepare_account_deploy_drains_even_when_no_peer_exists(monkeypatch):
    only = _backend("only")
    only.account_id = "alice"
    reg = BackendRegistry([only])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.prepare_account_deploy("alice")

    assert result["drained"] == ["only"]
    assert result["blocked"] == []
    assert only.lifecycle == "draining"


def test_complete_account_deploy_activates_target_and_drains_other_active(monkeypatch):
    old = _backend("old", lifecycle="draining")
    old.account_id = "alice"
    peer = _backend("peer")
    peer.account_id = "bob"
    reg = BackendRegistry([old, peer])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "reload_backends", lambda: len(reg.all()))
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.complete_account_deploy("alice")

    assert result["warmed"] == []
    assert result["activated"] == ["old"]
    assert result["retired"] == ["peer"]
    assert old.lifecycle == "active"
    assert peer.lifecycle == "draining"


def test_complete_account_deploy_activates_when_no_peer_exists(monkeypatch):
    only = _backend("only", lifecycle="draining")
    only.account_id = "alice"
    reg = BackendRegistry([only])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "reload_backends", lambda: len(reg.all()))
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.complete_account_deploy("alice")

    assert result["warmed"] == []
    assert result["activated"] == ["only"]
    assert only.lifecycle == "active"


def test_probeable_models_excludes_tts_and_asr():
    b = _backend("x")
    b.models = [
        "mimo-v2.5-pro", "mimo-v2-flash", "mimo-v2.5-tts",
        "mimo-v2.5-tts-voiceclone", "mimo-v2.5-asr", "mimo-v2-omni",
    ]
    assert runtime._probeable_models(b) == [
        "mimo-v2.5-pro", "mimo-v2-flash", "mimo-v2-omni",
    ]


def test_readiness_model_uses_first_probeable_not_hardcoded_flash():
    b = _backend("x")
    b.models = ["mimo-v2.5-pro", "mimo-v2-flash"]
    assert runtime._readiness_model(b) == "mimo-v2.5-pro"
    b.models = ["mimo-v2.5-tts", "mimo-v2-omni"]
    assert runtime._readiness_model(b) == "mimo-v2-omni"


def test_health_degrades_then_dies_then_recovers():
    b = _backend("x")
    b.record_failure("boom")
    assert b.health == "degraded"
    b.record_failure("boom")
    b.record_failure("boom")
    assert b.health == "dead"
    b.record_success()
    assert b.health == "alive"
    assert b.consecutive_failures == 0


def test_status_label_reflects_states():
    b = _backend("x", lifecycle="active")  # _backend() records a success → alive
    assert b.status_label() == "online"
    b.record_failure("partial")  # 1 failure < threshold → degraded, breaker closed
    assert b.status_label() == "degraded"
    b.record_success()
    b.enabled = False
    assert b.status_label() == "disabled"
    b.enabled = True
    b.lifecycle = "failed"
    assert b.status_label() == "failed"
    b.lifecycle = "warming"
    b.readiness_successes = 0
    assert b.status_label() == "warming"
    b.readiness_successes = 1
    assert b.status_label() == "warming_ready"
