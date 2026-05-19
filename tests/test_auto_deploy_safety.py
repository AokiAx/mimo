from __future__ import annotations

from datetime import datetime

from claw import auto_deploy


class MemoryLog:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def log(self, msg: str) -> None:
        self.lines.append(msg)


def test_deploy_start_blocks_without_takeover_backend(monkeypatch):
    def fake_prepare(account_filename: str, *, api_port: int | None = None):
        assert account_filename == "alice.json"
        assert api_port == 8800
        return {"matched": ["only"], "drained": [], "blocked": ["only"]}

    monkeypatch.setattr("gateway.runtime.prepare_account_deploy", fake_prepare)
    log = MemoryLog()

    prepared = auto_deploy._notify_gateway_deploy_start("alice.json", 8800, log)

    assert prepared["safe_to_destroy"] is False
    assert prepared["blocked"] == ["only"]
    assert any("跳过销毁" in line for line in log.lines)


def test_deploy_start_allows_destroy_after_drain(monkeypatch):
    calls: list[str] = []

    def fake_prepare(_account_filename: str, *, api_port: int | None = None):
        return {"matched": ["old"], "drained": ["old"], "blocked": []}

    def fake_wait(_account_filename: str, *, api_port: int | None = None):
        calls.append("wait")
        return {"success": True, "pending": []}

    monkeypatch.setattr("gateway.runtime.prepare_account_deploy", fake_prepare)
    monkeypatch.setattr("gateway.runtime.wait_for_account_drain", fake_wait)
    log = MemoryLog()

    prepared = auto_deploy._notify_gateway_deploy_start("alice.json", 8800, log)

    assert prepared["safe_to_destroy"] is True
    assert prepared["drained"] == ["old"]
    assert calls == ["wait"]


def _cfg(account_count: int) -> dict:
    return {
        "accounts": {
            f"acc{i}.json": {
                "enabled": True,
                "api_port": 8800 + i,
            }
            for i in range(account_count)
        }
    }


def _backend(account: str, age_s: float, *, healthy: bool = True, lifecycle: str = "active") -> dict:
    index = int(account.replace("acc", "", 1).replace(".json", ""))
    return {
        "id": f"backend-{account}",
        "account": account,
        "url": f"http://127.0.0.1:{8800 + index}",
        "models": ["mimo-v2.5-pro"],
        "healthy": healthy,
        "enabled": True,
        "lifecycle": lifecycle,
        "active_for_s": age_s,
    }


def test_rotation_policy_scales_for_account_counts():
    assert auto_deploy._rotation_policy(3) == {
        "desired_active": 3,
        "normal_min_active": 3,
        "emergency_min_active": 3,
        "normal_max_parallel": 1,
        "emergency_max_parallel": 1,
    }
    assert auto_deploy._rotation_policy(6) == {
        "desired_active": 6,
        "normal_min_active": 5,
        "emergency_min_active": 4,
        "normal_max_parallel": 1,
        "emergency_max_parallel": 2,
    }
    assert auto_deploy._rotation_policy(9) == {
        "desired_active": 9,
        "normal_min_active": 8,
        "emergency_min_active": 6,
        "normal_max_parallel": 1,
        "emergency_max_parallel": 3,
    }


def test_coordinator_selects_one_normal_rotation_for_six_accounts():
    cfg = _cfg(6)
    backends = [_backend(f"acc{i}.json", 10 * 60) for i in range(6)]
    backends[2]["active_for_s"] = 41 * 60

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["enabled_accounts"] == 6
    assert plan["counts"]["active_selectable"] == 6
    assert plan["counts"]["deploying"] == 0
    assert [item["account"] for item in plan["selected"]] == ["acc2.json"]
    assert plan["accounts"]["acc2.json"]["next_rotation_reason"] == "target_age"


def test_coordinator_allows_one_rotation_for_three_accounts_with_two_takeovers():
    cfg = _cfg(3)
    backends = [
        _backend("acc0.json", 41 * 60),
        _backend("acc1.json", 20 * 60),
        _backend("acc2.json", 20 * 60),
    ]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["normal_min_active"] == 3
    assert plan["counts"]["min_active_required"] == 2
    assert [item["account"] for item in plan["selected"]] == ["acc0.json"]


def test_coordinator_allows_two_emergency_rotations_for_six_accounts():
    cfg = _cfg(6)
    backends = [
        _backend("acc0.json", 56 * 60),
        _backend("acc1.json", 54 * 60),
        _backend("acc2.json", 52 * 60),
        _backend("acc3.json", 20 * 60),
        _backend("acc4.json", 20 * 60),
        _backend("acc5.json", 20 * 60),
    ]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["min_active_required"] == 4
    assert plan["counts"]["max_parallel"] == 2
    assert [item["account"] for item in plan["selected"]] == ["acc0.json", "acc1.json"]
    assert plan["accounts"]["acc0.json"]["next_rotation_reason"] == "hard_expiry_age"
    assert plan["accounts"]["acc2.json"]["skip_reason"] == "queued"


def test_coordinator_skips_active_rotation_when_capacity_would_drop_below_emergency_minimum():
    cfg = _cfg(6)
    backends = [
        _backend("acc0.json", 56 * 60),
        _backend("acc1.json", 56 * 60),
        _backend("acc2.json", 20 * 60),
        _backend("acc3.json", 20 * 60),
        _backend("acc4.json", 20 * 60, healthy=False),
        _backend("acc5.json", 20 * 60, healthy=False),
    ]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["active_selectable"] == 4
    assert [item["account"] for item in plan["selected"]] == ["acc4.json", "acc5.json"]
    assert plan["accounts"]["acc0.json"]["skip_reason"] == "skipped_capacity"
    assert plan["accounts"]["acc1.json"]["skip_reason"] == "skipped_capacity"


def test_coordinator_repairs_unselectable_accounts_without_reducing_capacity():
    cfg = _cfg(6)
    backends = [
        _backend("acc0.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc1.json", 20 * 60),
        _backend("acc2.json", 20 * 60),
        _backend("acc3.json", 20 * 60),
        _backend("acc4.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc5.json", 0, healthy=False, lifecycle="failed"),
    ]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["active_selectable"] == 3
    assert plan["counts"]["repair_candidate_count"] == 3
    assert plan["counts"]["min_active_required"] == 3
    assert [item["account"] for item in plan["selected"]] == ["acc0.json"]
    assert plan["selected"][0]["next_rotation_reason"] == "repair_no_selectable_backend"
    assert plan["accounts"]["acc4.json"]["skip_reason"] == "queued"
    assert plan["accounts"]["acc5.json"]["skip_reason"] == "queued"


def test_coordinator_bootstraps_one_repair_when_no_selectable_backend_exists():
    cfg = _cfg(6)
    backends = [
        _backend("acc0.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc1.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc2.json", 0, healthy=False, lifecycle="failed"),
        _backend("acc3.json", 0, healthy=False, lifecycle="failed"),
        _backend("acc4.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc5.json", 0, healthy=False, lifecycle="failed"),
    ]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["counts"]["active_selectable"] == 0
    assert plan["counts"]["repair_candidate_count"] == 6
    assert plan["counts"]["selected_count"] == 1
    assert plan["selected"][0]["next_rotation_reason"] == "repair_no_selectable_backend"
    assert plan["accounts"][plan["selected"][0]["account"]]["skip_reason"] == ""


def test_coordinator_marks_enabled_account_without_backend_as_unmatched():
    cfg = _cfg(3)
    backends = [_backend("acc0.json", 41 * 60), _backend("acc1.json", 10 * 60)]

    plan = auto_deploy._plan_rotation_batch(
        cfg,
        now=datetime(2026, 5, 19, 12, 0, 0),
        backends=backends,
        active_deploys={},
    )

    assert plan["accounts"]["acc2.json"]["skip_reason"] == "skipped_unmatched"


def test_scheduler_status_exposes_coordinator_fields(monkeypatch):
    cfg = _cfg(6)
    backends = [_backend(f"acc{i}.json", 10 * 60) for i in range(6)]
    backends[0]["active_for_s"] = 41 * 60
    monkeypatch.setattr(auto_deploy, "load_config", lambda: cfg)
    monkeypatch.setattr(auto_deploy, "_load_gateway_backends", lambda: backends)
    monkeypatch.setattr(auto_deploy, "_active_deploys", {})

    status = auto_deploy.get_scheduler_status()

    assert status["policy"]["desired_active"] == 6
    assert status["counts"]["active_selectable"] == 6
    assert status["counts"]["normal_min_active"] == 5
    assert status["counts"]["emergency_min_active"] == 4
    assert status["selected"][0]["account"] == "acc0.json"
    assert status["accounts"]["acc0.json"]["age_min"] == 41.0
    assert status["accounts"]["acc0.json"]["next_rotation_reason"] == "target_age"


def test_scheduler_loop_triggers_only_coordinator_selected_accounts(monkeypatch):
    cfg = _cfg(6)
    saved_configs: list[dict] = []
    triggered: list[str] = []

    plan = {
        "selected": [
            {
                "account": "acc1.json",
                "next_rotation_reason": "target_age",
                "age_min": 41.0,
            }
        ]
    }

    class StopScheduler(Exception):
        pass

    monkeypatch.setattr(auto_deploy, "load_config", lambda: cfg)
    monkeypatch.setattr(auto_deploy, "save_config", lambda updated: saved_configs.append(updated.copy()))
    monkeypatch.setattr(auto_deploy, "_plan_rotation_batch", lambda updated, now=None: plan)
    monkeypatch.setattr(auto_deploy, "trigger_deploy", lambda account: triggered.append(account))

    def stop_after_first_sleep(_seconds: int) -> None:
        auto_deploy._scheduler_running = False
        raise StopScheduler()

    monkeypatch.setattr(auto_deploy.time, "sleep", stop_after_first_sleep)

    try:
        auto_deploy._scheduler_loop()
    except StopScheduler:
        pass

    assert triggered == ["acc1.json"]
    assert saved_configs
    assert cfg["accounts"]["acc1.json"]["last_run"] > 0


def test_trigger_deploy_blocks_when_coordinator_reports_capacity_risk(monkeypatch):
    monkeypatch.setattr(
        auto_deploy,
        "_rotation_safety_for_account",
        lambda _account: {"safe": False, "reason": "skipped_capacity", "plan": {}},
    )

    result = auto_deploy.trigger_deploy("acc0.json")

    assert result["success"] is False
    assert result["reason"] == "skipped_capacity"


def test_manual_trigger_allows_repair_for_unselectable_account(monkeypatch):
    cfg = _cfg(6)
    backends = [
        _backend("acc0.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc1.json", 20 * 60),
        _backend("acc2.json", 20 * 60),
        _backend("acc3.json", 20 * 60),
        _backend("acc4.json", 0, healthy=False, lifecycle="warming"),
        _backend("acc5.json", 0, healthy=False, lifecycle="failed"),
    ]
    monkeypatch.setattr(auto_deploy, "load_config", lambda: cfg)
    monkeypatch.setattr(auto_deploy, "_load_gateway_backends", lambda: backends)
    monkeypatch.setattr(auto_deploy, "_active_deploys", {})

    result = auto_deploy._rotation_safety_for_account("acc0.json")

    assert result["safe"] is True
    assert result["reason"] == "coordinator_selected"


def test_trigger_deploy_marks_account_queued_before_starting_thread(monkeypatch):
    started: list[str] = []

    class FakeThread:
        def __init__(self, target, args, daemon):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            started.append(self.args[0])

    monkeypatch.setattr(
        auto_deploy,
        "_rotation_safety_for_account",
        lambda _account: {"safe": True, "reason": "manual_capacity_ok", "plan": {}},
    )
    monkeypatch.setattr(auto_deploy.threading, "Thread", FakeThread)
    auto_deploy._active_deploys.pop("acc0.json", None)

    result = auto_deploy.trigger_deploy("acc0.json")

    assert result["success"] is True
    assert started == ["acc0.json"]
    assert auto_deploy._active_deploys["acc0.json"]["state"] == "queued"
    auto_deploy._active_deploys.pop("acc0.json", None)
