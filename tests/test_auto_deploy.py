from claw.auto_deploy import (
    _is_account_risk_create,
    _is_retryable_create_429,
    _parse_ssh_pubkey,
    _render_ssh_payload,
    _relay_policy,
    _relay_reason,
    _REVERSE_TUNNEL_SH,
    probe_create_risk,
)


def test_create_429_pool_full_message_is_retryable():
    assert _is_retryable_create_429({
        "code": 429,
        "msg": "Mimo Claw使用中机器已达上限",
    })


def test_create_429_too_many_requests_message_is_retryable():
    assert _is_retryable_create_429({
        "code": 429,
        "msg": "Mimo Claw当前创建请求较多，请稍后重试",
    })


def test_create_429_too_frequent_message_is_retryable():
    """Live capture 2026-07-10: immediate re-create after success."""
    assert _is_retryable_create_429({
        "code": 429,
        "msg": "Mimo Claw创建请求过于频繁，请稍后重试",
    })


def test_create_non_429_error_is_not_retryable():
    assert not _is_retryable_create_429({
        "code": 500,
        "msg": "internal error",
    })


def test_risk_create_markers_match_live_wording():
    # Live capture 2026-07-10: body code is 200 (not 0), msg is the gate text.
    assert _is_account_risk_create({
        "code": 200,
        "msg": "当前账号存在风险，暂无法创建",
    })
    assert _is_account_risk_create({
        "code": 1,
        "msg": "当前账号存在风险，暂无法创建",
    })
    assert not _is_account_risk_create({
        "code": 429,
        "msg": "Mimo Claw创建请求过于频繁，请稍后重试",
    })
    assert not _is_account_risk_create({
        "code": 7001,
        "msg": "每天可创建1次免费使用4小时，您今日额度已用完，可前往订阅解锁更多权益",
    })


def test_probe_create_risk_classifies_rate_quota_capacity(monkeypatch):
    """probe_create_risk must not treat rate/quota as RISK."""
    import importlib

    responses = iter([
        ("HTTP_200", {"code": 429, "msg": "Mimo Claw创建请求过于频繁，请稍后重试"}),
        ("HTTP_200", {"code": 7001, "msg": "每天可创建1次免费使用4小时，您今日额度已用完"}),
        ("HTTP_200", {"code": 429, "msg": "Mimo Claw使用中机器已达上限"}),
        ("HTTP_200", {"code": 1, "msg": "当前账号存在风险，暂无法创建"}),
        ("HTTP_200", {"code": 0, "msg": "成功", "data": {"status": "CREATING"}}),
    ])

    class _App:
        @staticmethod
        def curl_api(*_a, **_k):
            return next(responses)

    real_import = importlib.import_module

    def import_module(name, package=None):
        if name == "app":
            return _App()
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", import_module)

    assert probe_create_risk([]) == "RATE"
    assert probe_create_risk([]) == "QUOTA"
    assert probe_create_risk([]) == "CAPACITY"
    assert probe_create_risk([]) == "RISK"
    assert probe_create_risk([]) == "OK"


def test_create_malformed_response_is_not_retryable():
    assert not _is_retryable_create_429("HTTP_429")


def test_parse_ssh_pubkey_extracts_ed25519_from_reply():
    reply = "好的，公钥如下：\nssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIxxxYYY claw\n以上。"
    pk = _parse_ssh_pubkey(reply)
    assert pk == "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIxxxYYY claw"


def test_parse_ssh_pubkey_returns_none_when_absent():
    assert _parse_ssh_pubkey("没有任何密钥内容") is None


def test_render_ssh_payload_substitutes_target_placeholders():
    target = {
        "host": "203.0.113.9",
        "tunnel_user": "tunnel",
        "ssh_port": 2222,
        "remote_api_port": 19090,
    }
    rendered = _render_ssh_payload(_REVERSE_TUNNEL_SH, target)
    # placeholders are substituted into the shell var assignments (the -R line
    # itself uses ${VAR} expansion resolved at runtime, not literal values).
    assert "__TARGET_HOST__" not in rendered and "__REMOTE_API_PORT__" not in rendered
    assert 'TARGET_HOST="203.0.113.9"' in rendered
    assert 'TARGET_SSH_PORT="2222"' in rendered
    assert 'REMOTE_API_PORT="19090"' in rendered
    assert 'LOCAL_PROXY_PORT="18800"' in rendered
    assert 'TARGET_USER="tunnel"' in rendered


def test_relay_policy_targets_two_active_for_overlap_fleet():
    p0 = _relay_policy(0)
    assert p0["desired_active"] == 0
    p5 = _relay_policy(5)
    assert p5["desired_active"] == 2
    assert p5["open_interval_s"] == 2 * 60 * 60
    assert p5["drain_before_s"] == 30 * 60
    assert p5["hard_ttl_s"] == 4 * 60 * 60


def test_relay_reason_uses_two_hour_open_and_drain_window():
    assert _relay_reason((1 * 60 * 60) + (59 * 60)) == "fresh"
    assert _relay_reason(2 * 60 * 60) == "open_next_due"
    assert _relay_reason((3 * 60 * 60) + (30 * 60)) == "draining_window"
    assert _relay_reason(4 * 60 * 60) == "expired"
