from claw.auto_deploy import (
    _is_retryable_create_429,
    _parse_ssh_pubkey,
    _render_ssh_payload,
    _REVERSE_TUNNEL_SH,
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


def test_create_non_429_error_is_not_retryable():
    assert not _is_retryable_create_429({
        "code": 500,
        "msg": "internal error",
    })


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
