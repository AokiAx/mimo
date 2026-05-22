from claw.auto_deploy import _is_retryable_create_429


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
