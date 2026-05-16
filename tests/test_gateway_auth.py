from __future__ import annotations

import asyncio

import pytest

from gateway.auth import authenticate_gateway_request
from gateway.config import APIKeyStore
from gateway.core import AuthError


class _State:
    pass


class _Request:
    def __init__(self, *, token: str = "", cookies: dict[str, str] | None = None,
                 header: str = "Authorization"):
        if token:
            if header == "Authorization":
                self.headers = {"Authorization": f"Bearer {token}"}
            else:
                # Anthropic clients send x-api-key; MiMo native uses api-key.
                self.headers = {header: token}
        else:
            self.headers = {}
        self.cookies = cookies or {}
        self.state = _State()


def test_authenticate_gateway_request_accepts_api_key_store_key(tmp_path, monkeypatch):
    import gateway.auth as auth

    store = APIKeyStore(str(tmp_path / "keys.db"))
    created = store.create(label="test", allowed_models=["m"])
    monkeypatch.setattr(auth, "_key_store", store)

    principal = asyncio.run(
        authenticate_gateway_request(_Request(token=created.secret), auth_cookie="panel")
    )

    assert principal.key_id == created.record.key_id
    assert principal.allowed_models == ("m",)
    store.close()
    monkeypatch.setattr(auth, "_key_store", None)


def test_authenticate_gateway_request_accepts_x_api_key_header(tmp_path, monkeypatch):
    """Claude Code / Anthropic SDK clients default to x-api-key, not Bearer."""
    import gateway.auth as auth

    store = APIKeyStore(str(tmp_path / "keys.db"))
    created = store.create(label="cc", allowed_models=["m"])
    monkeypatch.setattr(auth, "_key_store", store)

    principal = asyncio.run(
        authenticate_gateway_request(
            _Request(token=created.secret, header="x-api-key"),
            auth_cookie="panel",
        )
    )

    assert principal.key_id == created.record.key_id
    store.close()
    monkeypatch.setattr(auth, "_key_store", None)


def test_authenticate_gateway_request_accepts_api_key_header(tmp_path, monkeypatch):
    """MiMo's own docs list `api-key:` as an accepted header form."""
    import gateway.auth as auth

    store = APIKeyStore(str(tmp_path / "keys.db"))
    created = store.create(label="mimo-native", allowed_models=["m"])
    monkeypatch.setattr(auth, "_key_store", store)

    principal = asyncio.run(
        authenticate_gateway_request(
            _Request(token=created.secret, header="api-key"),
            auth_cookie="panel",
        )
    )

    assert principal.key_id == created.record.key_id
    store.close()
    monkeypatch.setattr(auth, "_key_store", None)


def test_authenticate_gateway_request_bearer_takes_precedence_over_x_api_key(tmp_path, monkeypatch):
    """If a caller provides both, Authorization wins — matches OpenAI/Anthropic
    SDK behavior (they don't expect to send both, but if they did, Bearer is
    the more explicit choice)."""
    import gateway.auth as auth

    store = APIKeyStore(str(tmp_path / "keys.db"))
    valid = store.create(label="real", allowed_models=["m"])
    monkeypatch.setattr(auth, "_key_store", store)

    class _Both:
        def __init__(self):
            self.headers = {
                "Authorization": f"Bearer {valid.secret}",
                "x-api-key": "this-fake-key-should-not-be-tried",
            }
            self.cookies = {}
            self.state = _State()

    principal = asyncio.run(
        authenticate_gateway_request(_Both(), auth_cookie="panel")
    )
    assert principal.key_id == valid.record.key_id
    store.close()
    monkeypatch.setattr(auth, "_key_store", None)


def test_authenticate_gateway_request_rejects_missing_token():
    with pytest.raises(AuthError):
        asyncio.run(authenticate_gateway_request(_Request(), auth_cookie="panel"))


def test_authenticate_gateway_request_rejects_empty_x_api_key():
    """Empty x-api-key header shouldn't slip past as a valid token."""
    class _Req:
        def __init__(self):
            self.headers = {"x-api-key": "   "}
            self.cookies = {}
            self.state = _State()
    with pytest.raises(AuthError):
        asyncio.run(authenticate_gateway_request(_Req(), auth_cookie="panel"))
