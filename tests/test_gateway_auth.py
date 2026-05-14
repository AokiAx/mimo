from __future__ import annotations

import asyncio

import pytest

from gateway.auth import authenticate_gateway_request
from gateway.config import APIKeyStore
from gateway.core import AuthError


class _State:
    pass


class _Request:
    def __init__(self, *, token: str = "", cookies: dict[str, str] | None = None):
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
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


def test_authenticate_gateway_request_rejects_missing_token():
    with pytest.raises(AuthError):
        asyncio.run(authenticate_gateway_request(_Request(), auth_cookie="panel"))
