from __future__ import annotations

import json

import pytest

import gateway.secrets_store as secrets_store


@pytest.fixture
def isolated_secrets_store(tmp_path, monkeypatch):
    original_values = {
        field: getattr(secrets_store.secrets, field)
        for field in secrets_store._EDITABLE
    }
    original_locked = set(secrets_store._env_locked)
    monkeypatch.setattr(secrets_store, "DATA_PATH", tmp_path / "secrets.json")
    secrets_store._env_locked.clear()
    seed = {
        "panel_password": "old-password",
        "public_api_token": "sk-mimo-old",
        "upstream_api_key": "upstream-old",
        "panel_session_token": "session-old",
        "status_api_token": "st-mimo-old",
    }
    for field, value in seed.items():
        setattr(secrets_store.secrets, field, value)
    yield secrets_store
    for field, value in original_values.items():
        setattr(secrets_store.secrets, field, value)
    secrets_store._env_locked.clear()
    secrets_store._env_locked.update(original_locked)


def test_update_rejects_empty_required_secret_without_partial_save(isolated_secrets_store):
    result = isolated_secrets_store.update({
        "panel_password": "",
        "public_api_token": "sk-mimo-new",
    })

    assert result["errors"] == {"panel_password": "不能为空"}
    assert isolated_secrets_store.secrets.panel_password == "old-password"
    assert isolated_secrets_store.secrets.public_api_token == "sk-mimo-old"
    assert not isolated_secrets_store.DATA_PATH.exists()


def test_update_rejects_whitespace_in_token_fields(isolated_secrets_store):
    result = isolated_secrets_store.update({"public_api_token": "sk-mimo bad"})

    assert result["errors"] == {"public_api_token": "不能包含空白字符"}
    assert isolated_secrets_store.secrets.public_api_token == "sk-mimo-old"


def test_update_allows_empty_upstream_api_key(isolated_secrets_store):
    result = isolated_secrets_store.update({"upstream_api_key": ""})

    assert result["errors"] == {}
    assert result["changed"] == ["upstream_api_key"]
    assert isolated_secrets_store.secrets.upstream_api_key == ""
    saved = json.loads(isolated_secrets_store.DATA_PATH.read_text(encoding="utf-8"))
    assert saved["upstream_api_key"] == ""


def test_update_skips_env_locked_fields_before_validation(isolated_secrets_store):
    isolated_secrets_store._env_locked.add("public_api_token")

    result = isolated_secrets_store.update({"public_api_token": ""})

    assert result["errors"] == {}
    assert result["skipped"] == ["public_api_token"]
    assert isolated_secrets_store.secrets.public_api_token == "sk-mimo-old"
