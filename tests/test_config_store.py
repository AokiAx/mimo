from __future__ import annotations

import json

import gateway.config_store as config_store
import gateway.model_groups_store as model_groups_store


def test_config_migration_leaves_model_groups_standalone(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    backends = {"backends": []}
    model_groups = {
        "groups": [
            {
                "id": "mimo",
                "name": "MiMo",
                "description": "",
                "mappings": [],
            }
        ]
    }
    (tmp_path / "backends.json").write_text(json.dumps(backends), encoding="utf-8")
    (tmp_path / "model_groups.json").write_text(
        json.dumps(model_groups),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_store, "CONFIG_PATH", config_path)

    config_store.migrate_once()

    assert json.loads(config_path.read_text(encoding="utf-8")) == {
        "backends": backends,
    }
    assert json.loads((tmp_path / "model_groups.json").read_text(encoding="utf-8")) == model_groups
    assert (tmp_path / "backends.json.bak").exists()
    assert not (tmp_path / "model_groups.json.bak").exists()


def test_model_groups_store_extracts_embedded_config_section(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    model_groups = {
        "groups": [
            {
                "id": "mimo",
                "name": "MiMo",
                "description": "",
                "mappings": [
                    {
                        "id": "m_001",
                        "exposed_name": "claude",
                        "native_model": "mimo-v2.5-pro",
                        "protocols": ["anthropic"],
                    }
                ],
            }
        ]
    }
    config_path.write_text(
        json.dumps({"backends": {"backends": []}, "model_groups": model_groups}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_store, "CONFIG_PATH", config_path)
    monkeypatch.delenv("MIMO_MODEL_GROUPS", raising=False)

    assert model_groups_store.list_groups() == model_groups["groups"]
    assert json.loads((tmp_path / "model_groups.json").read_text(encoding="utf-8")) == model_groups
    assert "model_groups" not in json.loads(config_path.read_text(encoding="utf-8"))
