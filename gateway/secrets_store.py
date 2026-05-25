"""Secrets store — loads credentials from data/secrets.json with env-var fallback.

``secrets.json`` is gitignored. On first load, if the file doesn't exist
a fresh one is generated with random values so the user never needs to
hand-edit it.

Usage::

    from gateway.secrets_store import secrets
    password = secrets.panel_password
    token    = secrets.public_api_token
"""
from __future__ import annotations

import json
import os
import secrets as _secrets_mod
from dataclasses import dataclass, field

from project_paths import SECRETS_PATH

DATA_PATH = SECRETS_PATH


@dataclass
class Secrets:
    panel_password: str = ""
    public_api_token: str = ""
    upstream_api_key: str = ""


def _generate_defaults() -> dict[str, str]:
    return {
        "panel_password": "Aoki-MiMo",
        "public_api_token": f"sk-mimo-{_secrets_mod.token_urlsafe(32)}",
        "upstream_api_key": "",
    }


def _load() -> Secrets:
    raw: dict[str, str] = {}
    if DATA_PATH.exists():
        try:
            raw = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass

    defaults = _generate_defaults()

    # Merge: file values win, defaults fill gaps, env vars override everything.
    merged = {**defaults, **raw}

    # Save back if file had missing keys.
    if set(defaults) - set(raw):
        DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        DATA_PATH.write_text(
            json.dumps(merged, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # Env overrides (useful for Docker / CI).
    env_map = {
        "MIMO_PANEL_PASSWORD": "panel_password",
        "MIMO_PUBLIC_API_TOKEN": "public_api_token",
        "MIMO_UPSTREAM_API_KEY": "upstream_api_key",
    }
    for env_key, field_name in env_map.items():
        val = os.environ.get(env_key)
        if val:
            merged[field_name] = val

    return Secrets(**merged)


secrets: Secrets = _load()
