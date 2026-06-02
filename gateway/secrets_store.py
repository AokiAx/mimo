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
import threading
from dataclasses import dataclass, field
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data" / "secrets.json"


@dataclass
class Secrets:
    panel_password: str = ""
    public_api_token: str = ""
    upstream_api_key: str = ""
    panel_session_token: str = ""
    status_api_token: str = ""


def _generate_defaults() -> dict[str, str]:
    return {
        "panel_password": "Aoki-MiMo",
        "public_api_token": f"sk-mimo-{_secrets_mod.token_urlsafe(32)}",
        "upstream_api_key": "",
        # Panel session cookie value — MUST be separate from the API token so a
        # leaked API token can't be replayed as an admin session cookie.
        "panel_session_token": _secrets_mod.token_urlsafe(32),
        # Read-only key for the public status endpoint (/api/status). Separate
        # from every other secret so it can be handed to an external status page
        # without exposing the API or panel.
        "status_api_token": f"st-mimo-{_secrets_mod.token_urlsafe(24)}",
    }


_ENV_MAP = {
    "MIMO_PANEL_PASSWORD": "panel_password",
    "MIMO_PUBLIC_API_TOKEN": "public_api_token",
    "MIMO_UPSTREAM_API_KEY": "upstream_api_key",
    "MIMO_PANEL_SESSION_TOKEN": "panel_session_token",
    "MIMO_STATUS_API_TOKEN": "status_api_token",
}

# Fields whose value is forced by an env var this process — the panel may show
# them but must not pretend to change them (the env wins on every restart).
_env_locked: set[str] = set()
_lock = threading.Lock()


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
    _env_locked.clear()
    for env_key, field_name in _ENV_MAP.items():
        val = os.environ.get(env_key)
        if val:
            merged[field_name] = val
            _env_locked.add(field_name)

    return Secrets(**merged)


secrets: Secrets = _load()


# ─── panel-managed mutation ───
# All consumers read attributes off the shared ``secrets`` singleton at call
# time (gateway.auth, runtime, app.py status/login/auth), so updates here mutate
# that instance IN PLACE — never reassign — to stay live without a restart.

_EDITABLE = (
    "panel_password", "public_api_token", "upstream_api_key",
    "panel_session_token", "status_api_token",
)
# Token fields and how to mint a fresh value when rotating.
_ROTATORS = {
    "public_api_token": lambda: f"sk-mimo-{_secrets_mod.token_urlsafe(32)}",
    "panel_session_token": lambda: _secrets_mod.token_urlsafe(32),
    "status_api_token": lambda: f"st-mimo-{_secrets_mod.token_urlsafe(24)}",
}


def _persist() -> None:
    """Write the current singleton back to data/secrets.json (file = source of
    truth for non-env fields). Env-locked fields are written too so the file
    stays complete, but the env still wins on next load."""
    data = {f: getattr(secrets, f) for f in _EDITABLE}
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def view() -> dict:
    """Admin-only snapshot: full values + which fields are env-locked. The panel
    is already the privileged surface (it can read secrets.json), so values are
    returned in clear for copy/rotate."""
    return {
        "secrets": {f: getattr(secrets, f) for f in _EDITABLE},
        "env_locked": sorted(_env_locked),
    }


def update(changes: dict) -> dict:
    """Apply ``{field: value}`` to editable, non-env-locked fields. Mutates the
    singleton in place + persists. Returns {changed:[...], skipped:[...]}."""
    changed, skipped = [], []
    with _lock:
        for field_name, value in (changes or {}).items():
            if field_name not in _EDITABLE:
                continue
            if field_name in _env_locked:
                skipped.append(field_name)
                continue
            if value is None:
                continue
            value = str(value)
            if value == getattr(secrets, field_name):
                continue
            setattr(secrets, field_name, value)
            changed.append(field_name)
        if changed:
            _persist()
    return {"changed": changed, "skipped": skipped}


def rotate(field_name: str) -> str | None:
    """Regenerate a token field to a fresh random value. Returns the new value,
    or None if the field isn't rotatable or is env-locked."""
    if field_name not in _ROTATORS or field_name in _env_locked:
        return None
    new_value = _ROTATORS[field_name]()
    with _lock:
        setattr(secrets, field_name, new_value)
        _persist()
    return new_value
