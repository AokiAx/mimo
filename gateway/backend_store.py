"""Persistent backend store — CRUD for the backends section of data/config.json.

Each entry is a dict with:
  id, name, base_url, models (list[str]), api_key, weight, enabled, account_id

Legacy format with ``model`` (str) + ``aliases`` (comma-string) is migrated
to ``models`` on read; written entries always use the new shape.

The store is the single source of truth. ``runtime.py`` reloads the
BackendRegistry from it on startup and after every mutation.
"""
from __future__ import annotations

import secrets
import threading
from typing import Any

from gateway import config_store

_lock = threading.Lock()


def _empty() -> dict:
    return {"backends": []}


def _normalize_models(raw: Any) -> list[str]:
    """Accept list[str], comma-string, or a single str; emit a deduped list."""
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, str):
        items = raw.split(",")
    else:
        return out
    for m in items:
        if not isinstance(m, str):
            continue
        s = m.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _migrate_entry(entry: dict) -> dict:
    """If entry uses legacy {model, aliases}, fold them into models[]."""
    if "models" in entry and isinstance(entry["models"], list):
        entry["models"] = _normalize_models(entry["models"])
        entry.pop("model", None)
        entry.pop("aliases", None)
        return entry
    merged: list[str] = []
    primary = entry.pop("model", None)
    if isinstance(primary, str) and primary.strip():
        merged.append(primary.strip())
    aliases = entry.pop("aliases", None)
    if isinstance(aliases, str):
        for a in aliases.split(","):
            a = a.strip()
            if a and a not in merged:
                merged.append(a)
    entry["models"] = merged
    return entry


def _load() -> dict:
    data = config_store.get_section("backends", None)
    if not isinstance(data, dict):
        return _empty()
    for b in data.get("backends") or []:
        _migrate_entry(b)
    return data


def _save(data: dict) -> None:
    config_store.set_section("backends", data)


def list_backends() -> list[dict[str, Any]]:
    with _lock:
        data = _load()
    return data.get("backends") or []


def add_backend(
    *,
    name: str,
    base_url: str,
    models: Any = None,
    api_key: str = "",
    weight: int = 1,
    account_id: str = "",
    # legacy kwargs accepted for callers that still pass model/aliases
    model: str = "",
    aliases: str = "",
) -> dict[str, Any]:
    name = (name or "").strip()
    base_url = (base_url or "").strip().rstrip("/")
    model_list = _normalize_models(models) if models is not None else []
    if not model_list:
        # legacy path
        if model:
            model_list.append(model.strip())
        for a in (aliases or "").split(","):
            a = a.strip()
            if a and a not in model_list:
                model_list.append(a)
    if not name:
        raise ValueError("name 不能为空")
    if not base_url:
        raise ValueError("base_url 不能为空")
    if not model_list:
        raise ValueError("models 不能为空")

    backend_id = secrets.token_hex(6)
    entry: dict[str, Any] = {
        "id": backend_id,
        "name": name,
        "base_url": base_url,
        "models": model_list,
        "api_key": api_key,
        "weight": max(1, int(weight)),
        "account_id": account_id,
        "enabled": True,
        "lifecycle": "warming",
    }
    with _lock:
        data = _load()
        for b in data["backends"]:
            if b.get("name") == name:
                raise ValueError(f"后端名 '{name}' 已存在")
        data["backends"].append(entry)
        _save(data)
    return entry


def upsert_account_backend(
    *, account_id: str, base_url: str, api_key: str = "",
    models: Any = None, name: str = "",
) -> dict[str, Any]:
    """Create or update the backend bound to an account. Used by the SSH deploy
    to point the account at its reverse-tunnel upstream (http://host:port).
    Matches the first existing backend with this account_id; else creates one."""
    base_url = (base_url or "").strip().rstrip("/")
    with _lock:
        data = _load()
        for b in data["backends"]:
            if (b.get("account_id") or "").casefold() == (account_id or "").casefold():
                existing_id = b["id"]
                break
        else:
            existing_id = None
    if existing_id:
        fields: dict[str, Any] = {"base_url": base_url, "api_key": api_key,
                                  "enabled": True, "lifecycle": "warming"}
        if models:
            fields["models"] = models
        return update_backend(existing_id, **fields)
    return add_backend(
        name=name or account_id,
        base_url=base_url,
        models=models or ["mimo-v2.5-pro"],
        api_key=api_key,
        account_id=account_id,
    )


def update_backend(backend_id: str, **fields: Any) -> dict[str, Any] | None:
    allowed = {"name", "base_url", "models", "api_key", "weight",
               "account_id", "enabled", "lifecycle", "generation_id",
               "rotation_failures", "disabled_until",
               "in_detection", "detection_entered_at"}
    # Legacy: caller passes {model, aliases} — fold into models.
    if "model" in fields or "aliases" in fields:
        legacy = []
        m = fields.pop("model", "")
        if isinstance(m, str) and m.strip():
            legacy.append(m.strip())
        a = fields.pop("aliases", "")
        if isinstance(a, str):
            for x in a.split(","):
                x = x.strip()
                if x and x not in legacy:
                    legacy.append(x)
        if legacy and "models" not in fields:
            fields["models"] = legacy

    with _lock:
        data = _load()
        for b in data["backends"]:
            if b["id"] == backend_id:
                for k, v in fields.items():
                    if k not in allowed:
                        continue
                    if k == "base_url" and isinstance(v, str):
                        v = v.rstrip("/")
                    if k == "models":
                        v = _normalize_models(v)
                        if not v:
                            continue  # ignore empty list — keep old
                    b[k] = v
                _save(data)
                return b
    return None


def delete_backend(backend_id: str) -> bool:
    with _lock:
        data = _load()
        before = len(data["backends"])
        data["backends"] = [b for b in data["backends"] if b["id"] != backend_id]
        if len(data["backends"]) == before:
            return False
        _save(data)
    return True
