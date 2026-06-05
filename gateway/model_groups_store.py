"""Persistent model-mapping store — CRUD for data/model_groups.json.

A *group* is a user-named container for one or more *mappings*. A mapping
expresses ``exposed_name → native_model`` plus which incoming protocols
(``openai`` / ``anthropic``) it answers to.

Schema::

    {
      "groups": [
        {
          "id": "mimo",                 # short slug, user-chosen, unique
          "name": "MiMo Native",
          "description": "",
          "mappings": [
            {
              "id": "m_001",            # short autoincrement-style id
              "exposed_name": "mimo-v2.5-pro",
              "native_model": "mimo-v2.5-pro",
              "protocols": ["openai", "anthropic"]
            }
          ]
        }
      ]
    }

Groups exist purely for organisation — they do not affect routing. The
same ``exposed_name`` may appear in multiple mappings (across or within
groups); ``resolve()`` returns the first match.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
from pathlib import Path
from typing import Any

from gateway import config_store

log = logging.getLogger(__name__)

VALID_PROTOCOLS = ("openai", "anthropic")
_SLUG_RE = re.compile(r"^[A-Za-z0-9_\-.]{1,32}$")

_lock = threading.RLock()


def _empty() -> dict:
    return {"groups": []}


def _path() -> Path:
    override = os.environ.get("MIMO_MODEL_GROUPS")
    if override:
        return Path(override)
    return config_store.CONFIG_PATH.parent / "model_groups.json"


def _read_file() -> dict:
    path = _path()
    if not path.exists():
        return _empty()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        log.exception("failed to read %s; treating as empty", path)
        return _empty()
    if not isinstance(data, dict) or not isinstance(data.get("groups"), list):
        return _empty()
    return data


def _write_file(data: dict) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def migrate_once() -> None:
    """Move a pre-split ``model_groups`` section out of config.json once.

    The config store no longer imports data/model_groups.json during legacy
    consolidation. This helper covers installs that already have a
    ``model_groups`` section inside data/config.json from the previous layout.
    """
    with _lock:
        embedded = config_store.get_section("model_groups", None)
        if embedded is None:
            return
        if (
            not _path().exists()
            and isinstance(embedded, dict)
            and isinstance(embedded.get("groups"), list)
        ):
            _write_file(embedded)
        config_store.delete_section("model_groups")


def _load() -> dict:
    migrate_once()
    return _read_file()


def _save(data: dict) -> None:
    _write_file(data)


def _normalize_protocols(raw: Any) -> list[str]:
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return list(VALID_PROTOCOLS)
    out: list[str] = []
    for p in raw:
        if isinstance(p, str) and p in VALID_PROTOCOLS and p not in out:
            out.append(p)
    return out or list(VALID_PROTOCOLS)


def _next_mapping_id(group: dict) -> str:
    """Pick the next ``m_NNN`` id that isn't taken in this group."""
    used: set[str] = {m.get("id", "") for m in group.get("mappings") or []}
    n = 1
    while True:
        candidate = "m_{:03d}".format(n)
        if candidate not in used:
            return candidate
        n += 1


def _validate_slug(slug: str, *, label: str = "id") -> str:
    s = (slug or "").strip()
    if not _SLUG_RE.match(s):
        raise ValueError(f"{label} 必须由字母/数字/-_/. 组成且 ≤32 字符")
    return s


# ────────────── group CRUD ──────────────


def list_groups() -> list[dict[str, Any]]:
    with _lock:
        data = _load()
    return data.get("groups") or []


def get_group(group_id: str) -> dict[str, Any] | None:
    for g in list_groups():
        if g.get("id") == group_id:
            return g
    return None


def add_group(*, id: str, name: str = "", description: str = "") -> dict[str, Any]:
    gid = _validate_slug(id, label="group id")
    entry = {
        "id": gid,
        "name": (name or gid).strip(),
        "description": (description or "").strip(),
        "mappings": [],
    }
    with _lock:
        data = _load()
        for g in data["groups"]:
            if g.get("id") == gid:
                raise ValueError(f"分组 ID '{gid}' 已存在")
        data["groups"].append(entry)
        _save(data)
    return entry


def update_group(group_id: str, **fields: Any) -> dict[str, Any] | None:
    allowed = {"name", "description"}
    with _lock:
        data = _load()
        for g in data["groups"]:
            if g.get("id") == group_id:
                for k, v in fields.items():
                    if k in allowed and isinstance(v, str):
                        g[k] = v.strip()
                _save(data)
                return g
    return None


def delete_group(group_id: str) -> bool:
    with _lock:
        data = _load()
        before = len(data["groups"])
        data["groups"] = [g for g in data["groups"] if g.get("id") != group_id]
        if len(data["groups"]) == before:
            return False
        _save(data)
    return True


# ────────────── mapping CRUD ──────────────


def add_mapping(
    group_id: str,
    *,
    exposed_name: str,
    native_model: str,
    protocols: Any = None,
) -> dict[str, Any] | None:
    exposed = (exposed_name or "").strip()
    native = (native_model or "").strip()
    if not exposed:
        raise ValueError("exposed_name 不能为空")
    if not native:
        raise ValueError("native_model 不能为空")
    proto = _normalize_protocols(protocols)

    with _lock:
        data = _load()
        for g in data["groups"]:
            if g.get("id") == group_id:
                g.setdefault("mappings", [])
                mapping = {
                    "id": _next_mapping_id(g),
                    "exposed_name": exposed,
                    "native_model": native,
                    "protocols": proto,
                }
                g["mappings"].append(mapping)
                _save(data)
                return mapping
    return None


def update_mapping(
    group_id: str,
    mapping_id: str,
    **fields: Any,
) -> dict[str, Any] | None:
    with _lock:
        data = _load()
        for g in data["groups"]:
            if g.get("id") != group_id:
                continue
            for m in g.get("mappings") or []:
                if m.get("id") != mapping_id:
                    continue
                if "exposed_name" in fields and isinstance(fields["exposed_name"], str):
                    v = fields["exposed_name"].strip()
                    if v:
                        m["exposed_name"] = v
                if "native_model" in fields and isinstance(fields["native_model"], str):
                    v = fields["native_model"].strip()
                    if v:
                        m["native_model"] = v
                if "protocols" in fields:
                    m["protocols"] = _normalize_protocols(fields["protocols"])
                _save(data)
                return m
    return None


def delete_mapping(group_id: str, mapping_id: str) -> bool:
    with _lock:
        data = _load()
        for g in data["groups"]:
            if g.get("id") != group_id:
                continue
            mappings = g.get("mappings") or []
            before = len(mappings)
            g["mappings"] = [m for m in mappings if m.get("id") != mapping_id]
            if len(g["mappings"]) == before:
                return False
            _save(data)
            return True
    return False


# ────────────── resolution ──────────────


def resolve(exposed_name: str, protocol: str) -> str | None:
    """Return the first matching ``native_model`` for the given exposed name
    and protocol, scanning groups in declared order. ``None`` if no match."""
    if not exposed_name:
        return None
    for g in list_groups():
        for m in g.get("mappings") or []:
            if m.get("exposed_name") != exposed_name:
                continue
            if protocol not in (m.get("protocols") or []):
                continue
            native = m.get("native_model")
            if isinstance(native, str) and native:
                return native
    return None


def list_exposed_names(protocol: str) -> list[str]:
    """Return all unique exposed names that are enabled for ``protocol``,
    in first-occurrence order across groups."""
    out: list[str] = []
    seen: set[str] = set()
    for g in list_groups():
        for m in g.get("mappings") or []:
            if protocol not in (m.get("protocols") or []):
                continue
            name = m.get("exposed_name")
            if isinstance(name, str) and name and name not in seen:
                seen.add(name)
                out.append(name)
    return out


# ────────────── bulk import ──────────────


def import_from_backends(
    *,
    group_id: str = "mimo",
    group_name: str = "MiMo 原生",
    backends: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create (or top up) a group containing 1:1 mappings for every native
    model exposed by any backend. Idempotent: existing mappings are left
    alone, missing ones are added with both protocols enabled.

    Returns ``{group, added: int, skipped: int}``.
    """
    from gateway.backend_store import list_backends as _bs_list

    backends = backends if backends is not None else _bs_list()
    native_models: list[str] = []
    seen: set[str] = set()
    for b in backends:
        for m in b.get("models") or []:
            if isinstance(m, str) and m and m not in seen:
                seen.add(m)
                native_models.append(m)

    with _lock:
        data = _load()
        group = next((g for g in data["groups"] if g.get("id") == group_id), None)
        if group is None:
            _validate_slug(group_id, label="group id")
            group = {
                "id": group_id,
                "name": group_name or group_id,
                "description": "auto-imported from backends",
                "mappings": [],
            }
            data["groups"].append(group)
        existing = {m.get("exposed_name") for m in group.get("mappings") or []}
        added = 0
        for native in native_models:
            if native in existing:
                continue
            group.setdefault("mappings", []).append({
                "id": _next_mapping_id(group),
                "exposed_name": native,
                "native_model": native,
                "protocols": list(VALID_PROTOCOLS),
            })
            existing.add(native)
            added += 1
        _save(data)
    return {
        "group": group,
        "added": added,
        "skipped": len(native_models) - added,
    }


def ensure_default_initialized() -> None:
    """First-run helper: if the file is empty/missing, auto-import from
    existing backends so the gateway keeps routing without manual setup."""
    migrate_once()
    if _path().exists():
        return
    try:
        result = import_from_backends()
        if result["added"] == 0:
            # No backends yet — still write an empty file so future writes
            # don't keep checking the filesystem.
            with _lock:
                _save({"groups": [{
                    "id": "mimo",
                    "name": "MiMo 原生",
                    "description": "auto-created on first run",
                    "mappings": [],
                }]})
    except Exception:
        # Never block startup on this — store will be created lazily on first save.
        pass
