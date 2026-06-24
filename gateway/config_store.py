"""Single consolidated config file for the gateway + deploy stores.

Backends, panel ACL, auto-deploy schedule, SSH targets and the aistudio pin
used to live in separate data/*.json files. They are all small,
human-editable config, so we keep them as named sections of ONE file
(data/config.json) to cut down on the pile of runtime files while staying
hand-editable.

Each owning module keeps its existing ``_load()/_save()`` interface but
delegates the actual read/write to :func:`get_section` / :func:`set_section`
here. All writes go through a single lock + atomic temp-rename, so concurrent
section writes from different modules never clobber the file.

``model_groups.json`` is intentionally NOT folded in — model mapping is edited
often through the panel and must remain an independent runtime file.
``secrets.json`` is intentionally NOT folded in — it has its own env-locking /
rotation semantics and is more sensitive, so it stays a separate file.

Path is overridable via ``MIMO_CONFIG``.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_DEFAULT = Path(__file__).parent.parent / "data" / "config.json"
CONFIG_PATH = Path(os.environ.get("MIMO_CONFIG") or _DEFAULT)

# section name -> legacy standalone filename (relative to CONFIG_PATH's dir)
_LEGACY = {
    "backends": "backends.json",
    "panel_acl": "panel_acl.json",
    "auto_deploy": "auto_deploy.json",
    "ssh_targets": "ssh_targets.json",
    "pin": "pin_config.json",
}

_lock = threading.RLock()


def _read_all() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        log.exception("failed to read %s; treating as empty", CONFIG_PATH)
        return {}


def _write_all(data: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_name(f"{CONFIG_PATH.name}.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, CONFIG_PATH)


def migrate_once() -> None:
    """If config.json doesn't exist yet but the legacy per-feature JSONs do,
    fold each into a named section once, then back the legacy files up as .bak.
    No-op on fresh installs (sections created on first set) and on already
    consolidated installs."""
    with _lock:
        if CONFIG_PATH.exists():
            return
        data: dict[str, Any] = {}
        found = False
        for section, fn in _LEGACY.items():
            p = CONFIG_PATH.parent / fn
            if not p.exists():
                continue
            try:
                data[section] = json.loads(p.read_text(encoding="utf-8"))
                found = True
            except (json.JSONDecodeError, OSError):
                log.exception("skip malformed legacy config %s", p)
        if not found:
            return
        _write_all(data)
        for fn in _LEGACY.values():
            p = CONFIG_PATH.parent / fn
            if p.exists():
                p.rename(p.with_suffix(p.suffix + ".bak"))
        log.info("consolidated legacy config files into %s", CONFIG_PATH.name)


def get_section(name: str, default: Any = None) -> Any:
    with _lock:
        return _read_all().get(name, default)


def set_section(name: str, value: Any) -> None:
    with _lock:
        data = _read_all()
        data[name] = value
        _write_all(data)


def delete_section(name: str) -> bool:
    with _lock:
        data = _read_all()
        if name not in data:
            return False
        del data[name]
        _write_all(data)
        return True


# Fold legacy files in once, before any owning module reads its section.
migrate_once()
