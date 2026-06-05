"""VPS probe registry — receives 10s reports from agent.py.

Stores nodes + latest snapshot in data/probe_nodes.json. Only the most
recent sample per node is kept (no history). A node is considered
offline when last_seen is older than OFFLINE_AFTER_S.

Token model: each node has a random token issued at add-time. The
agent sends it in the X-Probe-Token header. The token also doubles as
the node id internally so the URL never leaks the human-readable name.
"""
from __future__ import annotations

import secrets
import threading
import time

from gateway import config_store

OFFLINE_AFTER_S = 30  # 3x the default 10s report interval

_lock = threading.Lock()


def _empty():
    return {"nodes": {}}


def _load() -> dict:
    data = config_store.get_section("probe_nodes", None)
    return data if isinstance(data, dict) else _empty()


def _save(data: dict) -> None:
    config_store.set_section("probe_nodes", data)


def list_nodes(*, include_token: bool = False) -> list[dict]:
    """Return nodes with current online state, sorted by name."""
    with _lock:
        data = _load()
    now = time.time()
    out = []
    for node_id, n in data["nodes"].items():
        last_seen = n.get("last_seen", 0)
        online = bool(last_seen and (now - last_seen) < OFFLINE_AFTER_S)
        item = {
            "id": node_id,
            "name": n.get("name", ""),
            "added_at": n.get("added_at", 0),
            "last_seen": last_seen,
            "online": online,
            "sample": n.get("sample") if online else None,
        }
        if include_token:
            item["token"] = n.get("token", "")
        out.append(item)
    out.sort(key=lambda x: x["name"].lower())
    return out


def add_node(name: str) -> dict:
    """Create a node. Returns {id, name, token}."""
    name = (name or "").strip()
    if not name:
        raise ValueError("name 不能为空")
    token = secrets.token_urlsafe(24)
    node_id = secrets.token_hex(6)
    with _lock:
        data = _load()
        # Reject duplicate names so the panel UI stays unambiguous.
        for n in data["nodes"].values():
            if n.get("name") == name:
                raise ValueError(f"节点名 '{name}' 已存在")
        data["nodes"][node_id] = {
            "name": name,
            "token": token,
            "added_at": time.time(),
            "last_seen": 0,
            "sample": None,
        }
        _save(data)
    return {"id": node_id, "name": name, "token": token}


def delete_node(node_id: str) -> bool:
    with _lock:
        data = _load()
        if node_id not in data["nodes"]:
            return False
        del data["nodes"][node_id]
        _save(data)
    return True


def regenerate_token(node_id: str) -> str | None:
    with _lock:
        data = _load()
        if node_id not in data["nodes"]:
            return None
        token = secrets.token_urlsafe(24)
        data["nodes"][node_id]["token"] = token
        _save(data)
    return token


def ingest_report(token: str, name: str | None, sample: dict) -> bool:
    """Match by token, update last_seen + sample. Returns True on hit."""
    if not token or not isinstance(sample, dict):
        return False
    with _lock:
        data = _load()
        for node_id, n in data["nodes"].items():
            if n.get("token") == token:
                n["last_seen"] = time.time()
                n["sample"] = sample
                # Allow agent to update display name if it provides one
                # (only useful when initial add used a placeholder).
                if name and not n.get("name"):
                    n["name"] = name
                _save(data)
                return True
    return False
