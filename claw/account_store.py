"""Account file store and cookie helpers.

The panel still stores accounts as JSON files under ``accounts/``. This module
keeps that filesystem contract in one place so routes and app bootstrap code do
not need to reach through ``app.py`` for account state.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from project_paths import ACCOUNTS_DIR, CURRENT_ACCOUNT_FILE

_ACCOUNTS_DIR_RESOLVED: Path | None = None
SUMMARY_CACHE: dict[str, tuple[float, dict]] = {}
SUMMARY_TTL = 20.0


def ensure_accounts_dir() -> None:
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)


def account_filename(name: str) -> str:
    """Sanitize account name to a safe filename."""
    safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", name).strip("_")
    return safe if safe else "unnamed"


def is_safe_account_filename(filename: str) -> bool:
    """True iff ``filename`` is a safe single-segment account id."""
    if not filename or not isinstance(filename, str):
        return False
    if any(c in filename for c in ("\x00", "/", "\\")):
        return False
    if filename in ("..", ".") or filename.startswith("."):
        return False
    global _ACCOUNTS_DIR_RESOLVED
    if _ACCOUNTS_DIR_RESOLVED is None:
        _ACCOUNTS_DIR_RESOLVED = ACCOUNTS_DIR.resolve()
    candidate = (ACCOUNTS_DIR / f"{filename}.json").resolve()
    try:
        candidate.relative_to(_ACCOUNTS_DIR_RESOLVED)
    except ValueError:
        return False
    return True


def get_current_account_name() -> str | None:
    if not CURRENT_ACCOUNT_FILE.exists():
        return None
    try:
        with open(CURRENT_ACCOUNT_FILE) as f:
            data = json.load(f)
        return data.get("current")
    except (json.JSONDecodeError, ValueError):
        return None


def set_current_account(filename: str) -> None:
    ensure_accounts_dir()
    with open(CURRENT_ACCOUNT_FILE, "w") as f:
        json.dump({"current": filename}, f)
    invalidate_summary()


def load_account(filename: str) -> dict[str, Any] | None:
    if not is_safe_account_filename(filename):
        return None
    path = account_path(filename)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


def save_account(filename: str, account_data: dict[str, Any]) -> None:
    if not is_safe_account_filename(filename):
        raise ValueError("invalid account filename")
    ensure_accounts_dir()
    with open(account_path(filename), "w") as f:
        json.dump(account_data, f, indent=2, ensure_ascii=False)
    invalidate_summary(filename)


def account_path(filename: str) -> Path:
    return ACCOUNTS_DIR / f"{filename}.json"


def list_account_files() -> list[str]:
    ensure_accounts_dir()
    result = []
    for p in sorted(ACCOUNTS_DIR.glob("*.json")):
        if p.name.startswith("_"):
            continue
        result.append(p.stem)
    return result


def get_current_account() -> dict[str, Any] | None:
    name = get_current_account_name()
    if name:
        return load_account(name)
    return None


def load_cookies() -> list[dict[str, Any]]:
    account = get_current_account()
    if account and isinstance(account, dict) and account.get("cookies"):
        return account["cookies"]
    return []


def cookie_parts_from(cookies: list[dict[str, Any]]) -> tuple[str, str | None]:
    parts = []
    ph = None
    for c in cookies:
        if "xiaomimimo" in c.get("domain", ""):
            val = c["value"]
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            parts.append("{0}={1}".format(c["name"], val))
            if c["name"] == "xiaomichatbot_ph":
                ph = val
    if not ph:
        for c in cookies:
            if c["name"] == "xiaomichatbot_ph":
                ph = c["value"]
                if ph.startswith('"') and ph.endswith('"'):
                    ph = ph[1:-1]
                break
    return "; ".join(parts), ph


def cookie_header_all_from(cookies: list[dict[str, Any]]) -> str:
    parts = []
    for c in cookies:
        val = c["value"]
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        parts.append("{0}={1}".format(c["name"], val))
    return "; ".join(parts)


def invalidate_summary(filename: str | None = None) -> None:
    if filename is None:
        SUMMARY_CACHE.clear()
    else:
        SUMMARY_CACHE.pop(filename, None)
