"""Shared helpers for account-facing FastAPI routes."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from claw import account_store


async def fetch_user_info(acurl, cookies_list: list[dict[str, Any]]):
    """Fetch user info from MiMo API for a specific cookie list."""
    code, data = await acurl("GET", "/open-apis/user/mi/get", with_ph=False, cookies=cookies_list)
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {}) or {}
        return info.get("userId"), info.get("userName")
    return None, None


async def persist_login_cookies(
    acurl,
    filename: str,
    email: str,
    cookies: list[dict[str, Any]],
) -> dict[str, Any]:
    """Save cookies returned by SSO login into accounts/{filename}.json."""
    user_id, user_name = await fetch_user_info(acurl, cookies)
    existing = account_store.load_account(filename) or {}
    account = {
        "name": existing.get("name") or filename,
        "email": email or existing.get("email", ""),
        "cookies": cookies,
        "user_id": user_id or existing.get("user_id", ""),
        "user_name": user_name or existing.get("user_name", ""),
        "added_at": existing.get("added_at") or datetime.now(timezone.utc).isoformat(),
    }
    account_store.save_account(filename, account)
    if not account_store.get_current_account_name():
        account_store.set_current_account(filename)
    return account


def format_expire_time(expire_ms: int | float | str | None) -> str:
    """Format MiMo millisecond timestamps for API responses."""
    if not expire_ms:
        return ""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(expire_ms) / 1000))
    except Exception:
        return str(expire_ms)


def find_account_by_name_or_filename(name: str):
    """Resolve a user-visible account name to its stored filename and data."""
    fname = account_store.account_filename(name)
    acc = account_store.load_account(fname)
    if acc:
        return fname, acc
    for candidate in account_store.list_account_files():
        account = account_store.load_account(candidate)
        if account and account.get("name") == name:
            return candidate, account
    return fname, None
