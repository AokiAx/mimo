"""Compatibility exports historically provided by ``app.py``.

Keep these names importable from ``app`` while the real implementations live in
focused modules.
"""
from __future__ import annotations

import json
import logging
import os

from fastapi import Request

from claw import account_store
from claw import client as claw_client
from project_paths import ACCOUNTS_DIR

__all__ = [
    "MIMO_BASE",
    "_SUMMARY_TTL",
    "_account_filename",
    "_afetch_user_info",
    "_audit",
    "_cookie_header_all_from",
    "_cookie_parts_from",
    "_ensure_accounts_dir",
    "_get_current_account",
    "_get_current_account_name",
    "_get_http_client",
    "_get_sync_http_client",
    "_invalidate_summary",
    "_is_safe_account_filename",
    "_list_account_files",
    "_load_account_by_filename",
    "_save_account",
    "_set_current_account",
    "_summary_cache",
    "acurl",
    "claw_ws_chat",
    "curl_api",
    "get_cookie_header_all",
    "get_cookie_parts",
    "get_ph_encoded",
    "load_cookies",
    "switch_to_account",
    "upload_to_claw_fds",
]

MIMO_BASE = claw_client.MIMO_BASE

logger = logging.getLogger("app")


def _ensure_accounts_dir():
    """Make sure accounts directory exists."""
    account_store.ensure_accounts_dir()


def _account_filename(name):
    """Sanitize account name to a safe filename."""
    return account_store.account_filename(name)


def _is_safe_account_filename(filename: str) -> bool:
    """True iff ``filename`` is a safe single-segment account id."""
    return account_store.is_safe_account_filename(filename)


def _client_ip(request: Request) -> str:
    """Best-effort client IP."""
    if os.environ.get("MIMO_TRUST_PROXY_HEADERS") in ("1", "true", "yes"):
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            # First entry is the originating client per RFC 7239 convention.
            return xff.split(",")[0].strip() or (request.client.host if request.client else "?")
    return request.client.host if request.client else "?"


def _audit(event: str, request: Request, **extra) -> None:
    """Append a structured audit-log line for security-relevant events."""
    parts = [f"audit event={event}"]
    parts.append(f"ip={_client_ip(request)}")
    parts.append(f"ua={request.headers.get('user-agent', '?')[:200]!r}")
    parts.append(f"path={request.url.path}")
    for k, v in extra.items():
        parts.append(f"{k}={v!r}")
    logger.warning(" ".join(parts))


def _get_current_account_name():
    """Read the current account name from _current.json."""
    return account_store.get_current_account_name()


def _set_current_account(filename):
    """Set the current active account by filename (without .json)."""
    account_store.set_current_account(filename)


def _load_account_by_filename(filename):
    """Load an account dict by its filename (without .json)."""
    if not _is_safe_account_filename(filename):
        logger.warning("blocked unsafe account read filename=%r", filename)
        return None
    return account_store.load_account(filename)


def _save_account(filename, account_data):
    """Save an account dict to its file."""
    if not _is_safe_account_filename(filename):
        logger.error("blocked unsafe account write filename=%r", filename)
        raise ValueError("invalid account filename")
    account_store.save_account(filename, account_data)


def _list_account_files():
    """Return list of account filenames (without .json) in accounts dir."""
    return account_store.list_account_files()


async def _afetch_user_info(cookies_list):
    """Fetch user info from MiMo API given a cookies list."""
    code, data = await acurl("GET", "/open-apis/user/mi/get", with_ph=False, cookies=cookies_list)
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {}) or {}
        return info.get("userId"), info.get("userName")
    return None, None


def _get_current_account():
    """Get the current account dict, or None if no current account."""
    return account_store.get_current_account()


def load_cookies():
    """Load cookies from current account."""
    return account_store.load_cookies()


def _cookie_parts_from(cookies):
    """Return (cookie_header_str, ph_value) for xiaomimimo domain cookies."""
    return account_store.cookie_parts_from(cookies)


def _cookie_header_all_from(cookies):
    """Build cookie header from ALL cookies."""
    return account_store.cookie_header_all_from(cookies)


def get_cookie_parts():
    return claw_client.get_cookie_parts()


def get_ph_encoded():
    return claw_client.get_ph_encoded()


def get_cookie_header_all():
    return claw_client.get_cookie_header_all()


_summary_cache = account_store.SUMMARY_CACHE
_SUMMARY_TTL = account_store.SUMMARY_TTL


def _invalidate_summary(filename: str | None = None) -> None:
    """Drop one or all entries from the summary cache."""
    account_store.invalidate_summary(filename)


def _get_http_client():
    return claw_client._get_http_client()


def _get_sync_http_client():
    """Sync httpx client for callers running outside the FastAPI event loop."""
    return claw_client._get_sync_http_client()


def curl_api(method, path, body=None, with_ph=True, cookies=None):
    """Sync MiMo API call for background threads."""
    return claw_client.curl_api(method, path, body=body, with_ph=with_ph, cookies=cookies)


async def acurl(method, path, body=None, with_ph=True, cookies=None):
    """Call MiMo API via shared httpx.AsyncClient."""
    return await claw_client.acurl(method, path, body=body, with_ph=with_ph, cookies=cookies)


async def upload_to_claw_fds(
    filename: str,
    content: bytes,
    cookies: list | None = None,
    file_type: str = "txt",
) -> tuple[dict | None, str | None]:
    """Upload ``content`` to MiMo's Galaxy FDS so Claw can fetch it."""
    return await claw_client.upload_to_claw_fds(
        filename,
        content,
        cookies=cookies,
        file_type=file_type,
    )


async def claw_ws_chat(
    message: str,
    session_key: str | None = None,
    cookies: list | None = None,
    attachments: list[dict] | None = None,
) -> tuple[str, str | None]:
    """Send a message to Claw over the WS gateway and return ``(reply, error)``."""
    return await claw_client.claw_ws_chat(
        message,
        session_key=session_key,
        cookies=cookies,
        attachments=attachments,
    )


def switch_to_account(account_filename: str) -> bool:
    """Programmatically switch to an account. Returns True on success."""
    account_file = ACCOUNTS_DIR / f"{account_filename}.json"
    if not account_file.exists():
        return False
    try:
        data = json.loads(account_file.read_text(encoding="utf-8"))
        cookies = data.get("cookies", [])
        if not cookies:
            return False
        account_store.set_current_account(account_filename)
        return True
    except Exception as e:
        logger.exception("[switch_to_account] Error")
        return False
