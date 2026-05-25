"""Cookie status and browser-cookie sync routes."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI, Request

from claw import account_store
from claw.account_helpers import fetch_user_info


def register_account_cookie_routes(app: FastAPI, *, acurl) -> None:
    """Attach cookie-oriented account routes to ``app``."""

    @app.post("/api/cookie/refresh")
    async def cookie_refresh():
        name = account_store.get_current_account_name()
        acc = account_store.get_current_account()
        if not name or not acc:
            return {"success": False, "error": "无当前账号"}
        cookies = acc.get("cookies", [])
        user_id, user_name = await fetch_user_info(acurl, cookies)
        if user_id:
            acc["user_id"] = user_id
            acc["user_name"] = user_name or acc.get("user_name", "")
            account_store.save_account(name, acc)
            return {"success": True, "user_id": user_id, "user_name": user_name or ""}
        return {"success": False, "error": "Cookie 失效，请重新登录"}

    @app.get("/api/cookie/status")
    async def cookie_status():
        cookies = account_store.load_cookies()
        if not cookies:
            return {"valid": False, "count": 0, "reason": "No cookies file or empty"}
        ph = None
        for c in cookies:
            if c["name"] == "xiaomichatbot_ph":
                ph = c["value"]
                if ph.startswith('"') and ph.endswith('"'):
                    ph = ph[1:-1]
                break
        code, data = await acurl("GET", "/open-apis/user/mi/get", with_ph=False)
        valid = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        return {
            "valid": valid,
            "count": len(cookies),
            "has_ph": ph is not None,
            "test_code": code,
            "user_id": data.get("data", {}).get("userId", "") if isinstance(data, dict) else "",
        }

    @app.post("/api/accounts/sync-cookies")
    async def sync_cookies(request: Request):
        """Sync cookies from browser."""
        body = await request.json()
        cookie_str = body.get("cookies", "")
        if not cookie_str:
            return {"success": False, "error": "No cookies provided"}

        cookies = []
        for part in cookie_str.split(";"):
            part = part.strip()
            if "=" in part:
                name, value = part.split("=", 1)
                cookies.append({
                    "name": name.strip(),
                    "value": value.strip(),
                    "domain": ".xiaomimimo.com",
                    "path": "/",
                    "expires": -1,
                    "httpOnly": False,
                    "secure": False,
                    "sameSite": "None",
                })

        ph_found = any(c["name"] == "xiaomichatbot_ph" for c in cookies)
        if not ph_found:
            return {"success": False, "error": "缺少 xiaomichatbot_ph cookie，请确认已登录 MiMo"}

        user_id, user_name = await fetch_user_info(acurl, cookies)
        name = body.get("name", "default")
        email = body.get("email", "")
        fname = account_store.account_filename(name)
        account = {
            "name": name,
            "email": email,
            "cookies": cookies,
            "user_id": user_id or "",
            "user_name": user_name or "",
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
        account_store.save_account(fname, account)
        account_store.set_current_account(fname)
        return {
            "success": True,
            "user_id": user_id or "",
            "user_name": user_name or "",
            "cookie_count": len(cookies),
        }
