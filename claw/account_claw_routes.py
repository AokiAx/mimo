"""Per-account Claw status and chat routes."""
from __future__ import annotations

import asyncio
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from claw import account_store
from claw.account_helpers import format_expire_time


def register_account_claw_routes(app: FastAPI, *, acurl, claw_ws_chat) -> None:
    """Attach per-account Claw routes to ``app``."""

    @app.get("/api/account/{filename}/cookie/status")
    async def account_cookie_status(filename: str):
        """Per-account cookie status."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"valid": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])
        if not cookies:
            return {"valid": False, "count": 0, "has_ph": False, "reason": "无 Cookie"}
        ph = None
        for c in cookies:
            if c["name"] == "xiaomichatbot_ph":
                ph = c["value"]
                break
        code, data = await acurl("GET", "/open-apis/user/mi/get", with_ph=False, cookies=cookies)
        valid = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        user_id = data.get("data", {}).get("userId", "") if isinstance(data, dict) else ""
        user_name = data.get("data", {}).get("userName", "") if isinstance(data, dict) else ""
        return {
            "valid": valid,
            "count": len(cookies),
            "has_ph": ph is not None,
            "test_code": code,
            "user_id": user_id,
            "user_name": user_name,
        }

    @app.get("/api/account/{filename}/claw/status")
    async def account_claw_status(filename: str):
        """Per-account claw status."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])
        code, data = await acurl(
            "GET", "/open-apis/user/mimo-claw/status", with_ph=False, cookies=cookies
        )
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            info = data.get("data", {})
            expire_ms = info.get("expireTime", 0)
            return {
                "success": True,
                "data": {
                    "status": info.get("status", ""),
                    "message": info.get("message", ""),
                    "expireTime": expire_ms,
                    "expireStr": format_expire_time(expire_ms),
                },
            }
        return {"success": False, "code": code, "data": data}

    @app.post("/api/account/{filename}/claw/create")
    async def account_claw_create(filename: str):
        """Create claw for a specific account."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])
        code, data = await acurl(
            "POST", "/open-apis/user/mimo-claw/create", body={}, cookies=cookies
        )
        success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        account_store.invalidate_summary(filename)
        return {"success": success, "code": code, "data": data}

    @app.post("/api/account/{filename}/claw/destroy")
    async def account_claw_destroy(filename: str):
        """Destroy claw for a specific account."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])
        code, data = await acurl(
            "POST", "/open-apis/user/mimo-claw/destroy", body={}, cookies=cookies
        )
        success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        account_store.invalidate_summary(filename)
        return {"success": success, "code": code, "data": data}

    @app.post("/api/account/{filename}/claw/refresh")
    async def account_claw_refresh(filename: str):
        """Destroy + recreate claw for an account."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])
        await acurl("POST", "/open-apis/user/mimo-claw/destroy", body={}, cookies=cookies)
        await asyncio.sleep(1.0)
        code, data = await acurl(
            "POST", "/open-apis/user/mimo-claw/create", body={}, cookies=cookies
        )
        success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        account_store.invalidate_summary(filename)
        return {"success": success, "code": code, "data": data}

    @app.get("/api/account/{filename}/summary")
    async def account_summary(filename: str):
        """Combined snapshot for one account."""
        cached = account_store.SUMMARY_CACHE.get(filename)
        if cached and time.time() - cached[0] < account_store.SUMMARY_TTL:
            return cached[1]

        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        cookies = acc.get("cookies", [])

        user_task = acurl("GET", "/open-apis/user/mi/get", with_ph=False, cookies=cookies)
        claw_task = acurl("GET", "/open-apis/user/mimo-claw/status", with_ph=False, cookies=cookies)
        (code_u, data_u), (code_c, data_c) = await asyncio.gather(user_task, claw_task)

        cookie_valid = code_u == "HTTP_200" and isinstance(data_u, dict) and data_u.get("code") == 0
        user_id = data_u.get("data", {}).get("userId", "") if isinstance(data_u, dict) else ""
        user_name = data_u.get("data", {}).get("userName", "") if isinstance(data_u, dict) else ""

        claw_status = "unknown"
        claw_expire_str = ""
        if code_c == "HTTP_200" and isinstance(data_c, dict) and data_c.get("code") == 0:
            info = data_c.get("data", {}) or {}
            claw_status = info.get("status", "unknown")
            claw_expire_str = format_expire_time(info.get("expireTime", 0))

        result = {
            "success": True,
            "filename": filename,
            "name": acc.get("name", filename),
            "email": acc.get("email", ""),
            "user_id": user_id or acc.get("user_id", ""),
            "user_name": user_name or acc.get("user_name", ""),
            "cookie_valid": cookie_valid,
            "cookie_count": len(cookies),
            "claw_status": claw_status,
            "claw_expire": claw_expire_str,
            "is_current": filename == account_store.get_current_account_name(),
        }
        if cookie_valid or claw_status != "unknown":
            account_store.SUMMARY_CACHE[filename] = (time.time(), result)
        return result

    @app.post("/api/account/{filename}/chat")
    async def account_chat(filename: str, request: Request):
        """Per-account claw chat (WebSocket-based, like /api/claw/chat but explicit)."""
        acc = account_store.load_account(filename)
        if not acc:
            return JSONResponse({"success": False, "error": "账号不存在"}, status_code=404)
        body = await request.json()
        message = body.get("message", "")
        session_key = body.get("session_key", "agent:main:deploy-" + uuid.uuid4().hex[:8])
        if not message:
            return JSONResponse({"error": "No message"}, status_code=400)

        text, err = await claw_ws_chat(message, session_key, cookies=acc.get("cookies", []))
        if err:
            return {"success": False, "error": err}
        return {"success": True, "reply": text}
