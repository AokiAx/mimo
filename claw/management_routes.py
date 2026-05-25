"""FastAPI routes for current Claw status and ad-hoc chat."""
from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from claw import account_store
from project_paths import ACCOUNTS_DIR


def register_claw_management_routes(app: FastAPI, *, acurl, claw_ws_chat) -> None:
    """Attach current-account Claw management routes to ``app``."""

    @app.get("/api/status")
    async def api_status():
        result = {"claw": {"status": "unknown"}, "cookies": {"status": "unknown"}}

        cookies = account_store.load_cookies()
        ph_found = any(c["name"] == "xiaomichatbot_ph" for c in cookies)
        current_name = account_store.get_current_account_name()
        current_file = (ACCOUNTS_DIR / f"{current_name}.json") if current_name else None
        result["cookies"] = {
            "status": "ok" if ph_found and cookies else "error",
            "count": len(cookies),
            "has_ph": ph_found,
            "file_exists": bool(current_file and current_file.exists()),
        }

        try:
            code, data = await acurl("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
            if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
                info = data.get("data", {})
                result["claw"] = {
                    "status": "ok" if info.get("status") == "AVAILABLE" else info.get("status", "unknown").lower(),
                    "raw_status": info.get("status", ""),
                    "message": info.get("message", ""),
                    "expire_time": info.get("expireTime", 0),
                }
            else:
                result["claw"] = {"status": "error", "detail": str(data)[:200]}
        except Exception as e:
            result["claw"] = {"status": "error", "detail": str(e)[:200]}

        return result

    @app.post("/api/claw/create")
    async def claw_create():
        code, data = await acurl("POST", "/open-apis/user/mimo-claw/create", body={})
        success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        return {"success": success, "code": code, "data": data}

    @app.get("/api/claw/status")
    async def claw_status():
        code, data = await acurl("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            info = data.get("data", {})
            expire_ms = info.get("expireTime", 0)
            expire_str = ""
            if expire_ms:
                try:
                    expire_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_ms / 1000))
                except Exception:
                    expire_str = str(expire_ms)
            return {"success": True, "data": {
                "status": info.get("status", ""),
                "message": info.get("message", ""),
                "expireTime": expire_ms,
                "expireStr": expire_str,
            }}
        return {"success": False, "code": code, "data": data}

    @app.post("/api/claw/destroy")
    async def claw_destroy():
        code, data = await acurl("POST", "/open-apis/user/mimo-claw/destroy", body={})
        success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
        return {"success": success, "code": code, "data": data}

    @app.post("/api/claw/chat")
    async def claw_chat(request: Request):
        body = await request.json()
        message = body.get("message", "")
        session_key = body.get("session_key", "agent:main:deploy-" + uuid.uuid4().hex[:8])
        if not message:
            return JSONResponse({"error": "No message"}, status_code=400)
        text, err = await claw_ws_chat(message, session_key)
        if err:
            return {"success": False, "error": err}
        return {"success": True, "reply": text}
