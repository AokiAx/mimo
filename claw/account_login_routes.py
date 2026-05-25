"""Account SSO login routes."""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from claw.account_helpers import persist_login_cookies


def register_account_login_routes(app: FastAPI, *, acurl) -> None:
    """Attach account login routes to ``app``."""

    @app.post("/api/account/{filename}/login")
    async def account_login(filename: str, request: Request):
        """Start SSO login for an account."""
        from claw.mimo_auth import web_start_login

        body = await request.json()
        email = (body.get("email") or "").strip()
        password = body.get("password") or ""
        if not email or not password:
            return JSONResponse({"status": "error", "error": "缺少 email 或 password"}, status_code=400)
        result = await asyncio.to_thread(web_start_login, email, password)
        if result.get("status") == "ok":
            acc = await persist_login_cookies(acurl, filename, email, result["cookies"])
            result["filename"] = filename
            result["user_id"] = acc.get("user_id", "")
            result["user_name"] = acc.get("user_name", "")
        return result

    @app.post("/api/account/{filename}/login/verify")
    async def account_login_verify(filename: str, request: Request):
        """Submit 2FA verification code."""
        from claw.mimo_auth import web_submit_code

        body = await request.json()
        session_id = body.get("session_id", "")
        code = body.get("code", "")
        email = (body.get("email") or "").strip()
        if not session_id or not code:
            return JSONResponse({"status": "error", "error": "缺少 session_id 或 code"}, status_code=400)
        result = await asyncio.to_thread(web_submit_code, session_id, code)
        if result.get("status") == "ok":
            acc = await persist_login_cookies(acurl, filename, email, result["cookies"])
            result["filename"] = filename
            result["user_id"] = acc.get("user_id", "")
            result["user_name"] = acc.get("user_name", "")
        return result
