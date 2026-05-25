"""Account list, add, switch, and delete routes."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from claw import account_store
from claw.account_helpers import fetch_user_info, find_account_by_name_or_filename
from project_paths import CURRENT_ACCOUNT_FILE


def register_account_crud_routes(app: FastAPI, *, acurl) -> None:
    """Attach account CRUD routes to ``app``."""

    @app.get("/api/accounts")
    async def accounts_list():
        """List all accounts."""
        current_name = account_store.get_current_account_name()
        acc_files = account_store.list_account_files()
        result = []
        for fname in acc_files:
            acc = account_store.load_account(fname)
            if acc and isinstance(acc, dict):
                result.append({
                    "filename": fname,
                    "name": acc.get("name", fname),
                    "email": acc.get("email", ""),
                    "user_id": acc.get("user_id", ""),
                    "user_name": acc.get("user_name", ""),
                    "added_at": acc.get("added_at", ""),
                    "is_current": fname == current_name,
                })
        return {"accounts": result, "current": current_name}

    @app.get("/api/accounts/current")
    async def accounts_current():
        """Get current account info."""
        acc = account_store.get_current_account()
        if not acc:
            return {"success": False, "error": "No current account set"}
        current_name = account_store.get_current_account_name()
        return {
            "success": True,
            "filename": current_name,
            "name": acc.get("name", ""),
            "email": acc.get("email", ""),
            "user_id": acc.get("user_id", ""),
            "user_name": acc.get("user_name", ""),
            "added_at": acc.get("added_at", ""),
        }

    @app.post("/api/accounts/add")
    async def accounts_add(request: Request):
        """Add a new account."""
        body = await request.json()
        name = body.get("name", "").strip()
        cookies = body.get("cookies", [])
        if not name:
            return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)
        if not cookies or not isinstance(cookies, list):
            return JSONResponse({"success": False, "error": "Cookies array is required"}, status_code=400)

        fname = account_store.account_filename(name)
        if account_store.account_path(fname).exists():
            return JSONResponse({"success": False, "error": f"Account '{name}' already exists"}, status_code=409)

        user_id, user_name = await fetch_user_info(acurl, cookies)
        account = {
            "name": name,
            "email": body.get("email", ""),
            "cookies": cookies,
            "user_id": user_id or "",
            "user_name": user_name or "",
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
        account_store.save_account(fname, account)
        if not account_store.get_current_account_name():
            account_store.set_current_account(fname)
        return {
            "success": True,
            "filename": fname,
            "name": name,
            "user_id": user_id or "",
            "user_name": user_name or "",
            "is_current": fname == account_store.get_current_account_name(),
        }

    @app.post("/api/accounts/switch")
    async def accounts_switch(request: Request):
        """Switch active account."""
        body = await request.json()
        name = body.get("name", "").strip()
        if not name:
            return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)

        fname, acc = find_account_by_name_or_filename(name)
        if not acc:
            return JSONResponse({"success": False, "error": "Account not found"}, status_code=404)

        account_store.set_current_account(fname)
        return {
            "success": True,
            "filename": fname,
            "name": acc.get("name", ""),
            "user_id": acc.get("user_id", ""),
            "user_name": acc.get("user_name", ""),
        }

    @app.post("/api/accounts/delete")
    async def accounts_delete(request: Request):
        """Delete an account."""
        body = await request.json()
        name = body.get("name", "").strip()
        if not name:
            return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)

        fname, _acc = find_account_by_name_or_filename(name)
        path = account_store.account_path(fname)
        if not path.exists():
            return JSONResponse({"success": False, "error": "Account not found"}, status_code=404)

        was_current = fname == account_store.get_current_account_name()
        path.unlink()

        if was_current:
            remaining = account_store.list_account_files()
            if remaining:
                account_store.set_current_account(remaining[0])
            elif CURRENT_ACCOUNT_FILE.exists():
                CURRENT_ACCOUNT_FILE.unlink()

        return {"success": True, "deleted": fname, "was_current": was_current}
