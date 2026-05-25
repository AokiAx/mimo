"""Aggregate account-related FastAPI routes."""
from __future__ import annotations

from fastapi import FastAPI

from claw.account_claw_routes import register_account_claw_routes
from claw.account_cookie_routes import register_account_cookie_routes
from claw.account_crud_routes import register_account_crud_routes
from claw.account_login_routes import register_account_login_routes


def register_account_routes(app: FastAPI, *, acurl, claw_ws_chat=None) -> None:
    """Attach account, cookie, login, and account-scoped Claw routes."""
    if claw_ws_chat is None:
        from claw.client import claw_ws_chat as default_claw_ws_chat

        claw_ws_chat = default_claw_ws_chat
    register_account_cookie_routes(app, acurl=acurl)
    register_account_login_routes(app, acurl=acurl)
    register_account_claw_routes(app, acurl=acurl, claw_ws_chat=claw_ws_chat)
    register_account_crud_routes(app, acurl=acurl)
