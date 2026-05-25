"""FastAPI application factory."""
from __future__ import annotations

import logging

from fastapi import FastAPI

from app_compat import _audit, _is_safe_account_filename, acurl, claw_ws_chat
from app_lifecycle import create_lifespan
from claw.account_routes import register_account_routes
from claw.auto_deploy_routes import register_auto_deploy_routes
from claw.management_routes import register_claw_management_routes
from gateway.logging_setup import setup_logging
from gateway.panel_auth import register_panel_auth
from gateway.panel_routes import register_panel_routes
from gateway.routes import register_gateway_routes
from gateway.secrets_store import secrets
from project_paths import BASE_DIR, PROBE_DIR

AUTH_COOKIE = "mimo_panel_auth"
AUTH_TOKEN = secrets.public_api_token
PANEL_PASSWORD = secrets.panel_password

LOG_DIR = setup_logging(BASE_DIR)
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Build and wire the FastAPI application."""
    app = FastAPI(title="MiMo Manager", lifespan=create_lifespan(panel_password=PANEL_PASSWORD))

    register_account_routes(app, acurl=acurl, claw_ws_chat=claw_ws_chat)
    register_claw_management_routes(app, acurl=acurl, claw_ws_chat=claw_ws_chat)
    register_auto_deploy_routes(app)
    register_panel_routes(app, base_dir=BASE_DIR, log_dir=LOG_DIR, probe_dir=PROBE_DIR)
    register_gateway_routes(app, auth_cookie=AUTH_COOKIE)
    register_panel_auth(
        app,
        auth_cookie=AUTH_COOKIE,
        auth_token=AUTH_TOKEN,
        panel_password=PANEL_PASSWORD,
        audit_fn=_audit,
        safe_filename_fn=_is_safe_account_filename,
    )
    return app
