#!/usr/bin/env python3
"""
MiMo Claw/API Management Dashboard - FastAPI Backend
"""
from __future__ import annotations

from app_compat import *  # noqa: F403 - re-export legacy app.py helpers
from app_factory import (
    AUTH_COOKIE,
    AUTH_TOKEN,
    LOG_DIR,
    PANEL_PASSWORD,
    create_app,
)

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8088)
