"""FastAPI startup/shutdown lifecycle wiring."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI

logger = logging.getLogger(__name__)


async def startup_services(*, panel_password: str) -> None:
    """Start auto-deploy scheduler and gateway probe."""
    # Warn (don't refuse) on the well-known default panel password. The default
    # lives in ``gateway/secrets_store.py`` and ships in this public repo, so any
    # deployment that keeps it = open admin.
    if panel_password == "Aoki-MiMo":
        logger.warning(
            "[startup] SECURITY: panel password is the public default "
            "'Aoki-MiMo'. If this panel is reachable from the internet, "
            "anyone scanning can log in. Change it via data/secrets.json."
        )
    if os.environ.get("DISABLE_SCHEDULER") in ("1", "true", "yes"):
        logger.info("[startup] DISABLE_SCHEDULER set — auto-deploy scheduler not started")
    else:
        try:
            from claw.auto_deploy import start_scheduler

            start_scheduler()
        except Exception:
            logger.exception("[startup] Failed to start scheduler")
    try:
        from gateway.runtime import start_probe as start_router_probe

        start_router_probe()
    except Exception:
        logger.exception("[startup] Failed to start gateway probe")


async def shutdown_services() -> None:
    """Close shared resources on shutdown."""
    try:
        from claw.client import close_clients

        await close_clients()
    except Exception:
        logger.exception("[shutdown] Failed to close Claw client")
    try:
        from gateway.runtime import shutdown as shutdown_gateway_runtime

        await shutdown_gateway_runtime()
    except Exception:
        logger.exception("[shutdown] Failed to close gateway runtime")
    try:
        from gateway.auth import close_key_store

        close_key_store()
    except Exception:
        logger.exception("[shutdown] Failed to close gateway auth store")


def create_lifespan(*, panel_password: str):
    """Build the FastAPI lifespan context for app startup/shutdown."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await startup_services(panel_password=panel_password)
        try:
            yield
        finally:
            await shutdown_services()

    return lifespan


def register_lifecycle(app: FastAPI, *, panel_password: str) -> None:
    """Attach lifecycle wiring to an already-created FastAPI app."""
    app.router.lifespan_context = create_lifespan(panel_password=panel_password)
