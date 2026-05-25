"""Aggregate FastAPI routes for the management panel."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from gateway.panel_backend_routes import register_panel_backend_routes
from gateway.panel_metrics_routes import register_panel_metrics_routes
from gateway.panel_model_group_routes import register_panel_model_group_routes
from gateway.panel_page_routes import register_panel_page_routes
from gateway.panel_probe_routes import register_panel_probe_routes


def register_panel_routes(
    app: FastAPI,
    *,
    base_dir: Path,
    log_dir: Path,
    probe_dir: Path,
) -> None:
    """Attach panel/admin routes to ``app``."""
    register_panel_page_routes(app, base_dir=base_dir, log_dir=log_dir)
    register_panel_backend_routes(app)
    register_panel_metrics_routes(app)
    register_panel_model_group_routes(app)
    register_panel_probe_routes(app, probe_dir=probe_dir)
