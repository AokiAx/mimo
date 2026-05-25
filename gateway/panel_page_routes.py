"""Dashboard and log viewer routes."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from gateway.logging_setup import list_log_files, read_log_tail


def register_panel_page_routes(
    app: FastAPI,
    *,
    base_dir: Path,
    log_dir: Path,
) -> None:
    """Attach dashboard, log, and public stats pages."""

    @app.get("/", response_class=HTMLResponse)
    async def index():
        html_file = base_dir / "templates" / "index.html"
        return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

    def _log_retention_days() -> int:
        try:
            return max(1, int(os.environ.get("MIMO_LOG_RETENTION_DAYS", "14")))
        except ValueError:
            return 14

    @app.get("/api/logs")
    async def api_logs_list():
        """List application log files available to the authenticated panel."""
        return {
            "log_dir": str(log_dir),
            "files": list_log_files(log_dir),
            "retention_days": _log_retention_days(),
        }

    @app.get("/api/logs/tail")
    async def api_logs_tail(file: str = "error.log", lines: int = 300):
        """Return the tail of a selected log file for troubleshooting."""
        try:
            return {"success": True, "file": file, "content": read_log_tail(log_dir, file, lines=lines)}
        except FileNotFoundError:
            return JSONResponse({"success": False, "error": "Log file not found"}, status_code=404)
        except ValueError as e:
            return JSONResponse({"success": False, "error": str(e)}, status_code=400)

    @app.get("/stats", response_class=HTMLResponse)
    async def public_stats_page():
        """Public stats page — no auth, safe to share."""
        html_file = base_dir / "templates" / "stats.html"
        return HTMLResponse(content=html_file.read_text(encoding="utf-8"))
