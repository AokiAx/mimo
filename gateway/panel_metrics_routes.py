"""Gateway metrics routes for the panel and public stats API."""
from __future__ import annotations

from fastapi import FastAPI


def register_panel_metrics_routes(app: FastAPI) -> None:
    """Attach gateway metrics routes."""

    @app.get("/api/gateway/metrics")
    async def gateway_metrics():
        """Request metrics for the metrics page."""
        try:
            from gateway.metrics import get_metrics_summary
            return get_metrics_summary()
        except ImportError:
            return {"error": "Gateway module not installed"}

    @app.get("/api/gateway/metrics/hourly")
    async def gateway_metrics_hourly(hours: int = 24):
        """24h request histogram (or N hours), oldest bucket first."""
        try:
            from gateway.metrics import get_hourly_buckets
            return {"buckets": get_hourly_buckets(hours=max(1, min(int(hours), 168)))}
        except ImportError:
            return {"buckets": []}

    @app.get("/api/gateway/metrics/backends")
    async def gateway_metrics_backends(hours: int = 24):
        """Per-backend stats over the window."""
        try:
            from gateway.metrics import get_backend_stats
            return {"backends": get_backend_stats(hours=max(1, min(int(hours), 168)))}
        except ImportError:
            return {"backends": []}

    @app.get("/api/gateway/metrics/status")
    async def gateway_metrics_status(hours: int = 24):
        """HTTP status-code distribution."""
        try:
            from gateway.metrics import get_status_distribution
            return {"distribution": get_status_distribution(hours=max(1, min(int(hours), 168)))}
        except ImportError:
            return {"distribution": {}}

    @app.get("/api/public/stats")
    async def public_stats():
        """All-time totals — safe to expose without auth."""
        try:
            from gateway.metrics import get_public_totals
            return get_public_totals()
        except ImportError:
            return {"total_requests": 0, "total_tokens": 0}
