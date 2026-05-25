"""Gateway backend management routes for the panel."""
from __future__ import annotations

from fastapi import FastAPI, Request


def register_panel_backend_routes(app: FastAPI) -> None:
    """Attach gateway backend management routes."""

    @app.get("/api/gateway/status")
    async def gateway_status():
        """Gateway status overview for dashboard."""
        try:
            from gateway.runtime import get_router_status
            return get_router_status()
        except ImportError:
            return {"error": "Gateway module not installed"}

    @app.get("/api/gateway/backends")
    async def gateway_backends():
        """List all backend servers with health/routing info."""
        try:
            from gateway.runtime import get_all_backends
            return {"backends": get_all_backends()}
        except ImportError:
            return {"backends": []}

    @app.post("/api/gateway/backends/{backend_id}/toggle")
    async def gateway_backend_toggle(backend_id: str):
        """Enable/disable a backend."""
        try:
            from gateway.runtime import toggle_backend
            result = toggle_backend(backend_id)
            return result
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}

    @app.post("/api/gateway/backends/{backend_id}/activate")
    async def gateway_backend_activate(backend_id: str):
        """Hard-switch traffic to a backend and drain peers serving the same models."""
        try:
            from gateway.runtime import activate_backend
            return activate_backend(backend_id)
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}

    @app.post("/api/gateway/backends/reload")
    async def gateway_backends_reload():
        """Re-read backends.json and rebuild the backend registry."""
        try:
            from gateway.runtime import reload_backends
            count = reload_backends()
            return {"success": True, "backends": count}
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}

    @app.post("/api/gateway/backends/add")
    async def gateway_backend_add(request: Request):
        """Add a new backend server."""
        body = await request.json()
        try:
            from gateway.backend_store import add_backend
            from gateway.runtime import reload_backends
            entry = add_backend(
                name=body.get("name", ""),
                base_url=body.get("base_url", ""),
                models=body.get("models") if body.get("models") is not None else body.get("model", ""),
                api_key=body.get("api_key", ""),
                aliases=body.get("aliases", ""),
                weight=body.get("weight", 1),
                account_id=body.get("account_id", ""),
            )
            reload_backends()
            return {"success": True, "backend": entry}
        except ValueError as e:
            return {"success": False, "error": str(e)}
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}

    @app.post("/api/gateway/backends/{backend_id}/update")
    async def gateway_backend_update(backend_id: str, request: Request):
        """Update a backend's config."""
        body = await request.json()
        try:
            from gateway.backend_store import update_backend
            from gateway.runtime import reload_backends
            entry = update_backend(backend_id, **body)
            if entry is None:
                return {"success": False, "error": f"Backend {backend_id!r} not found"}
            reload_backends()
            return {"success": True, "backend": entry}
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}

    @app.post("/api/gateway/backends/{backend_id}/delete")
    async def gateway_backend_delete(backend_id: str):
        """Delete a backend."""
        try:
            from gateway.backend_store import delete_backend
            from gateway.runtime import reload_backends
            ok = delete_backend(backend_id)
            if not ok:
                return {"success": False, "error": f"Backend {backend_id!r} not found"}
            reload_backends()
            return {"success": True}
        except ImportError:
            return {"success": False, "error": "Gateway module not installed"}
