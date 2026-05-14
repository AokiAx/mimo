"""FastAPI routes for the gateway data plane.

This module keeps the bulky /v1 proxy, CORS, and public health endpoints out of
``app.py`` while preserving the same external routes.
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from gateway.auth import authenticate_gateway_request
from gateway.core import AuthError

_GATEWAY_PATHS = {"/v1/chat/completions", "/v1/messages", "/v1/responses", "/v1/models"}


def register_gateway_routes(app: FastAPI, *, auth_cookie: str) -> None:
    """Attach gateway routes to ``app``."""

    @app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def gateway_proxy(request: Request, path: str):
        """Proxy OpenAI-compatible requests through the gateway router."""
        full_path = f"/v1/{path}"

        # /v1/models remains public for compatibility with OpenAI SDK probes.
        if full_path == "/v1/models" and request.method == "GET":
            try:
                from gateway.model_groups_store import list_exposed_names, ensure_default_initialized
                ensure_default_initialized()
                names = list_exposed_names("openai")
                if not names:
                    from gateway.runtime import get_all_backends
                    seen: set[str] = set()
                    for b in get_all_backends():
                        for m in b.get("models") or []:
                            if m and m not in seen:
                                seen.add(m)
                                names.append(m)
                if not names:
                    names = ["mimo-v2.5-pro"]
                return {
                    "object": "list",
                    "data": [{"id": m, "object": "model", "owned_by": "mimo"} for m in names],
                }
            except ImportError:
                return {"object": "list", "data": [
                    {"id": "mimo-v2.5-pro", "object": "model", "owned_by": "mimo"},
                ]}

        if full_path not in _GATEWAY_PATHS:
            return JSONResponse({"error": {"message": f"Unknown path: {full_path}"}}, status_code=404)

        adapter_name = "openai_chat"
        if full_path == "/v1/messages":
            adapter_name = "anthropic"
        elif full_path == "/v1/responses":
            adapter_name = "openai_responses"

        try:
            principal = await authenticate_gateway_request(request, auth_cookie=auth_cookie)
            request.state.gateway_principal = principal
            from gateway.runtime import dispatch
            return await dispatch(adapter_name, request)
        except AuthError as e:
            return JSONResponse(
                {"error": {"message": e.message, "type": e.error_code, "code": e.error_code}},
                status_code=e.http_status,
            )
        except ImportError:
            return JSONResponse(
                {"error": {"message": "Gateway module not installed"}},
                status_code=503,
            )

    @app.options("/v1/{path:path}")
    async def gateway_cors_preflight(path: str):
        """Handle CORS preflight for gateway routes."""
        return HTMLResponse(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Max-Age": "86400",
                "Content-Length": "0",
            },
        )

    @app.get("/health")
    async def gateway_health():
        """Public health endpoint for the gateway."""
        try:
            from gateway.runtime import get_router_status
            status = get_router_status()
            return {"status": "ok", **status}
        except ImportError:
            return {"status": "ok", "note": "Gateway module not installed"}

    @app.get("/gateway/status")
    async def gateway_status_page():
        """Public gateway status (no auth required, for monitoring)."""
        try:
            from gateway.runtime import get_router_status
            return get_router_status()
        except ImportError:
            return {"error": "Gateway module not installed"}
