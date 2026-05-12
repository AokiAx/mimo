"""
Gateway error hierarchy.

Each error carries an HTTP status and a stable error code; adapters serialize
them into the protocol-specific error envelope (Anthropic ``{type:"error"}``,
OpenAI ``{error:{...}}``).

Never raise ``GatewayError`` directly — pick the most specific subclass.
"""
from __future__ import annotations

from typing import Any


class GatewayError(Exception):
    """Base for all gateway-level errors. Maps to an HTTP response."""
    http_status: int = 500
    error_code: str = "gateway_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.error_code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return {"error": body}


class AuthError(GatewayError):
    http_status = 401
    error_code = "authentication_error"


class RateLimitError(GatewayError):
    http_status = 429
    error_code = "rate_limit_exceeded"


class BadRequestError(GatewayError):
    """Client payload failed validation (e.g. missing model)."""
    http_status = 400
    error_code = "invalid_request"


class ModelNotFoundError(GatewayError):
    """The requested model name is not configured in any mapping."""
    http_status = 404
    error_code = "model_not_found"


class UpstreamError(GatewayError):
    """Backend returned non-2xx, or transport raised."""
    http_status = 502
    error_code = "upstream_error"


class BackendUnavailableError(GatewayError):
    """All backends unhealthy / circuit-open."""
    http_status = 503
    error_code = "backend_unavailable"


class UpstreamTimeoutError(GatewayError):
    """Upstream did not respond within configured timeout."""
    http_status = 504
    error_code = "gateway_timeout"


class AdapterError(GatewayError):
    """Adapter could not parse or serialize a payload."""
    http_status = 500
    error_code = "adapter_error"
