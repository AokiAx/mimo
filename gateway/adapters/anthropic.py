"""
Anthropic Messages API adapter — client-facing shell.

Endpoint: ``/v1/messages``. Anthropic requests do NOT go through the IES
conversion path: ``GatewayHandler.handle`` short-circuits ``adapter.name ==
"anthropic"`` to a native byte-passthrough against MiMo's
``/anthropic/v1/messages`` (see ``gateway.handler`` and
``gateway.anthropic_passthrough``), so ``thinking`` blocks and ``signature``
round-trip untouched and we avoid a lossy Anthropic⇄IES conversion.

This adapter therefore only provides the surfaces the runtime still uses for
Anthropic:
  * ``name`` / ``matches_path`` — routing identity
  * ``error_envelope`` — Anthropic-shaped error JSON

The IES request/response conversion methods required by ``ProtocolAdapter`` are
intentionally stubbed: the passthrough short-circuit means they are never
reached for this adapter.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from gateway.core import GatewayError, InternalEvent, InternalRequest

from .base import ProtocolAdapter


# ────────────── Anthropic-specific error type names ──────────────

_GATEWAY_TO_ANTHROPIC_ERR_TYPE: dict[str, str] = {
    "invalid_request": "invalid_request_error",
    "authentication_error": "authentication_error",
    "rate_limit_exceeded": "rate_limit_error",
    "upstream_error": "api_error",
    "backend_unavailable": "overloaded_error",
    "gateway_timeout": "api_error",
    "adapter_error": "api_error",
    "gateway_error": "api_error",
}

_NATIVE_ONLY = (
    "Anthropic requests use the native byte-passthrough path "
    "(GatewayHandler._handle_anthropic_native); the IES conversion methods are "
    "never invoked for this adapter."
)


class AnthropicAdapter(ProtocolAdapter):
    """Anthropic Messages: ``/v1/messages`` (native passthrough; see module doc)."""

    name = "anthropic"

    @classmethod
    def matches_path(cls, path: str) -> bool:
        return path.endswith("/messages")

    # ============ IES conversion — never reached (passthrough) ============

    def parse_request(self, body: dict[str, Any]) -> InternalRequest:
        raise NotImplementedError(_NATIVE_ONLY)

    def serialize_response_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError(_NATIVE_ONLY)

    def serialize_response(self, events: list[InternalEvent]) -> bytes:
        raise NotImplementedError(_NATIVE_ONLY)

    # ============ Error envelope ============

    def error_envelope(self, err: GatewayError) -> bytes:
        return json.dumps({
            "type": "error",
            "error": {
                "type": _GATEWAY_TO_ANTHROPIC_ERR_TYPE.get(err.error_code, "api_error"),
                "message": err.message,
            },
        }).encode()
