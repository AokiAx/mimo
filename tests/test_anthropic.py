"""Unit tests for gateway.adapters.anthropic.

The Anthropic adapter is a client-facing shell: ``/v1/messages`` requests go
through the native byte-passthrough in ``GatewayHandler`` (see
``test_anthropic_passthrough.py``), NOT the IES conversion path. So this adapter
only exposes routing identity + the Anthropic error envelope; its
``parse_request`` / ``serialize_response*`` methods are stubbed and never
invoked. These tests cover the live surfaces only.
"""
from __future__ import annotations

import json

import pytest

from gateway.adapters.anthropic import AnthropicAdapter
from gateway.core import AuthError, BadRequestError, RateLimitError, UpstreamError


def _adapter() -> AnthropicAdapter:
    return AnthropicAdapter()


# ───────── matches_path ─────────


def test_matches_path():
    assert AnthropicAdapter.matches_path("/v1/messages")
    assert AnthropicAdapter.matches_path("/anthropic/v1/messages")
    assert not AnthropicAdapter.matches_path("/v1/chat/completions")


# ───────── error_envelope ─────────


def test_error_envelope_anthropic_shape():
    payload = json.loads(_adapter().error_envelope(BadRequestError("bad")).decode())
    assert payload == {
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "bad"},
    }


def test_error_envelope_maps_codes_to_anthropic_types():
    cases = [
        (AuthError("x"), "authentication_error"),
        (RateLimitError("x"), "rate_limit_error"),
        (UpstreamError("x"), "api_error"),
    ]
    for err, expected_type in cases:
        payload = json.loads(_adapter().error_envelope(err).decode())
        assert payload["error"]["type"] == expected_type


# ───────── IES conversion is stubbed (passthrough only) ─────────


def test_ies_conversion_methods_are_not_implemented():
    a = _adapter()
    with pytest.raises(NotImplementedError):
        a.parse_request({"model": "claude-3-5-sonnet", "max_tokens": 8, "messages": []})
    with pytest.raises(NotImplementedError):
        a.serialize_response([])
