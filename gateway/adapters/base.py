"""
ProtocolAdapter ABC + UpstreamCodec contract.

A client-facing adapter handles three concerns:
    parse_request               client body  → InternalRequest
    serialize_response_stream   IES events   → client SSE bytes
    serialize_response          IES events   → client JSON bytes

The OpenAI Chat adapter additionally implements UpstreamCodec — it knows how
to talk to the OpenAI-compatible MiMo upstream.
"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable

from gateway.core import GatewayError, InternalEvent, InternalRequest


class ProtocolAdapter(ABC):
    """Convert between a specific client protocol and the IES."""

    name: str = ""

    @classmethod
    @abstractmethod
    def matches_path(cls, path: str) -> bool:
        """Whether this adapter handles the given /v1/... path."""

    @abstractmethod
    def parse_request(self, body: dict[str, Any]) -> InternalRequest:
        """Client request body → InternalRequest."""

    @abstractmethod
    def serialize_response_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        """Stream IES events → SSE bytes for the client.

        Returned iterator owns the upstream lifecycle (events generator);
        consumers must drain it or close it.
        """

    @abstractmethod
    def serialize_response(self, events: list[InternalEvent]) -> bytes:
        """Non-stream: collect IES events → final response JSON."""

    def error_envelope(self, err: GatewayError) -> bytes:
        """Default: protocol-agnostic JSON. Subclasses override for protocol-specific shapes."""
        return json.dumps(err.to_dict()).encode()


@runtime_checkable
class UpstreamCodec(Protocol):
    """An adapter that also knows how to encode/decode the upstream protocol.

    Currently only OpenAIChatAdapter implements this since the MiMo upstream
    is OpenAI-compatible. Splitting it out keeps the client-side contract
    minimal and makes the upstream-codec role discoverable via isinstance().
    """

    def serialize_to_upstream(self, req: InternalRequest) -> dict[str, Any]: ...

    def parse_upstream_stream(
        self,
        raw: AsyncIterator[bytes],
        *,
        conversation_key: str,
    ) -> AsyncIterator[InternalEvent]: ...

    def parse_upstream_response(
        self,
        body: bytes,
        *,
        conversation_key: str,
    ) -> list[InternalEvent]: ...
