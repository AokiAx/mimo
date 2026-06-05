"""
Upstream HTTP transport.

Wraps httpx.AsyncClient with the two methods the handler needs:
``post_json`` (non-stream) and ``post_stream`` (returns an async iterator
of bytes that can be fed straight to the adapter's ``parse_upstream_*``).

Errors are normalized into GatewayErrors so callers don't need to catch
httpx's exception hierarchy.
"""
from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any, Protocol

import httpx

from gateway.core import (
    BackendUnavailableError,
    GatewayError,
    UpstreamError,
    UpstreamTimeoutError,
)


logger = logging.getLogger(__name__)
_MAX_LOG_BODY_CHARS = 4000


class UpstreamTransport(Protocol):
    """Minimal interface so handler tests can inject a fake."""

    async def post_json(
        self,
        url: str,
        body: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        timeout_s: float = 60.0,
    ) -> tuple[int, bytes]: ...

    async def post_stream(
        self,
        url: str,
        body: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        timeout_s: float = 600.0,
    ) -> tuple[int, AsyncIterator[bytes]]: ...

    async def close(self) -> None: ...


class HttpxTransport:
    """httpx.AsyncClient-backed transport."""

    def __init__(
        self,
        *,
        connect_timeout_s: float = 10.0,
        keepalive: int = 20,
        max_connections: int = 100,
        trust_env: bool = False,
    ):
        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=keepalive,
        )
        # trust_env=False by default: a gateway always knows the exact
        # upstream URL, so any system-level HTTP proxy (e.g. WinINET WPAD)
        # interfering with that traffic is a misconfiguration, not a feature.
        self._client = httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(connect_timeout_s),
            trust_env=trust_env,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def post_json(
        self,
        url: str,
        body: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        timeout_s: float = 60.0,
    ) -> tuple[int, bytes]:
        try:
            resp = await self._client.post(
                url, json=body, headers=headers or {},
                timeout=httpx.Timeout(timeout_s),
            )
        except httpx.TimeoutException as e:
            raise UpstreamTimeoutError(f"Upstream timeout: {e}") from e
        except httpx.ConnectError as e:
            raise BackendUnavailableError(f"Upstream connect failed: {e}") from e
        except httpx.HTTPError as e:
            raise UpstreamError(f"Upstream transport error: {e}") from e

        if resp.status_code >= 400:
            _log_upstream_http_error(url, resp.status_code, resp.content, body)
        return resp.status_code, resp.content

    async def post_stream(
        self,
        url: str,
        body: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        timeout_s: float = 600.0,
    ) -> tuple[int, AsyncIterator[bytes]]:
        # stream() returns a context manager; we manage it via the iterator
        # so the caller drains the body before we close it.
        ctx_mgr = self._client.stream(
            "POST", url, json=body, headers=headers or {},
            timeout=httpx.Timeout(timeout_s),
        )
        try:
            response = await ctx_mgr.__aenter__()
        except httpx.TimeoutException as e:
            raise UpstreamTimeoutError(f"Upstream timeout: {e}") from e
        except httpx.ConnectError as e:
            raise BackendUnavailableError(f"Upstream connect failed: {e}") from e
        except httpx.HTTPError as e:
            raise UpstreamError(f"Upstream transport error: {e}") from e

        status = response.status_code
        if status >= 400:
            raw = await response.aread()
            await ctx_mgr.__aexit__(None, None, None)
            _log_upstream_http_error(url, status, raw, body)
            return status, _single_chunk_iter(raw)

        async def iter_bytes() -> AsyncIterator[bytes]:
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await ctx_mgr.__aexit__(None, None, None)

        return status, iter_bytes()


async def _single_chunk_iter(raw: bytes) -> AsyncIterator[bytes]:
    if raw:
        yield raw


def _log_upstream_http_error(url: str, status_code: int, raw: bytes, body: dict[str, Any]) -> None:
    """Log upstream HTTP error details for troubleshooting.

    Request prompts can be sensitive, so the log includes only routing/body shape
    fields plus the upstream error payload that explains the failure.
    """
    logger.error(
        "Upstream MiMo API returned HTTP %s for %s; request=%s; response=%s",
        status_code,
        url,
        _summarize_request_body(body),
        _format_response_body(raw),
    )


def _summarize_request_body(body: dict[str, Any]) -> dict[str, Any]:
    messages = body.get("messages")
    summary: dict[str, Any] = {
        "model": body.get("model", ""),
        "stream": bool(body.get("stream", False)),
        "keys": sorted(str(k) for k in body.keys()),
    }
    if isinstance(messages, list):
        summary["messages_count"] = len(messages)
    return summary


def _format_response_body(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return "<empty>"

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return _truncate_for_log(text)

    return _truncate_for_log(json.dumps(parsed, ensure_ascii=False, separators=(",", ":")))


def _truncate_for_log(text: str) -> str:
    if len(text) <= _MAX_LOG_BODY_CHARS:
        return text
    return text[:_MAX_LOG_BODY_CHARS] + "…<truncated>"


def normalize_upstream_exception(e: Exception) -> GatewayError:
    """Convert any upstream-side exception to a GatewayError."""
    if isinstance(e, GatewayError):
        return e
    if isinstance(e, httpx.TimeoutException):
        return UpstreamTimeoutError(f"Upstream timeout: {e}")
    if isinstance(e, httpx.ConnectError):
        return BackendUnavailableError(f"Upstream connect failed: {e}")
    if isinstance(e, httpx.HTTPError):
        return UpstreamError(f"Upstream transport error: {e}")
    return UpstreamError(f"Unexpected upstream error: {type(e).__name__}: {e}")
