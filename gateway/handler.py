"""
Request handler — the terminal step of the Pipeline.

Given an authenticated/rate-limited RequestContext plus the parsed body,
the handler:

  1. Asks the Router for a backend and records the routing decision.
  2. Asks the OpenAI Chat upstream codec to serialize the IES request.
  3. Calls the upstream via UpstreamTransport.
  4. Hands the upstream response back to the client adapter for
     serialization (streaming or non-streaming, as the request asked).

The handler also drives load-balancer state on the Backend object: it
inc/decs ``in_flight`` around the upstream call and records latency
into the EWMA so subsequent routing decisions see real numbers.

The handler owns the upstream lifecycle: the streaming AsyncIterator it
returns transitively closes the httpx response when fully drained.
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any, Protocol

from gateway.adapters import OpenAIChatAdapter, ProtocolAdapter, UpstreamCodec
from gateway.core import (
    AdapterError,
    AuthError,
    GatewayError,
    InternalEvent,
    ModelNotFoundError,
    RequestContext,
    UpstreamError,
)
from gateway.routing import Router
from gateway.transport import UpstreamTransport


# Map the adapter name (``ctx.src_protocol``) to the short tag stored in
# model-mapping ``protocols`` lists. OpenAI Chat / OpenAI Responses both
# count as ``openai``; only Anthropic Messages counts as ``anthropic``.
_PROTOCOL_TAG = {
    "openai_chat": "openai",
    "openai_responses": "openai",
    "anthropic": "anthropic",
}


class DecisionLogWriter(Protocol):
    def write(self, decision: Any) -> None: ...


class MetricsRecorder(Protocol):
    """Optional sink for per-request metrics."""

    def record(
        self,
        *,
        ctx: RequestContext,
        backend_id: str,
        status_code: int,
        latency_ms: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: str = "",
    ) -> None: ...


class GatewayHandler:
    """Glue between the client adapter, the router, and the upstream codec."""

    def __init__(
        self,
        *,
        router: Router,
        transport: UpstreamTransport,
        upstream_codec: UpstreamCodec | None = None,
        decision_log: DecisionLogWriter | None = None,
        metrics: MetricsRecorder | None = None,
        upstream_path: str = "/v1/chat/completions",
        upstream_timeout_s: float = 600.0,
    ):
        self._router = router
        self._transport = transport
        # Default to OpenAIChatAdapter as the upstream codec — that's what
        # MiMo speaks. Someone could swap it for a different upstream proto
        # later without changing the handler.
        self._codec = upstream_codec or OpenAIChatAdapter()
        self._decision_log = decision_log
        self._metrics = metrics
        self._upstream_path = upstream_path
        self._upstream_timeout_s = upstream_timeout_s

    async def handle(
        self,
        ctx: RequestContext,
        adapter: ProtocolAdapter,
        body: dict[str, Any],
    ) -> tuple[bytes, AsyncIterator[bytes] | None, str]:
        """Execute the full request lifecycle.

        Returns ``(headers_content_type, stream_body_or_none, response_bytes_or_empty)``::

          * For non-stream: ``(content_type, None, body_bytes)``
          * For stream:     ``(content_type, async_iter, b"")``

        Raises GatewayError; callers (the FastAPI route) translate that
        into the proper protocol error envelope via ``adapter.error_envelope``.
        """
        req = adapter.parse_request(body)
        ctx.model = req.model
        ctx.is_stream = req.stream
        requested_model = req.model

        principal = getattr(ctx, "principal", None)
        allowed_models = tuple(getattr(principal, "allowed_models", ()) or ())
        if allowed_models and requested_model not in allowed_models:
            raise AuthError(
                f"API key is not allowed to access model {requested_model!r}",
                details={"model": requested_model},
            )

        # Resolve the client-facing model name to a native upstream model via
        # the model-mapping store. No match → ModelNotFoundError (404).
        from gateway.model_groups_store import resolve as _resolve_mapping
        proto_tag = _PROTOCOL_TAG.get(adapter.name, adapter.name)
        native = _resolve_mapping(req.model, proto_tag)
        if native is None:
            raise ModelNotFoundError(
                f"Model {req.model!r} is not configured for protocol {proto_tag!r}",
                details={"requested_model": req.model, "protocol": proto_tag},
            )
        if native != req.model:
            # Rewrite so the upstream sees the native name, not the client-facing alias.
            req.model = native
            ctx.model = native

        upstream_body = self._codec.serialize_to_upstream(req)

        if req.stream:
            return await self._handle_stream_with_retries(
                ctx, adapter, req.model, upstream_body,
            )
        return await self._handle_non_stream_with_retries(
            ctx, adapter, req.model, upstream_body,
        )

    def _choose_backend(self, ctx: RequestContext, model: str, *, exclude: set[str] | None = None):
        backend, decision = self._router.choose(
            request_id=ctx.request_id, model=model, exclude=exclude,
        )
        ctx.target_backend_id = backend.backend_id
        ctx.upstream_url = backend.base_url.rstrip("/") + self._upstream_path
        ctx.decide(f"route:{backend.backend_id}:{decision.reason}")
        if self._decision_log is not None:
            try:
                self._decision_log.write(decision)
            except Exception:
                pass  # best-effort
        return backend

    @staticmethod
    def _headers_for(backend) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if backend.api_key:
            headers["Authorization"] = f"Bearer {backend.api_key}"
        return headers

    async def _handle_non_stream_with_retries(
        self, ctx: RequestContext, adapter: ProtocolAdapter, model: str, upstream_body,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        # Keep retries deliberately small: one retry is enough to mask transient
        # backend failures without multiplying upstream cost.
        for _attempt in range(2):
            try:
                backend = self._choose_backend(ctx, model, exclude=tried)
            except GatewayError:
                if last_error is not None:
                    raise last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_non_stream(
                    ctx, adapter, backend, upstream_body, self._headers_for(backend),
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_non_stream(e):
                    raise
                ctx.decide(f"retry_after:{backend.backend_id}:{e.error_code}")
                continue
        assert last_error is not None
        raise last_error

    @staticmethod
    def _is_retryable_non_stream(err: GatewayError) -> bool:
        status = err.details.get("status") if getattr(err, "details", None) else None
        if isinstance(status, int):
            return status >= 500
        return getattr(err, "http_status", 500) in (502, 503, 504)

    async def _handle_non_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            try:
                status, raw = await self._transport.post_json(
                    ctx.upstream_url, upstream_body,
                    headers=headers, timeout_s=self._upstream_timeout_s,
                )
            except GatewayError as e:
                backend.record_failure(f"{e.error_code}: {e.message}")
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=e.message)
                raise
            except Exception as e:
                backend.record_failure(f"transport: {type(e).__name__}: {e}")
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=str(e))
                raise UpstreamError(f"Upstream call failed: {e}") from e

            ctx.upstream_status = status

            if status >= 400:
                # 4xx normally means the request payload/auth was rejected by
                # upstream. Do not poison backend health or rotate traffic across
                # otherwise healthy nodes for client/gateway request-shape bugs.
                if status >= 500:
                    backend.record_failure(f"upstream http {status}")
                self._record_metric(ctx, backend.backend_id, status,
                                    (time.monotonic() - started) * 1000,
                                    error=f"http {status}")
                raise UpstreamError(
                    f"Upstream returned {status}: {raw[:200]!r}",
                    details={"status": status},
                )

            latency_ms = (time.monotonic() - started) * 1000
            backend.record_success()
            backend.record_latency(latency_ms)

            try:
                events = self._codec.parse_upstream_response(raw)
            except GatewayError:
                raise
            except Exception as e:
                raise AdapterError(f"Failed to parse upstream JSON: {e}") from e

            prompt_t, completion_t = _extract_token_counts(events)
            self._record_metric(
                ctx, backend.backend_id, status, latency_ms,
                prompt_tokens=prompt_t, completion_tokens=completion_t,
            )

            body_bytes = adapter.serialize_response(events)
            return _content_type_for(adapter, stream=False), None, body_bytes
        finally:
            backend.dec_in_flight()

    async def _handle_stream_with_retries(
        self, ctx: RequestContext, adapter: ProtocolAdapter, model: str, upstream_body,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        """Stream with retry on pre-stream failures.

        Once data starts flowing to the client we cannot retry (the client
        would see duplicated content).  But connection errors and upstream
        5xx responses happen before any data is sent, so those are safe to
        retry with a different backend.
        """
        tried: set[str] = set()
        last_error: GatewayError | None = None
        for _attempt in range(2):
            try:
                backend = self._choose_backend(ctx, model, exclude=tried)
            except GatewayError:
                if last_error is not None:
                    raise last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_stream(
                    ctx, adapter, backend, upstream_body, self._headers_for(backend),
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_stream(e):
                    raise
                ctx.decide(f"stream_retry_after:{backend.backend_id}:{e.error_code}")
                continue
        assert last_error is not None
        raise last_error

    @staticmethod
    def _is_retryable_stream(err: GatewayError) -> bool:
        status = err.details.get("status") if getattr(err, "details", None) else None
        if isinstance(status, int):
            return status >= 500
        # Connection errors / timeouts have no status — retry them too.
        return getattr(err, "http_status", 500) in (502, 503, 504)

    async def _handle_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            status, raw_iter = await self._transport.post_stream(
                ctx.upstream_url, upstream_body,
                headers=headers, timeout_s=self._upstream_timeout_s,
            )
        except GatewayError as e:
            backend.record_failure(f"{e.error_code}: {e.message}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=e.message)
            raise
        except Exception as e:
            backend.record_failure(f"transport: {type(e).__name__}: {e}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=str(e))
            raise UpstreamError(f"Upstream call failed: {e}") from e

        ctx.upstream_status = status
        if status >= 400:
            try:
                async for _ in raw_iter:
                    pass
            except Exception:
                pass
            if status >= 500:
                backend.record_failure(f"upstream http {status}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, status,
                                (time.monotonic() - started) * 1000,
                                error=f"http {status}")
            raise UpstreamError(
                f"Upstream returned {status}",
                details={"status": status},
            )

        ies_events = self._codec.parse_upstream_stream(raw_iter)
        # Tee usage out of the IES stream while it flows to the serializer.
        # Upstream usage typically arrives in the final MessageEnd event,
        # so we only know real numbers after the stream finishes.
        captured = {"prompt": 0, "completion": 0}

        async def _tee_usage(src):
            async for ev in src:
                u = getattr(ev, "usage", None)
                if u is not None:
                    p = int(getattr(u, "input_tokens", 0)
                            or getattr(u, "prompt_tokens", 0) or 0)
                    c = int(getattr(u, "output_tokens", 0)
                            or getattr(u, "completion_tokens", 0) or 0)
                    if p or c:
                        captured["prompt"] = p
                        captured["completion"] = c
                yield ev

        client_bytes = adapter.serialize_response_stream(_tee_usage(ies_events))
        recorder = self._metrics
        bid = backend.backend_id

        async def counted_chunks() -> AsyncIterator[bytes]:
            error = ""
            completed = False
            try:
                async for chunk in client_bytes:
                    ctx.response_chunks += 1
                    yield chunk
                completed = True
            except Exception as e:
                error = f"stream: {type(e).__name__}: {e}"
                backend.record_failure(error)
                raise
            finally:
                latency_ms = (time.monotonic() - started) * 1000
                if completed:
                    backend.record_success()
                    backend.record_latency(latency_ms)
                backend.dec_in_flight()
                if recorder is not None:
                    try:
                        recorder.record(
                            ctx=ctx, backend_id=bid, status_code=status if completed else 0,
                            latency_ms=latency_ms,
                            prompt_tokens=captured["prompt"],
                            completion_tokens=captured["completion"],
                            error=error,
                        )
                    except Exception:
                        pass

        return _content_type_for(adapter, stream=True), counted_chunks(), b""

    def _record_metric(
        self, ctx: RequestContext, backend_id: str, status_code: int,
        latency_ms: float, *, prompt_tokens: int = 0,
        completion_tokens: int = 0, error: str = "",
    ) -> None:
        if self._metrics is None:
            return
        try:
            self._metrics.record(
                ctx=ctx, backend_id=backend_id, status_code=status_code,
                latency_ms=latency_ms, prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens, error=error,
            )
        except Exception:
            pass


def _content_type_for(adapter: ProtocolAdapter, *, stream: bool) -> str:
    if stream:
        return "text/event-stream"
    if adapter.name == "anthropic":
        return "application/json"
    return "application/json"


def _extract_token_counts(events) -> tuple[int, int]:
    """Pull (prompt_tokens, completion_tokens) from a non-stream IES event list.

    Returns (0, 0) if the upstream didn't include usage. The IES Usage
    dataclass uses Anthropic-style naming (input_tokens/output_tokens) but
    we also accept OpenAI-style dicts as a fallback.
    """
    for ev in events:
        usage = getattr(ev, "usage", None)
        if usage is None and isinstance(ev, dict):
            usage = ev.get("usage")
        if usage:
            try:
                if isinstance(usage, dict):
                    return (
                        int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
                        int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
                    )
                return (
                    int(getattr(usage, "input_tokens", 0)
                        or getattr(usage, "prompt_tokens", 0) or 0),
                    int(getattr(usage, "output_tokens", 0)
                        or getattr(usage, "completion_tokens", 0) or 0),
                )
            except (TypeError, ValueError):
                pass
    return 0, 0


# helper for typing-only import
_ = InternalEvent
