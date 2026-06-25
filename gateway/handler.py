"""
Request handler — the terminal step of the Pipeline.

Given an authenticated/rate-limited RequestContext plus the parsed body,
the handler:

  1. Asks the Router for a backend and records the routing decision.
  2. Asks the OpenAI Chat upstream codec to serialize the IES request.
  3. Calls the upstream via UpstreamTransport.
  4. Hands the upstream response back to the client adapter for
     serialization (streaming or non-streaming, as the request asked).

The handler only tracks per-request ``in_flight`` and metrics. Backend
health, breaker state, and routing latency are owned by the runtime's active
chat probes, not by user traffic.

The handler owns the upstream lifecycle: the streaming AsyncIterator it
returns transitively closes the httpx response when fully drained.

Anthropic clients (``adapter.name == "anthropic"``) take a separate
byte-passthrough path: the body goes through ``patch_request_thinking``
to rehydrate dropped ``thinking`` blocks, then hits the upstream
``/anthropic/v1/messages`` endpoint directly. The response is streamed
back unchanged while a tee'd parser harvests fresh thinking content into
the same reasoning cache. No IES conversion happens on this path because
the upstream protocol already matches the client's, and keeping bytes
intact preserves ``signature`` / future Anthropic fields automatically.
"""
from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import Any, Protocol

from gateway.adapters import OpenAIChatAdapter, ProtocolAdapter, UpstreamCodec
from gateway.anthropic_passthrough import (
    normalize_tool_choice,
    patch_request_thinking,
    scan_response_json,
    tee_stream_capture_thinking,
)
from gateway.core import (
    AdapterError,
    AuthError,
    BackendUnavailableError,
    BadRequestError,
    GatewayError,
    InternalEvent,
    InternalRequest,
    ModelNotFoundError,
    RequestContext,
    UpstreamError,
)
from gateway.model_capabilities import (
    validate_anthropic_body_capabilities,
    validate_request_capabilities,
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

# Try up to this many distinct backends before giving up on a request. A 401
# usually means one backend's injected upstream key is momentarily stale (the
# claw-side proxy rotates it), and a 429 means that account hit its quota — both
# are worth retrying on a *different* backend. When every attempt fails we
# surface a single friendly "high load" 503 instead of a raw per-backend error.
_MAX_ATTEMPTS = 8

# Upstream HTTP statuses worth retrying on another backend (in addition to any
# 5xx). 401: stale rotated key on one node. 429: that account is rate-limited.
_RETRYABLE_UPSTREAM_STATUSES = frozenset({401, 429})


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
        ttft_ms: float = 0,
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
        anthropic_upstream_path: str = "/anthropic/v1/messages",
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
        self._anthropic_upstream_path = anthropic_upstream_path
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
        if adapter.name == "anthropic":
            return await self._handle_anthropic_native(ctx, adapter, body)

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
        validate_request_capabilities(req)

        upstream_body = self._codec.serialize_to_upstream(req)
        # Conversation-scope key derived from this request's full message
        # history *and* tool surface. The codec writes captured reasoning
        # under this key on the response path; next turn's
        # serialize_to_upstream uses per-message prefix hashes (also
        # including tools/tool_choice) to look back up. Different
        # conversations / different tool sets get different keys, so an
        # attacker who guesses a tool_id can't pull another conversation's
        # reasoning out of the cache.
        from gateway.adapters.openai_chat import _conversation_key_for_request
        scoped_conversation_key = _conversation_key_for_request(
            req.messages,
            tools=req.tools,
            tool_choice=req.tool_choice,
            thinking=req.metadata.get("thinking") if req.metadata else None,
        )
        conversation_key = scoped_conversation_key

        if req.stream:
            return await self._handle_stream_with_retries(
                ctx, adapter, req.model, upstream_body,
                conversation_key=conversation_key,
            )
        return await self._handle_non_stream_with_retries(
            ctx, adapter, req.model, upstream_body,
            conversation_key=conversation_key,
        )

    # ============ Anthropic-native byte-passthrough ============

    async def _handle_anthropic_native(
        self,
        ctx: RequestContext,
        adapter: ProtocolAdapter,
        body: dict[str, Any],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        """Bypass IES for Anthropic clients.

        Hits MiMo's ``/anthropic/v1/messages`` directly so ``thinking`` blocks
        and ``signature`` round-trip as bytes. Before forwarding, we patch any
        assistant history message that has ``tool_use`` blocks but is missing
        the corresponding ``thinking`` block by rehydrating from the reasoning
        cache. After forwarding, we tee the response to harvest fresh thinking
        content back into the same cache.
        """
        requested_model = body.get("model")
        if not isinstance(requested_model, str) or not requested_model:
            raise BadRequestError("Missing 'model'")
        ctx.model = requested_model
        ctx.is_stream = bool(body.get("stream", False))

        principal = getattr(ctx, "principal", None)
        allowed_models = tuple(getattr(principal, "allowed_models", ()) or ())
        if allowed_models and requested_model not in allowed_models:
            raise AuthError(
                f"API key is not allowed to access model {requested_model!r}",
                details={"model": requested_model},
            )

        from gateway.model_groups_store import resolve as _resolve_mapping
        native = _resolve_mapping(requested_model, "anthropic")
        if native is None:
            raise ModelNotFoundError(
                f"Model {requested_model!r} is not configured for protocol 'anthropic'",
                details={"requested_model": requested_model, "protocol": "anthropic"},
            )
        if native != requested_model:
            body["model"] = native
            ctx.model = native

        # Capability gate runs on the raw Anthropic body (handles both base64
        # and URL image sources); no IES parse needed on the passthrough path.
        validate_anthropic_body_capabilities(native, body)

        # Rehydrate missing thinking blocks before sending to upstream.
        # patch_request_thinking computes its own per-assistant-message
        # prefix hashes internally — it walks ``body["messages"]`` and
        # scopes each lookup to the prefix up to that turn.
        normalize_tool_choice(body)
        patch_request_thinking(body)

        # Response-side capture uses one key derived from the full body we
        # sent up. Compute once, pass to scan / tee.
        from gateway.anthropic_passthrough import _conversation_key_from_body
        conversation_key = _conversation_key_from_body(body)

        if ctx.is_stream:
            return await self._handle_anthropic_stream_with_retries(
                ctx, adapter, native, body,
                conversation_key=conversation_key,
            )
        return await self._handle_anthropic_non_stream_with_retries(
            ctx, adapter, native, body,
            conversation_key=conversation_key,
        )

    def _headers_for_anthropic(
        self, backend, ctx: RequestContext,
    ) -> dict[str, str]:
        """OpenAI-style auth + forward Anthropic-specific request headers.

        Codex P1 from PR review: clients announce protocol version and beta
        features via ``anthropic-version`` / ``anthropic-beta``, and the
        upstream rejects requests that mismatch its expected version. The
        ECS proxy already passes these through; we need to too.
        """
        headers = self._headers_for(backend)
        # ctx.headers keys are lowercased by _ctx_from_request; forward in the
        # canonical Anthropic-* casing upstream sees from real Anthropic SDKs.
        for src_key, upstream_key in (
            ("anthropic-version", "Anthropic-Version"),
            ("anthropic-beta", "Anthropic-Beta"),
        ):
            v = ctx.headers.get(src_key)
            if v:
                headers[upstream_key] = v
        return headers

    async def _handle_anthropic_non_stream_with_retries(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        model: str, upstream_body: dict[str, Any],
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        for _attempt in range(_MAX_ATTEMPTS):
            try:
                backend = self._choose_backend(
                    ctx, model, exclude=tried,
                    upstream_path=self._anthropic_upstream_path,
                )
            except GatewayError:
                if last_error is not None:
                    raise _high_load_error(last_error) from last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_anthropic_non_stream(
                    ctx, adapter, backend, upstream_body,
                    self._headers_for_anthropic(backend, ctx),
                    conversation_key=conversation_key,
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_non_stream(e):
                    raise
                ctx.decide(f"retry_after:{backend.backend_id}:{e.error_code}")
                continue
        raise _high_load_error(last_error) from last_error

    async def _handle_anthropic_non_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body: dict[str, Any], headers: dict[str, str],
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            try:
                pxy = backend.metadata.get("proxy_url") if backend.metadata else None
                send_body = upstream_body
                status, raw = await self._transport.post_json(
                    ctx.upstream_url, send_body,
                    headers=headers, timeout_s=self._upstream_timeout_s,
                    proxy=pxy,
                )
            except GatewayError as e:
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=e.message)
                raise
            except Exception as e:
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=str(e))
                raise UpstreamError(f"Upstream call failed: {e}") from e

            ctx.upstream_status = status

            if status >= 400:
                self._record_metric(ctx, backend.backend_id, status,
                                    (time.monotonic() - started) * 1000,
                                    error=f"http {status}")
                _raise_client_payload_error_if_applicable(status, raw)
                raise UpstreamError(
                    f"Upstream returned {status}: {raw[:200]!r}",
                    details={"status": status},
                )

            latency_ms = (time.monotonic() - started) * 1000

            # Harvest thinking before returning bytes to client.
            scan_response_json(raw, conversation_key=conversation_key)
            prompt_t, completion_t = _extract_anthropic_token_counts(raw)

            self._record_metric(
                ctx, backend.backend_id, status, latency_ms,
                ttft_ms=latency_ms,
                prompt_tokens=prompt_t, completion_tokens=completion_t,
            )

            return "application/json", None, raw
        finally:
            backend.dec_in_flight()

    async def _handle_anthropic_stream_with_retries(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        model: str, upstream_body: dict[str, Any],
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        for _attempt in range(_MAX_ATTEMPTS):
            try:
                backend = self._choose_backend(
                    ctx, model, exclude=tried,
                    upstream_path=self._anthropic_upstream_path,
                )
            except GatewayError:
                if last_error is not None:
                    raise _high_load_error(last_error) from last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_anthropic_stream(
                    ctx, adapter, backend, upstream_body,
                    self._headers_for_anthropic(backend, ctx),
                    conversation_key=conversation_key,
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_stream(e):
                    raise
                ctx.decide(f"stream_retry_after:{backend.backend_id}:{e.error_code}")
                continue
        raise _high_load_error(last_error) from last_error

    async def _handle_anthropic_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body: dict[str, Any], headers: dict[str, str],
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            pxy = backend.metadata.get("proxy_url") if backend.metadata else None
            send_body = upstream_body
            status, raw_iter = await self._transport.post_stream(
                ctx.upstream_url, send_body,
                headers=headers, timeout_s=self._upstream_timeout_s,
                proxy=pxy,
            )
        except GatewayError as e:
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=e.message)
            raise
        except Exception as e:
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=str(e))
            raise UpstreamError(f"Upstream call failed: {e}") from e

        ctx.upstream_status = status
        if status >= 400:
            raw_error = bytearray()
            try:
                async for chunk in raw_iter:
                    raw_error.extend(chunk)
            except Exception:
                pass
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, status,
                                (time.monotonic() - started) * 1000,
                                error=f"http {status}")
            _raise_client_payload_error_if_applicable(status, bytes(raw_error))
            raise UpstreamError(
                f"Upstream returned {status}",
                details={"status": status},
            )

        # Shared mutable holder: the tee parser fills this as message_start /
        # message_delta frames flow past, so the metrics record after the
        # stream finishes can report real token counts.
        usage_sink: dict[str, int] = {}
        teed = tee_stream_capture_thinking(
            raw_iter, usage_sink, conversation_key=conversation_key,
        )
        recorder = self._metrics
        bid = backend.backend_id

        async def counted_chunks() -> AsyncIterator[bytes]:
            error = ""
            completed = False
            ttft_ms = 0.0
            try:
                async for chunk in teed:
                    if ttft_ms <= 0:
                        ttft_ms = (time.monotonic() - started) * 1000
                    ctx.response_chunks += 1
                    yield chunk
                completed = True
            except Exception as e:
                error = f"stream: {type(e).__name__}: {e}"
                raise
            finally:
                latency_ms = (time.monotonic() - started) * 1000
                backend.dec_in_flight()
                if recorder is not None:
                    try:
                        recorder.record(
                            ctx=ctx, backend_id=bid,
                            status_code=status if completed else 0,
                            latency_ms=latency_ms,
                            ttft_ms=ttft_ms,
                            prompt_tokens=usage_sink.get("input_tokens", 0),
                            completion_tokens=usage_sink.get("output_tokens", 0),
                            error=error,
                        )
                    except Exception:
                        pass

        return "text/event-stream", counted_chunks(), b""

    # ============ Shared helpers ============

    def _choose_backend(
        self,
        ctx: RequestContext,
        model: str,
        *,
        exclude: set[str] | None = None,
        upstream_path: str | None = None,
    ):
        backend, decision = self._router.choose(
            request_id=ctx.request_id, model=model, exclude=exclude,
        )
        ctx.target_backend_id = backend.backend_id
        ctx.upstream_url = backend.base_url.rstrip("/") + (upstream_path or self._upstream_path)
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
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        # Try up to _MAX_ATTEMPTS distinct backends. Each retryable failure
        # (401 stale key / 429 quota / 5xx) rotates to a fresh backend; once
        # we run out of backends or attempts we return a friendly high-load 503.
        for _attempt in range(_MAX_ATTEMPTS):
            try:
                backend = self._choose_backend(ctx, model, exclude=tried)
            except GatewayError:
                if last_error is not None:
                    raise _high_load_error(last_error) from last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_non_stream(
                    ctx, adapter, backend, upstream_body, self._headers_for(backend),
                    conversation_key=conversation_key,
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_non_stream(e):
                    raise
                ctx.decide(f"retry_after:{backend.backend_id}:{e.error_code}")
                continue
        raise _high_load_error(last_error) from last_error

    @staticmethod
    def _is_retryable_non_stream(err: GatewayError) -> bool:
        status = err.details.get("status") if getattr(err, "details", None) else None
        if isinstance(status, int):
            return status in _RETRYABLE_UPSTREAM_STATUSES or status >= 500
        return getattr(err, "http_status", 500) in (401, 429, 502, 503, 504)

    async def _handle_non_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
        *, conversation_key: str | list[str],
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
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=e.message)
                raise
            except Exception as e:
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=str(e))
                raise UpstreamError(f"Upstream call failed: {e}") from e

            ctx.upstream_status = status

            if status >= 400:
                # 4xx normally means the request payload/auth was rejected by
                # upstream. Do not poison backend health or rotate traffic across
                # otherwise healthy nodes for client/gateway request-shape bugs.
                self._record_metric(ctx, backend.backend_id, status,
                                    (time.monotonic() - started) * 1000,
                                    error=f"http {status}")
                _raise_client_payload_error_if_applicable(status, raw)
                raise UpstreamError(
                    f"Upstream returned {status}: {raw[:200]!r}",
                    details={"status": status},
                )

            latency_ms = (time.monotonic() - started) * 1000

            try:
                events = self._codec.parse_upstream_response(
                    raw, conversation_key=conversation_key,
                )
            except GatewayError:
                raise
            except Exception as e:
                raise AdapterError(f"Failed to parse upstream JSON: {e}") from e

            prompt_t, completion_t = _extract_token_counts(events)
            self._record_metric(
                ctx, backend.backend_id, status, latency_ms,
                ttft_ms=latency_ms,
                prompt_tokens=prompt_t, completion_tokens=completion_t,
            )

            body_bytes = adapter.serialize_response(events)
            return _content_type_for(adapter, stream=False), None, body_bytes
        finally:
            backend.dec_in_flight()

    async def _handle_stream_with_retries(
        self, ctx: RequestContext, adapter: ProtocolAdapter, model: str, upstream_body,
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        """Stream with retry on pre-stream failures.

        Once data starts flowing to the client we cannot retry (the client
        would see duplicated content).  But connection errors and upstream
        5xx responses happen before any data is sent, so those are safe to
        retry with a different backend.
        """
        tried: set[str] = set()
        last_error: GatewayError | None = None
        for _attempt in range(_MAX_ATTEMPTS):
            try:
                backend = self._choose_backend(ctx, model, exclude=tried)
            except GatewayError:
                if last_error is not None:
                    raise _high_load_error(last_error) from last_error
                raise
            tried.add(backend.backend_id)
            try:
                return await self._handle_stream(
                    ctx, adapter, backend, upstream_body, self._headers_for(backend),
                    conversation_key=conversation_key,
                )
            except GatewayError as e:
                last_error = e
                if not self._is_retryable_stream(e):
                    raise
                ctx.decide(f"stream_retry_after:{backend.backend_id}:{e.error_code}")
                continue
        raise _high_load_error(last_error) from last_error

    @staticmethod
    def _is_retryable_stream(err: GatewayError) -> bool:
        status = err.details.get("status") if getattr(err, "details", None) else None
        if isinstance(status, int):
            return status in _RETRYABLE_UPSTREAM_STATUSES or status >= 500
        # Connection errors / timeouts have no status — retry them too.
        return getattr(err, "http_status", 500) in (401, 429, 502, 503, 504)

    async def _handle_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
        *, conversation_key: str | list[str],
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            status, raw_iter = await self._transport.post_stream(
                ctx.upstream_url, upstream_body,
                headers=headers, timeout_s=self._upstream_timeout_s,
            )
        except GatewayError as e:
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=e.message)
            raise
        except Exception as e:
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=str(e))
            raise UpstreamError(f"Upstream call failed: {e}") from e

        ctx.upstream_status = status
        if status >= 400:
            raw_error = bytearray()
            try:
                async for chunk in raw_iter:
                    raw_error.extend(chunk)
            except Exception:
                pass
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, status,
                                (time.monotonic() - started) * 1000,
                                error=f"http {status}")
            _raise_client_payload_error_if_applicable(status, bytes(raw_error))
            raise UpstreamError(
                f"Upstream returned {status}",
                details={"status": status},
            )

        ies_events = self._codec.parse_upstream_stream(
            raw_iter, conversation_key=conversation_key,
        )
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
            ttft_ms = 0.0
            try:
                async for chunk in client_bytes:
                    if ttft_ms <= 0:
                        ttft_ms = (time.monotonic() - started) * 1000
                    ctx.response_chunks += 1
                    yield chunk
                completed = True
            except Exception as e:
                error = f"stream: {type(e).__name__}: {e}"
                raise
            finally:
                latency_ms = (time.monotonic() - started) * 1000
                backend.dec_in_flight()
                if recorder is not None:
                    try:
                        recorder.record(
                            ctx=ctx, backend_id=bid, status_code=status if completed else 0,
                            latency_ms=latency_ms,
                            ttft_ms=ttft_ms,
                            prompt_tokens=captured["prompt"],
                            completion_tokens=captured["completion"],
                            error=error,
                        )
                    except Exception:
                        pass

        return _content_type_for(adapter, stream=True), counted_chunks(), b""

    def _record_metric(
        self, ctx: RequestContext, backend_id: str, status_code: int,
        latency_ms: float, *, ttft_ms: float = 0, prompt_tokens: int = 0,
        completion_tokens: int = 0, error: str = "",
    ) -> None:
        if self._metrics is None:
            return
        try:
            self._metrics.record(
                ctx=ctx, backend_id=backend_id, status_code=status_code,
                latency_ms=latency_ms, ttft_ms=ttft_ms, prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens, error=error,
            )
        except Exception:
            pass


def _high_load_error(last_error: GatewayError | None) -> BackendUnavailableError:
    """Friendly 503 returned after all retry attempts are exhausted."""
    details: dict[str, Any] = {}
    if last_error is not None:
        details["last_error"] = last_error.error_code
        status = last_error.details.get("status") if last_error.details else None
        if status is not None:
            details["last_upstream_status"] = status
    return BackendUnavailableError("上游高负载，请稍后再试", details=details)


def _content_type_for(adapter: ProtocolAdapter, *, stream: bool) -> str:
    if stream:
        return "text/event-stream"
    if adapter.name == "anthropic":
        return "application/json"
    return "application/json"


def _raise_client_payload_error_if_applicable(status: int, raw: bytes) -> None:
    """Turn upstream 400 payload/schema rejections into clearer client errors."""
    if status != 400:
        return
    upstream = _parse_upstream_error(raw)
    raise BadRequestError(
        "客户端请求体参数不符合 MiMo API 要求",
        details={
            "upstream_status": status,
            "upstream_error": upstream,
        },
    )


def _parse_upstream_error(raw: bytes) -> dict[str, str]:
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, ValueError):
        text = raw.decode("utf-8", errors="replace").strip()
        return {"message": text[:500] if text else "Param Incorrect"}
    if not isinstance(data, dict):
        return {"message": "Param Incorrect"}
    err = data.get("error")
    if isinstance(err, dict):
        return {
            str(k): str(v)
            for k, v in err.items()
            if k in {"code", "message", "param", "type"} and v is not None
        }
    return {
        str(k): str(v)
        for k, v in data.items()
        if k in {"code", "message", "param", "type"} and v is not None
    }


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


def _extract_anthropic_token_counts(raw: bytes) -> tuple[int, int]:
    """Pull (prompt, completion) from an Anthropic non-stream response body."""
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return 0, 0
    if not isinstance(data, dict):
        return 0, 0
    usage = data.get("usage")
    if not isinstance(usage, dict):
        return 0, 0
    try:
        return (
            int(usage.get("input_tokens") or 0),
            int(usage.get("output_tokens") or 0),
        )
    except (TypeError, ValueError):
        return 0, 0


# helper for typing-only import
_ = InternalEvent
