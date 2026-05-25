"""FastAPI routes for the gateway data plane.

This module keeps the bulky /v1 proxy, CORS, and public health endpoints out of
``app.py`` while preserving the same external routes.
"""
from __future__ import annotations

import base64
import json

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from gateway.audio_speech import (
    AudioSpeechRequest,
    audio_media_type,
    map_openai_tts_model,
    map_openai_tts_voice,
)
from gateway.auth import authenticate_gateway_request
from gateway.core import AuthError

_GATEWAY_PATHS = {"/v1/chat/completions", "/v1/audio/speech", "/v1/messages", "/v1/responses", "/v1/models"}


def _translate_audio_speech_request(payload: AudioSpeechRequest) -> dict[str, object]:
    input_text = payload.input.strip()
    model = map_openai_tts_model(payload.model)
    messages: list[dict[str, str]] = []
    audio: dict[str, object] = {"format": payload.response_format.lower()}

    if model == "mimo-v2.5-tts-voiceclone":
        sample_b64 = (payload.voice_sample_base64 or "").strip()
        sample_mime = (payload.voice_sample_mime_type or "").strip()
        if not sample_b64:
            raise ValueError("`voice_sample_base64` 是 mimo-v2.5-tts-voiceclone 的必填字段")
        if not sample_mime:
            raise ValueError("`voice_sample_mime_type` 是 mimo-v2.5-tts-voiceclone 的必填字段")
        if isinstance(payload.instructions, str) and payload.instructions.strip():
            messages.append({"role": "user", "content": payload.instructions.strip()})
        messages.append({"role": "assistant", "content": input_text})
        audio["voice"] = f"data:{sample_mime};base64,{sample_b64}"
        return {
            "model": model,
            "messages": messages,
            "audio": audio,
            "stream": False,
        }

    if model == "mimo-v2.5-tts-voicedesign":
        voice_description = (payload.voice_description or "").strip()
        if not voice_description:
            raise ValueError("`voice_description` 是 mimo-v2.5-tts-voicedesign 的必填字段")
        messages.append({"role": "user", "content": voice_description})
        messages.append({"role": "assistant", "content": input_text})
        audio["voice"] = map_openai_tts_voice(payload.voice)
        if payload.optimize_text_preview is not None:
            audio["optimize_text_preview"] = payload.optimize_text_preview
        return {
            "model": model,
            "messages": messages,
            "audio": audio,
            "stream": False,
        }

    if isinstance(payload.instructions, str) and payload.instructions.strip():
        messages.append({"role": "user", "content": payload.instructions.strip()})
    messages.append({"role": "assistant", "content": input_text})
    audio["voice"] = map_openai_tts_voice(payload.voice)
    return {
        "model": model,
        "messages": messages,
        "audio": audio,
        "stream": False,
    }


def _extract_audio_response_bytes(resp_json: dict, *, fallback_format: str) -> tuple[bytes, str]:
    audio = (((resp_json.get("choices") or [{}])[0].get("message") or {}).get("audio") or {})
    audio_b64 = audio.get("data") if isinstance(audio, dict) else None
    audio_format = audio.get("format") if isinstance(audio, dict) else None
    if not isinstance(audio_b64, str) or not audio_b64:
        raise ValueError("上游 TTS 响应里没有音频数据")
    try:
        audio_bytes = base64.b64decode(audio_b64)
    except Exception as e:  # noqa: BLE001
        raise ValueError("上游 TTS 音频数据损坏") from e
    return audio_bytes, (audio_format or fallback_format).lower()


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

        try:
            principal = await authenticate_gateway_request(request, auth_cookie=auth_cookie)
            request.state.gateway_principal = principal

            if full_path == "/v1/audio/speech":
                if request.method != "POST":
                    return JSONResponse({"error": {"message": "Method not allowed"}}, status_code=405)
                try:
                    raw_body = await request.body()
                    payload = AudioSpeechRequest.model_validate_json(raw_body)
                except Exception as e:
                    return JSONResponse({"error": {"message": f"Invalid request body: {e}"}}, status_code=400)

                input_text = payload.input.strip()
                if not input_text:
                    return JSONResponse({"error": {"message": "`input` 不能为空"}}, status_code=400)

                from gateway.runtime import dispatch_with_body_override
                try:
                    translated = _translate_audio_speech_request(payload)
                except ValueError as e:
                    return JSONResponse({"error": {"message": str(e)}}, status_code=400)
                upstream_resp = await dispatch_with_body_override("openai_chat", request, translated)
                if upstream_resp.status_code >= 400:
                    return upstream_resp
                try:
                    resp_json = json.loads(upstream_resp.body)
                except Exception:
                    return JSONResponse({"error": {"message": "上游 TTS 返回了非法 JSON"}}, status_code=502)
                try:
                    audio_bytes, audio_format = _extract_audio_response_bytes(
                        resp_json,
                        fallback_format=payload.response_format,
                    )
                except ValueError as e:
                    return JSONResponse({"error": {"message": str(e)}}, status_code=502)
                return Response(audio_bytes, media_type=audio_media_type(audio_format))

            from gateway.runtime import dispatch
            adapter_name = "openai_chat"
            if full_path == "/v1/messages":
                adapter_name = "anthropic"
            elif full_path == "/v1/responses":
                adapter_name = "openai_responses"

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
