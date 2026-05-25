"""MiMo HTTP and Claw WebSocket client helpers."""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from urllib.parse import quote

import httpx

from claw import account_store

MIMO_BASE = "https://aistudio.xiaomimimo.com"

_http_client: httpx.AsyncClient | None = None
_sync_http_client: httpx.Client | None = None


def get_cookie_parts():
    return account_store.cookie_parts_from(account_store.load_cookies())


def get_ph_encoded():
    _, ph = get_cookie_parts()
    if not ph:
        return None
    return quote(ph, safe="")


def get_cookie_header_all():
    return account_store.cookie_header_all_from(account_store.load_cookies())


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            trust_env=False,
            follow_redirects=False,
        )
    return _http_client


def _get_sync_http_client() -> httpx.Client:
    global _sync_http_client
    if _sync_http_client is None:
        _sync_http_client = httpx.Client(
            timeout=httpx.Timeout(20.0, connect=10.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            trust_env=False,
            follow_redirects=False,
        )
    return _sync_http_client


async def close_clients() -> None:
    global _http_client, _sync_http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
    if _sync_http_client is not None:
        _sync_http_client.close()
        _sync_http_client = None


def _build_mimo_request(method, path, body, with_ph, cookies):
    """Shared URL/header/content builder for both sync and async API paths."""
    if cookies is None:
        cookies = account_store.load_cookies()
    cookie_header = account_store.cookie_header_all_from(cookies)
    _, ph = account_store.cookie_parts_from(cookies)
    ph_enc = quote(ph, safe="") if ph else None

    url = "{0}{1}".format(MIMO_BASE, path)
    if with_ph and ph_enc:
        sep = "&" if "?" in path else "?"
        url = "{0}{1}xiaomichatbot_ph={2}".format(url, sep, ph_enc)

    headers = {
        "cookie": cookie_header,
        "content-type": "application/json",
    }
    content = (
        json.dumps(body, ensure_ascii=False).encode("utf-8")
        if body is not None
        else None
    )
    return method, url, headers, content


def _parse_mimo_response(resp):
    code_line = "HTTP_{0}".format(resp.status_code)
    text = resp.text
    try:
        resp_json = json.loads(text) if text else ""
    except (json.JSONDecodeError, ValueError):
        resp_json = text
    return code_line, resp_json


def curl_api(method, path, body=None, with_ph=True, cookies=None):
    """Sync MiMo API call for background threads."""
    method, url, headers, content = _build_mimo_request(method, path, body, with_ph, cookies)
    try:
        resp = _get_sync_http_client().request(method, url, headers=headers, content=content)
        return _parse_mimo_response(resp)
    except httpx.TimeoutException as e:
        return "ERROR", "Timeout: {}".format(e)
    except httpx.HTTPError as e:
        return "ERROR", "{}: {}".format(type(e).__name__, e)
    except Exception as e:
        return "ERROR", "{}: {}".format(type(e).__name__, e)


async def acurl(method, path, body=None, with_ph=True, cookies=None):
    """Call MiMo API via shared httpx.AsyncClient."""
    method, url, headers, content = _build_mimo_request(method, path, body, with_ph, cookies)
    try:
        resp = await _get_http_client().request(method, url, headers=headers, content=content)
        return _parse_mimo_response(resp)
    except httpx.TimeoutException as e:
        return "ERROR", "Timeout: {}".format(e)
    except httpx.HTTPError as e:
        return "ERROR", "{}: {}".format(type(e).__name__, e)
    except Exception as e:
        return "ERROR", "{}: {}".format(type(e).__name__, e)


async def upload_to_claw_fds(
    filename: str,
    content: bytes,
    cookies: list | None = None,
    file_type: str = "txt",
) -> tuple[dict | None, str | None]:
    """Upload ``content`` to MiMo's Galaxy FDS so Claw can fetch it."""
    md5_hex = hashlib.md5(content).hexdigest()
    code, data = await acurl(
        "POST", "/open-apis/resource/genUploadInfo",
        body={"fileName": filename, "fileContentMd5": md5_hex},
        cookies=cookies,
    )
    if not (code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0):
        return None, f"genUploadInfo failed: {code} {data}"
    info = data.get("data") or {}
    upload_url = info.get("uploadUrl")
    resource_url = info.get("resourceUrl")
    if not upload_url or not resource_url:
        return None, f"genUploadInfo missing urls: {info}"

    try:
        resp = await _get_http_client().put(
            upload_url,
            content=content,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-MD5": md5_hex,
            },
        )
    except httpx.HTTPError as e:
        return None, f"FDS PUT failed: {type(e).__name__}: {e}"
    if resp.status_code != 200:
        return None, f"FDS PUT status {resp.status_code}: {resp.text[:200]}"

    return {
        "name": filename,
        "size": len(content),
        "url": resource_url,
        "type": file_type,
    }, None


async def claw_ws_chat(
    message: str,
    session_key: str | None = None,
    cookies: list | None = None,
    attachments: list[dict] | None = None,
) -> tuple[str, str | None]:
    """Send a message to Claw over the WS gateway and return ``(reply, error)``."""
    if not session_key:
        session_key = "agent:main:deploy-" + uuid.uuid4().hex[:8]

    if attachments:
        envelope = {
            "files": attachments,
            "prompt": "以上为用户上传的文件列表，请先下载上述文件后再回答 用户的问题。",
        }
        message = (
            "<mimo-files>\n"
            + json.dumps(envelope, ensure_ascii=False)
            + "\n</mimo-files>\n"
            + message
        )

    await acurl("POST", "/open-apis/user/mimo-claw/create", body={}, cookies=cookies)

    code, data = await acurl("GET", "/open-apis/user/ws/ticket", cookies=cookies)
    ticket = None
    if isinstance(data, dict) and data.get("code") == 0:
        ticket = data.get("data", {}).get("ticket")

    code2, data2 = await acurl("GET", "/open-apis/user/mi/get", with_ph=False, cookies=cookies)
    user_id = None
    if isinstance(data2, dict) and data2.get("code") == 0:
        user_id = data2.get("data", {}).get("userId")

    if not ticket or not user_id:
        def _redact(d):
            if not isinstance(d, dict):
                return d
            data_blob = d.get("data")
            if isinstance(data_blob, dict) and "ticket" in data_blob:
                redacted = dict(data_blob)
                redacted["ticket"] = "<redacted>"
                return {**d, "data": redacted}
            return d

        return "", (
            f"Failed to get WS ticket/userId — "
            f"ticket_call: http={code}, body={_redact(data)!r}; "
            f"userid_call: http={code2}, body={_redact(data2)!r}"
        )

    ws_url = "wss://aistudio.xiaomimimo.com/ws/proxy?ticket={0}&userId={1}".format(ticket, user_id)

    try:
        import websockets
    except ImportError:
        return "", "websockets not installed"

    full_text = ""
    debug_log: list[str] = []
    try:
        async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10) as ws:
            init_msg = await asyncio.wait_for(ws.recv(), timeout=10)
            debug_log.append(f"init: {init_msg[:200]}")

            req_id = str(uuid.uuid4())
            await ws.send(json.dumps({
                "type": "req", "id": req_id, "method": "connect",
                "params": {
                    "minProtocol": 3, "maxProtocol": 3,
                    "client": {"id": "cli", "version": "mimo-manager", "platform": "Linux", "mode": "cli"},
                    "role": "operator",
                    "scopes": ["operator.admin", "operator.read", "operator.write"],
                    "caps": ["tool-events"], "userAgent": "Mozilla/5.0", "locale": "zh-CN"
                }
            }))

            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                d = json.loads(msg)
                debug_log.append(f"pre-connect: type={d.get('type')} id={d.get('id','')[:8]} ok={d.get('ok')}")
                if d.get("id") == req_id:
                    if not d.get("ok"):
                        return "", "Connect failed: {0}".format(d)
                    break

            msg_id = str(uuid.uuid4())
            await ws.send(json.dumps({
                "type": "req", "id": msg_id, "method": "chat.send",
                "params": {
                    "sessionKey": session_key,
                    "message": message,
                    "deliver": False,
                    "idempotencyKey": msg_id,
                }
            }))
            debug_log.append(f"sent chat.send msg_id={msg_id[:8]}")

            for i in range(500):
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=300)
                    data = json.loads(raw)
                    dtype = data.get("type", "")
                    devent = data.get("event", "")
                    dpayload = data.get("payload", {})
                    did = data.get("id", "")
                    if i < 50:
                        debug_log.append(f"[{i}] type={dtype} event={devent} id={did[:8]} ok={data.get('ok')}")

                    if dtype == "res":
                        if did == msg_id and not data.get("ok"):
                            return "", "chat.send error: {0}".format(data)
                        continue
                    if dtype != "event":
                        continue
                    if devent == "health":
                        continue
                    if devent == "agent" and dpayload.get("stream") == "assistant":
                        delta = dpayload.get("data", {}).get("delta", "")
                        if delta:
                            full_text += delta
                    if devent == "chat" and dpayload.get("state") == "final":
                        msg_content = dpayload.get("message", {})
                        if msg_content.get("content"):
                            for block in msg_content["content"]:
                                if block.get("type") == "text":
                                    final_text = block.get("text", "")
                                    if final_text and len(final_text) >= len(full_text):
                                        full_text = final_text
                        break
                except asyncio.TimeoutError:
                    debug_log.append(f"timeout at iteration {i}")
                    break
                except Exception as ws_err:
                    debug_log.append(f"ws_error: {type(ws_err).__name__}: {ws_err}")
                    break
    except Exception as e:
        return "", "WS connect failed: {}: {}".format(type(e).__name__, e)

    if not full_text:
        return "", "No reply (debug tail: {})".format(" | ".join(debug_log[-3:]))
    return full_text, None
