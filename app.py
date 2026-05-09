#!/usr/bin/env python3
"""
MiMo Claw/API Management Dashboard - FastAPI Backend
"""
import asyncio
import json
import os
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

# Paths
BASE_DIR = Path(__file__).parent
CLAW_DIR = BASE_DIR / "claw"
COOKIE_FILE = CLAW_DIR / "mimo_cookies.json"
ACCOUNTS_DIR = BASE_DIR / "accounts"
CURRENT_ACCOUNT_FILE = ACCOUNTS_DIR / "_current.json"
MIMO_BASE = "https://aistudio.xiaomimimo.com"

app = FastAPI(title="MiMo Manager")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ── Access Password ──
PANEL_PASSWORD = "Aoki-MiMo"
AUTH_COOKIE = "mimo_panel_auth"
AUTH_TOKEN = "aoki_mimo_2026"


LOGIN_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MiMo Manager - Login</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0e17;color:#c8d6e5;font-family:system-ui,-apple-system,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh}
.login-box{background:#111827;border:1px solid #1e3a5f;border-radius:12px;padding:40px;width:360px;text-align:center}
h1{color:#4fc3f7;font-size:1.5rem;margin-bottom:8px}
.sub{color:#556677;font-size:0.85rem;margin-bottom:24px}
input[type=password]{width:100%;padding:12px 16px;background:#0d1b2a;border:1px solid #1e3a5f;border-radius:8px;color:#e0e0e0;font-size:1rem;margin-bottom:16px;outline:none}
input:focus{border-color:#4fc3f7}
button{width:100%;padding:12px;background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;border:none;border-radius:8px;font-size:1rem;cursor:pointer}
button:hover{opacity:0.9}
.err{color:#ef5350;font-size:0.85rem;margin-bottom:12px;display:none}
</style></head>
<body>
<div class="login-box">
<h1>🤖 MiMo Manager</h1>
<p class="sub">Enter password to continue</p>
<div class="err" id="err">Wrong password</div>
<form method="POST" action="/do_login">
<input type="password" name="password" placeholder="Password" autofocus>
<button type="submit">Login</button>
</form>
</div>
<script>
if(location.search.includes("err=1"))document.getElementById("err").style.display="block";
</script>
</body></html>"""

@app.get("/login")
async def login_page():
    return HTMLResponse(LOGIN_HTML)

@app.post("/do_login")
async def do_login(request: Request):
    body = await request.body()
    from urllib.parse import parse_qs
    params = parse_qs(body.decode())
    pwd = params.get("password", [""])[0]
    if pwd == PANEL_PASSWORD:
        resp = RedirectResponse("/", status_code=302)
        resp.set_cookie(AUTH_COOKIE, AUTH_TOKEN, max_age=86400*30, httponly=True)
        return resp
    return RedirectResponse("/login?err=1", status_code=302)

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in ("/login", "/do_login") or path.startswith("/static"):
        return await call_next(request)
    # Gateway API routes handle their own auth (Bearer token)
    if path.startswith("/v1/") or path in ("/health", "/gateway/status"):
        return await call_next(request)
    # Publicly readable endpoints (no auth — safe to expose)
    if path in ("/stats", "/api/public/stats"):
        return await call_next(request)
    if request.cookies.get(AUTH_COOKIE) == AUTH_TOKEN:
        return await call_next(request)
    return RedirectResponse("/login", status_code=302)

@app.on_event("startup")
async def startup_event():
    """Run account migration on startup and start auto-deploy scheduler."""
    _migrate_legacy_accounts()
    try:
        from claw.auto_deploy import start_scheduler
        start_scheduler()
    except Exception as e:
        print(f"[startup] Failed to start scheduler: {e}")
    try:
        from gateway.runtime import start_probe as start_router_probe
        start_router_probe()
    except Exception as e:
        print(f"[startup] Failed to start gateway probe: {e}")
    try:
        from gateway.vps_probe import start_probe
        start_probe()
    except Exception as e:
        print(f"[startup] Failed to start VPS probe: {e}")

# ──────────── Account management helpers ────────────

def _ensure_accounts_dir():
    """Make sure accounts directory exists."""
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)

def _account_filename(name):
    """Sanitize account name to a safe filename."""
    safe = re.sub(r'[^a-zA-Z0-9_\-]', '_', name).strip('_')
    return safe if safe else "unnamed"

def _get_current_account_name():
    """Read the current account name from _current.json."""
    if not CURRENT_ACCOUNT_FILE.exists():
        return None
    try:
        with open(CURRENT_ACCOUNT_FILE) as f:
            data = json.load(f)
        return data.get("current")
    except (json.JSONDecodeError, ValueError):
        return None

def _set_current_account(filename):
    """Set the current active account by filename (without .json)."""
    _ensure_accounts_dir()
    with open(CURRENT_ACCOUNT_FILE, "w") as f:
        json.dump({"current": filename}, f)

def _load_account_by_filename(filename):
    """Load an account dict by its filename (without .json)."""
    path = ACCOUNTS_DIR / "{}.json".format(filename)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError):
        return None

def _save_account(filename, account_data):
    """Save an account dict to its file."""
    _ensure_accounts_dir()
    path = ACCOUNTS_DIR / "{}.json".format(filename)
    with open(path, "w") as f:
        json.dump(account_data, f, indent=2, ensure_ascii=False)

def _list_account_files():
    """Return list of account filenames (without .json) in accounts dir."""
    _ensure_accounts_dir()
    result = []
    for p in sorted(ACCOUNTS_DIR.glob("*.json")):
        if p.name.startswith("_"):
            continue
        result.append(p.stem)
    return result

def _fetch_user_info(cookies_list):
    """Fetch user info from MiMo API given a cookies list. Returns (user_id, user_name) or (None, None)."""
    parts = []
    for c in cookies_list:
        val = c.get("value", "")
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        parts.append("{0}={1}".format(c.get("name", ""), val))
    cookie_header = "; ".join(parts)

    ph = None
    for c in cookies_list:
        if c["name"] == "xiaomichatbot_ph":
            ph = c["value"]
            if ph.startswith('"') and ph.endswith('"'):
                ph = ph[1:-1]
            break

    url = "{0}/open-apis/user/mi/get".format(MIMO_BASE)
    if ph:
        url = "{0}?xiaomichatbot_ph={1}".format(url, quote(ph, safe=""))

    cmd = ["curl", "-s", "-w", "\nHTTP_%{http_code}", "-X", "GET", url,
           "-H", "cookie: {0}".format(cookie_header),
           "-H", "content-type: application/json"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        lines = r.stdout.strip().split("\n")
        resp_text = "\n".join(lines[:-1])
        resp = json.loads(resp_text)
        if isinstance(resp, dict) and resp.get("code") == 0:
            data = resp.get("data", {})
            return data.get("userId"), data.get("userName")
    except Exception:
        pass
    return None, None

def _migrate_legacy_accounts():
    """On startup: convert legacy raw-cookie files in accounts/ to account format.
    Also import from claw/mimo_cookies.json if accounts/ is empty."""
    _ensure_accounts_dir()
    account_files = _list_account_files()

    # Check each existing file - if it's a raw cookie list, convert it
    for fname in list(account_files):
        path = ACCOUNTS_DIR / "{}.json".format(fname)
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            continue
        # If it's a list (raw cookies), convert to account format
        if isinstance(data, list):
            user_id, user_name = _fetch_user_info(data)
            account = {
                "name": fname,
                "email": "",
                "cookies": data,
                "user_id": user_id or "",
                "user_name": user_name or "",
                "added_at": datetime.now(timezone.utc).isoformat(),
            }
            _save_account(fname, account)

    # If no accounts at all, import from claw/mimo_cookies.json
    account_files = _list_account_files()
    if not account_files and COOKIE_FILE.exists():
        try:
            with open(COOKIE_FILE) as f:
                cookies = json.load(f)
            if isinstance(cookies, list) and cookies:
                user_id, user_name = _fetch_user_info(cookies)
                account = {
                    "name": "default",
                    "email": "",
                    "cookies": cookies,
                    "user_id": user_id or "",
                    "user_name": user_name or "",
                    "added_at": datetime.now(timezone.utc).isoformat(),
                }
                _save_account("default", account)
        except (json.JSONDecodeError, ValueError):
            pass

    # Set current if not set yet
    if not _get_current_account_name():
        acc_files = _list_account_files()
        if acc_files:
            _set_current_account(acc_files[0])

def _get_current_account():
    """Get the current account dict, or None if no current account."""
    name = _get_current_account_name()
    if name:
        return _load_account_by_filename(name)
    return None

# ──────────── Cookie helpers ────────────

def load_cookies():
    """Load cookies from current account, falling back to COOKIE_FILE."""
    account = _get_current_account()
    if account and isinstance(account, dict) and account.get("cookies"):
        return account["cookies"]
    # Fallback to legacy file
    if COOKIE_FILE.exists():
        with open(COOKIE_FILE) as f:
            return json.load(f)
    return []

def get_cookie_parts():
    """Return (cookie_header_str, ph_value) for xiaomimimo domain cookies."""
    cookies = load_cookies()
    parts = []
    ph = None
    for c in cookies:
        if "xiaomimimo" in c.get("domain", ""):
            val = c["value"]
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            parts.append("{0}={1}".format(c["name"], val))
            if c["name"] == "xiaomichatbot_ph":
                ph = val
    if not ph:
        # Try all cookies if none in xiaomimimo domain
        for c in cookies:
            if c["name"] == "xiaomichatbot_ph":
                ph = c["value"]
                if ph.startswith('"') and ph.endswith('"'):
                    ph = ph[1:-1]
                break
    return "; ".join(parts), ph

def get_ph_encoded():
    """Get URL-encoded ph value."""
    _, ph = get_cookie_parts()
    if not ph:
        return None
    return quote(ph, safe="")

def get_cookie_header_all():
    """Build cookie header from ALL cookies (for curl calls)."""
    cookies = load_cookies()
    parts = []
    for c in cookies:
        val = c["value"]
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        parts.append("{0}={1}".format(c["name"], val))
    return "; ".join(parts)

# ──────────── API proxy helpers ────────────

def curl_api(method, path, body=None, with_ph=True):
    """Make API call via curl subprocess."""
    cookie_header = get_cookie_header_all()
    ph_enc = get_ph_encoded()

    url = "{0}{1}".format(MIMO_BASE, path)
    if with_ph and ph_enc:
        sep = "&" if "?" in path else "?"
        url = "{0}{1}xiaomichatbot_ph={2}".format(url, sep, ph_enc)

    cmd = ["curl", "-s", "-w", "\nHTTP_%{http_code}", "-X", method, url,
           "-H", "cookie: {0}".format(cookie_header),
           "-H", "content-type: application/json"]
    if body is not None:
        cmd += ["-d", json.dumps(body, ensure_ascii=False)]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        lines = r.stdout.strip().split("\n")
        code_line = lines[-1] if lines[-1].startswith("HTTP_") else "?"
        resp_text = "\n".join(lines[:-1])
        try:
            resp_json = json.loads(resp_text)
        except (json.JSONDecodeError, ValueError):
            resp_json = resp_text
        return code_line, resp_json
    except Exception as e:
        return "ERROR", str(e)

# ──────────── HTML page ────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html_file = BASE_DIR / "templates" / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

# ──────────── Status overview ────────────

@app.get("/api/status")
async def api_status():
    result = {"claw": {"status": "unknown"}, "cookies": {"status": "unknown"}}

    # Cookie status
    cookies = load_cookies()
    ph_found = any(c["name"] == "xiaomichatbot_ph" for c in cookies)
    result["cookies"] = {
        "status": "ok" if ph_found and cookies else "error",
        "count": len(cookies),
        "has_ph": ph_found,
        "file_exists": COOKIE_FILE.exists(),
    }

    # Claw status
    try:
        code, data = curl_api("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            info = data.get("data", {})
            result["claw"] = {
                "status": "ok" if info.get("status") == "AVAILABLE" else info.get("status", "unknown").lower(),
                "raw_status": info.get("status", ""),
                "message": info.get("message", ""),
                "expire_time": info.get("expireTime", 0),
            }
        else:
            result["claw"] = {"status": "error", "detail": str(data)[:200]}
    except Exception as e:
        result["claw"] = {"status": "error", "detail": str(e)[:200]}

    return result

# ──────────── Claw management ────────────

@app.post("/api/claw/create")
async def claw_create():
    code, data = curl_api("POST", "/open-apis/user/mimo-claw/create", body={})
    success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
    return {"success": success, "code": code, "data": data}

@app.get("/api/claw/status")
async def claw_status():
    code, data = curl_api("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {})
        expire_ms = info.get("expireTime", 0)
        expire_str = ""
        if expire_ms:
            try:
                expire_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_ms / 1000))
            except Exception:
                expire_str = str(expire_ms)
        return {"success": True, "data": {
            "status": info.get("status", ""),
            "message": info.get("message", ""),
            "expireTime": expire_ms,
            "expireStr": expire_str,
        }}
    return {"success": False, "code": code, "data": data}

# ──────────── Claw WebSocket chat ────────────

@app.post("/api/claw/destroy")
async def claw_destroy():
    code, data = curl_api("POST", "/open-apis/user/mimo-claw/destroy", body={})
    success = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
    return {"success": success, "code": code, "data": data}

@app.post("/api/claw/chat")
async def claw_chat(request: Request):
    body = await request.json()
    message = body.get("message", "")
    session_key = body.get("session_key", "agent:main:deploy-" + uuid.uuid4().hex[:8])
    if not message:
        return JSONResponse({"error": "No message"}, status_code=400)

    # Use curl-based approach for WS chat (shell out to a helper)
    cookie_header = get_cookie_header_all()
    ph_enc = get_ph_encoded()

    # Ensure claw available
    curl_api("POST", "/open-apis/user/mimo-claw/create", body={})

    # Get ticket
    code, data = curl_api("GET", "/open-apis/user/ws/ticket")
    ticket = None
    if isinstance(data, dict) and data.get("code") == 0:
        ticket = data.get("data", {}).get("ticket")

    # Get userId
    code2, data2 = curl_api("GET", "/open-apis/user/mi/get", with_ph=False)
    user_id = None
    if isinstance(data2, dict) and data2.get("code") == 0:
        user_id = data2.get("data", {}).get("userId")

    if not ticket or not user_id:
        return JSONResponse({"error": "Failed to get WS ticket/userId"}, status_code=500)

    ws_url = "wss://aistudio.xiaomimimo.com/ws/proxy?ticket={0}&userId={1}".format(ticket, user_id)

    # Run async WS chat
    try:
        import websockets
    except ImportError:
        return JSONResponse({"error": "websockets not installed"}, status_code=500)

    async def ws_chat():
        full_text = ""
        debug_log = []
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
                        return "", "Connect failed: {0} | debug: {1}".format(d, debug_log)
                    break

            session_key_passed = session_key
            msg_id = str(uuid.uuid4())
            await ws.send(json.dumps({
                "type": "req", "id": msg_id, "method": "chat.send",
                "params": {
                    "sessionKey": session_key_passed,
                    "message": message,
                    "deliver": False,
                    "idempotencyKey": msg_id
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
                    if i < 50:  # Log first 50 messages for debug
                        debug_log.append(f"[{i}] type={dtype} event={devent} id={did[:8]} ok={data.get('ok')} payload_keys={list(dpayload.keys()) if isinstance(dpayload, dict) else 'N/A'}")

                    if data.get("type") == "res":
                        if data.get("id") == msg_id and not data.get("ok"):
                            return "", "chat.send error: {0} | debug: {1}".format(data, debug_log)
                        continue
                    if data.get("type") != "event":
                        debug_log.append(f"[{i}] non-event type={dtype}, continuing")
                        continue
                    event = data.get("event", "")
                    payload = data.get("payload", {})
                    if event == "health":
                        continue
                    if event == "agent" and payload.get("stream") == "assistant":
                        delta = payload.get("data", {}).get("delta", "")
                        if delta:
                            full_text += delta
                    if event == "chat" and payload.get("state") == "final":
                        msg_content = payload.get("message", {})
                        if msg_content.get("content"):
                            for block in msg_content["content"]:
                                if block.get("type") == "text":
                                    final_text = block.get("text", "")
                                    if final_text and len(final_text) >= len(full_text):
                                        full_text = final_text
                        break
                    # Catch agent.tool-use events (Claw executing commands)
                    if event == "agent" and payload.get("stream") == "tool-result":
                        tool_data = payload.get("data", {})
                        debug_log.append(f"tool-result: {json.dumps(tool_data, ensure_ascii=False)[:300]}")
                except asyncio.TimeoutError:
                    debug_log.append(f"timeout at iteration {i}")
                    break
                except Exception as ws_err:
                    debug_log.append(f"ws_error at iteration {i}: {type(ws_err).__name__}: {ws_err}")
                    break

            # Write debug to file for inspection
            with open("/tmp/ws_debug.log", "a") as _df:
                _df.write(f"\n=== chat.send {msg_id[:8]} at {time.strftime('%H:%M:%S')} ===\n")
                for entry in debug_log:
                    _df.write(f"  {entry}\n")
                _df.write(f"  full_text={repr(full_text[:200])}\n")
                _df.flush()

            return full_text, None

    text, err = await ws_chat()
    if err:
        return {"success": False, "error": err}
    return {"success": True, "reply": text}

# ──────────── Cookie management ────────────

@app.post("/api/cookie/refresh")
async def cookie_refresh():
    auth_script = CLAW_DIR / "mimo_auth.py"
    if not auth_script.exists():
        return {"success": False, "error": "mimo_auth.py not found"}
    try:
        r = subprocess.run(["python3", str(auth_script), "login"],
                           capture_output=True, text=True, timeout=120,
                           cwd=str(CLAW_DIR))
        return {"success": r.returncode == 0, "stdout": r.stdout[-1000:], "stderr": r.stderr[-500:]}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/cookie/status")
async def cookie_status():
    cookies = load_cookies()
    if not cookies:
        return {"valid": False, "count": 0, "reason": "No cookies file or empty"}
    ph = None
    for c in cookies:
        if c["name"] == "xiaomichatbot_ph":
            ph = c["value"]
            if ph.startswith('"') and ph.endswith('"'):
                ph = ph[1:-1]
            break
    # Test if cookies work by hitting a simple endpoint
    code, data = curl_api("GET", "/open-apis/user/mi/get", with_ph=False)
    valid = code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0
    return {
        "valid": valid,
        "count": len(cookies),
        "has_ph": ph is not None,
        "test_code": code,
        "user_id": data.get("data", {}).get("userId", "") if isinstance(data, dict) else "",
    }

# ──────────── Account management endpoints ────────────

@app.get("/api/accounts")
async def accounts_list():
    """List all accounts."""
    current_name = _get_current_account_name()
    acc_files = _list_account_files()
    result = []
    for fname in acc_files:
        acc = _load_account_by_filename(fname)
        if acc and isinstance(acc, dict):
            result.append({
                "filename": fname,
                "name": acc.get("name", fname),
                "email": acc.get("email", ""),
                "user_id": acc.get("user_id", ""),
                "user_name": acc.get("user_name", ""),
                "added_at": acc.get("added_at", ""),
                "is_current": fname == current_name,
            })
    return {"accounts": result, "current": current_name}

@app.get("/api/accounts/current")
async def accounts_current():
    """Get current account info."""
    acc = _get_current_account()
    if not acc:
        return {"success": False, "error": "No current account set"}
    current_name = _get_current_account_name()
    return {
        "success": True,
        "filename": current_name,
        "name": acc.get("name", ""),
        "email": acc.get("email", ""),
        "user_id": acc.get("user_id", ""),
        "user_name": acc.get("user_name", ""),
        "added_at": acc.get("added_at", ""),
    }

@app.post("/api/accounts/add")
async def accounts_add(request: Request):
    """Add a new account. Body: {"name": "...", "cookies": [...]}"""
    body = await request.json()
    name = body.get("name", "").strip()
    cookies = body.get("cookies", [])
    if not name:
        return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)
    if not cookies or not isinstance(cookies, list):
        return JSONResponse({"success": False, "error": "Cookies array is required"}, status_code=400)

    fname = _account_filename(name)
    # Check if already exists
    if (ACCOUNTS_DIR / "{}.json".format(fname)).exists():
        return JSONResponse({"success": False, "error": "Account '{0}' already exists".format(name)}, status_code=409)

    user_id, user_name = _fetch_user_info(cookies)
    account = {
        "name": name,
        "email": body.get("email", ""),
        "cookies": cookies,
        "user_id": user_id or "",
        "user_name": user_name or "",
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_account(fname, account)

    # If this is the first account, make it current
    if not _get_current_account_name():
        _set_current_account(fname)

    return {
        "success": True,
        "filename": fname,
        "name": name,
        "user_id": user_id or "",
        "user_name": user_name or "",
        "is_current": fname == _get_current_account_name(),
    }

@app.post("/api/accounts/switch")
async def accounts_switch(request: Request):
    """Switch active account. Body: {"name": "filename_or_name"}"""
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)

    # Try as filename first, then search by display name
    fname = _account_filename(name)
    acc = _load_account_by_filename(fname)
    if not acc:
        # Search by display name
        for f in _list_account_files():
            a = _load_account_by_filename(f)
            if a and a.get("name") == name:
                fname = f
                acc = a
                break
    if not acc:
        return JSONResponse({"success": False, "error": "Account not found"}, status_code=404)

    _set_current_account(fname)
    return {
        "success": True,
        "filename": fname,
        "name": acc.get("name", ""),
        "user_id": acc.get("user_id", ""),
        "user_name": acc.get("user_name", ""),
    }

@app.post("/api/accounts/sync-cookies")
async def sync_cookies(request: Request):
    """Sync cookies from browser. Body: {\"cookies\": \"name=value; name2=value2\"}"""
    body = await request.json()
    cookie_str = body.get("cookies", "")
    
    if not cookie_str:
        return {"success": False, "error": "No cookies provided"}
    
    # Parse cookie string
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, value = part.split("=", 1)
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".xiaomimimo.com",
                "path": "/",
                "expires": -1,
                "httpOnly": False,
                "secure": False,
                "sameSite": "None",
            })
    
    # Check for ph cookie
    ph_found = any(c["name"] == "xiaomichatbot_ph" for c in cookies)
    
    if not ph_found:
        return {"success": False, "error": "缺少 xiaomichatbot_ph cookie，请确认已登录 MiMo"}
    
    # Get user info
    user_id, user_name = _fetch_user_info(cookies)
    
    # Save as account
    name = body.get("name", "default")
    email = body.get("email", "")
    fname = _account_filename(name)
    account = {
        "name": name,
        "email": email,
        "cookies": cookies,
        "user_id": user_id or "",
        "user_name": user_name or "",
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_account(fname, account)
    _set_current_account(fname)
    
    return {
        "success": True,
        "user_id": user_id or "",
        "user_name": user_name or "",
        "cookie_count": len(cookies),
    }

@app.post("/api/accounts/delete")
async def accounts_delete(request: Request):
    """Delete an account. Body: {"name": "filename_or_name"}"""
    body = await request.json()
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)

    # Try as filename first, then search by display name
    fname = _account_filename(name)
    path = ACCOUNTS_DIR / "{}.json".format(fname)
    if not path.exists():
        # Search by display name
        for f in _list_account_files():
            a = _load_account_by_filename(f)
            if a and a.get("name") == name:
                fname = f
                path = ACCOUNTS_DIR / "{}.json".format(fname)
                break
    if not path.exists():
        return JSONResponse({"success": False, "error": "Account not found"}, status_code=404)

    was_current = fname == _get_current_account_name()
    path.unlink()

    # If we deleted the current account, switch to another
    if was_current:
        remaining = _list_account_files()
        if remaining:
            _set_current_account(remaining[0])
        else:
            # Remove current file
            if CURRENT_ACCOUNT_FILE.exists():
                CURRENT_ACCOUNT_FILE.unlink()

    return {"success": True, "deleted": fname, "was_current": was_current}

# ──────────── Auto-deploy endpoints ────────────

def switch_to_account(account_filename: str) -> bool:
    """Programmatically switch to an account. Returns True on success."""
    accounts_dir = ACCOUNTS_DIR
    account_file = accounts_dir / f"{account_filename}.json"
    if not account_file.exists():
        return False
    try:
        data = json.loads(account_file.read_text(encoding="utf-8"))
        cookies = data.get("cookies", [])
        if not cookies:
            return False
        # Write to cookie file
        cookie_list = []
        for c in cookies:
            cookie_list.append({
                "name": c.get("name", ""),
                "value": c.get("value", ""),
                "domain": c.get("domain", ".xiaomimimo.com"),
                "path": c.get("path", "/"),
            })
        COOKIE_FILE.write_text(json.dumps(cookie_list, indent=2, ensure_ascii=False), encoding="utf-8")
        # Update current account
        CURRENT_ACCOUNT_FILE.write_text(json.dumps({"current": account_filename}, ensure_ascii=False), encoding="utf-8")
        return True
    except Exception as e:
        print(f"[switch_to_account] Error: {e}")
        return False


@app.get("/api/auto-deploy/config")
async def auto_deploy_config():
    """Get auto-deploy configuration."""
    from claw.auto_deploy import load_config
    return load_config()


@app.post("/api/auto-deploy/config")
async def auto_deploy_config_update(request: Request):
    """Update auto-deploy configuration. Body: full config or partial update."""
    from claw.auto_deploy import load_config, save_config
    body = await request.json()
    cfg = load_config()

    # Update global settings
    # (none currently)

    # Update account settings
    if "accounts" in body:
        for acc_name, acc_cfg in body["accounts"].items():
            if acc_name not in cfg["accounts"]:
                cfg["accounts"][acc_name] = {}
            cfg["accounts"][acc_name].update(acc_cfg)

    save_config(cfg)
    return {"success": True, "config": cfg}


@app.post("/api/auto-deploy/account/{account_filename}")
async def auto_deploy_account_update(account_filename: str, request: Request):
    """Update a single account's deploy config."""
    from claw.auto_deploy import load_config, save_config
    body = await request.json()
    cfg = load_config()

    if "accounts" not in cfg:
        cfg["accounts"] = {}
    if account_filename not in cfg["accounts"]:
        cfg["accounts"][account_filename] = {
            "enabled": False,
            "schedule_time": "03:00",
            "interval_hours": 24,
            "port": 8800,
        }
    cfg["accounts"][account_filename].update(body)
    save_config(cfg)
    return {"success": True, "account": cfg["accounts"][account_filename]}


@app.post("/api/auto-deploy/trigger/{account_filename}")
async def auto_deploy_trigger(account_filename: str):
    """Manually trigger deployment for an account."""
    from claw.auto_deploy import trigger_deploy
    result = trigger_deploy(account_filename)
    return result


@app.post("/api/auto-deploy/cancel/{account_filename}")
async def auto_deploy_cancel(account_filename: str):
    """Cancel an active deployment."""
    from claw.auto_deploy import cancel_deploy
    return cancel_deploy(account_filename)


@app.get("/api/auto-deploy/status")
async def auto_deploy_status():
    """Get deployment status for all accounts."""
    from claw.auto_deploy import get_deploy_status, get_scheduler_status
    return {
        "deploys": get_deploy_status(),
        "scheduler": get_scheduler_status(),
    }


@app.get("/api/auto-deploy/status/{account_filename}")
async def auto_deploy_account_status(account_filename: str):
    """Get deployment status for a specific account."""
    from claw.auto_deploy import get_deploy_status
    return get_deploy_status(account_filename)


@app.get("/api/auto-deploy/history/{account_filename}")
async def auto_deploy_history(account_filename: str):
    """Get run history for a specific account."""
    from claw.auto_deploy import get_run_history
    return {"history": get_run_history(account_filename)}


# ──────────── Gateway API endpoints ────────────

@app.get("/api/gateway/status")
async def gateway_status():
    """Gateway status overview for dashboard."""
    try:
        from gateway.runtime import get_router_status
        return get_router_status()
    except ImportError:
        return {"error": "Gateway module not installed"}


@app.get("/api/gateway/backends")
async def gateway_backends():
    """List all backend servers with health/routing info."""
    try:
        from gateway.runtime import get_all_backends
        return {"backends": get_all_backends()}
    except ImportError:
        return {"backends": []}


@app.post("/api/gateway/backends/{backend_id}/toggle")
async def gateway_backend_toggle(backend_id: str):
    """Enable/disable a backend."""
    try:
        from gateway.runtime import toggle_backend
        result = toggle_backend(backend_id)
        return result
    except ImportError:
        return {"success": False, "error": "Gateway module not installed"}


@app.post("/api/gateway/backends/reload")
async def gateway_backends_reload():
    """Re-read auto_deploy.json and rebuild the backend registry."""
    try:
        from gateway.runtime import reload_backends
        count = reload_backends()
        return {"success": True, "backends": count}
    except ImportError:
        return {"success": False, "error": "Gateway module not installed"}


@app.get("/api/gateway/metrics")
async def gateway_metrics():
    """Request metrics for the metrics page."""
    try:
        from gateway.metrics import get_metrics_summary
        return get_metrics_summary()
    except ImportError:
        return {"error": "Gateway module not installed"}


@app.get("/api/gateway/metrics/hourly")
async def gateway_metrics_hourly(hours: int = 24):
    """24h request histogram (or N hours), oldest bucket first."""
    try:
        from gateway.metrics import get_hourly_buckets
        return {"buckets": get_hourly_buckets(hours=max(1, min(int(hours), 168)))}
    except ImportError:
        return {"buckets": []}


@app.get("/api/gateway/metrics/backends")
async def gateway_metrics_backends(hours: int = 24):
    """Per-backend stats (count, success rate, latency, tokens) over the window."""
    try:
        from gateway.metrics import get_backend_stats
        return {"backends": get_backend_stats(hours=max(1, min(int(hours), 168)))}
    except ImportError:
        return {"backends": []}


@app.get("/api/gateway/metrics/status")
async def gateway_metrics_status(hours: int = 24):
    """HTTP status-code distribution."""
    try:
        from gateway.metrics import get_status_distribution
        return {"distribution": get_status_distribution(hours=max(1, min(int(hours), 168)))}
    except ImportError:
        return {"distribution": {}}


@app.get("/api/public/stats")
async def public_stats():
    """All-time totals — safe to expose without auth."""
    try:
        from gateway.metrics import get_public_totals
        return get_public_totals()
    except ImportError:
        return {"total_requests": 0, "total_tokens": 0}


@app.get("/api/gateway/vps")
async def gateway_vps_status():
    """Latest VPS probe snapshot (jump host + every enabled tunnel)."""
    try:
        from gateway.vps_probe import get_status
        return get_status()
    except ImportError:
        return {"summary": {"total": 0, "up": 0, "down": 0, "unknown": 0}, "targets": []}


@app.post("/api/gateway/vps/refresh")
async def gateway_vps_refresh():
    """Force a probe cycle now (panel "刷新" button)."""
    try:
        from gateway.vps_probe import probe_once
        await probe_once()
        from gateway.vps_probe import get_status
        return get_status()
    except ImportError:
        return {"summary": {"total": 0, "up": 0, "down": 0, "unknown": 0}, "targets": []}


@app.get("/stats", response_class=HTMLResponse)
async def public_stats_page():
    """Public stats page — no auth, safe to share."""
    html_file = BASE_DIR / "templates" / "stats.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


# ──────────── Gateway Proxy Routes ────────────

_GATEWAY_PATHS = {"/v1/chat/completions", "/v1/messages", "/v1/responses", "/v1/models"}


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def gateway_proxy(request: Request, path: str):
    """Proxy OpenAI-compatible requests through the gateway router."""
    full_path = f"/v1/{path}"

    # /v1/models — return static model list
    if full_path == "/v1/models" and request.method == "GET":
        return {
            "object": "list",
            "data": [
                {"id": "mimo-v2.5-pro", "object": "model", "owned_by": "mimo"},
                {"id": "mimo-v2.5", "object": "model", "owned_by": "mimo"},
                {"id": "mimo-v2-flash", "object": "model", "owned_by": "mimo"},
                {"id": "mimo-v2.5-tts", "object": "model", "owned_by": "mimo"},
            ],
        }

    if full_path not in _GATEWAY_PATHS:
        return JSONResponse({"error": {"message": f"Unknown path: {full_path}"}}, status_code=404)

    # Auth: accept Bearer token or panel cookie
    auth = request.headers.get("Authorization", "")
    is_api_auth = auth.startswith("Bearer ") and auth[7:].strip() == "sk-Aoki-MiMo"
    is_panel_auth = request.cookies.get(AUTH_COOKIE) == AUTH_TOKEN
    if not is_api_auth and not is_panel_auth:
        return JSONResponse(
            {"error": {"message": "Missing or invalid Authorization", "type": "auth_error"}},
            status_code=401,
        )

    # Read body
    body = await request.body()
    headers = dict(request.headers)

    # Map source path → adapter name (the new pipeline does protocol
    # encoding/decoding bidirectionally — including streaming SSE frames).
    adapter_name = "openai_chat"
    if full_path == "/v1/messages":
        adapter_name = "anthropic"
    elif full_path == "/v1/responses":
        adapter_name = "openai_responses"

    try:
        from gateway.runtime import dispatch
        return await dispatch(adapter_name, request)
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8088)
