"""Panel login and authentication middleware."""
from __future__ import annotations

import logging
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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

logger = logging.getLogger(__name__)


def register_panel_auth(
    app: FastAPI,
    *,
    auth_cookie: str,
    auth_token: str,
    panel_password: str,
    audit_fn,
    safe_filename_fn,
) -> None:
    """Attach login page and panel auth middleware to ``app``."""

    @app.get("/login")
    async def login_page():
        return HTMLResponse(LOGIN_HTML)

    @app.post("/do_login")
    async def do_login(request: Request):
        body = await request.body()
        params = parse_qs(body.decode())
        pwd = params.get("password", [""])[0]
        if pwd == panel_password:
            audit_fn("login_success", request)
            resp = RedirectResponse("/", status_code=302)
            resp.set_cookie(auth_cookie, auth_token, max_age=86400 * 30, httponly=True)
            return resp
        audit_fn("login_failure", request, pwd_len=len(pwd))
        return RedirectResponse("/login?err=1", status_code=302)

    @app.middleware("http")
    async def error_logging_middleware(request: Request, call_next):
        """Log unhandled request exceptions to rotated error logs."""
        try:
            return await call_next(request)
        except Exception:
            logger.exception("Unhandled request error: %s %s", request.method, request.url.path)
            raise

    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):
        path = request.url.path
        if path in ("/login", "/do_login") or path.startswith("/static"):
            return await call_next(request)
        # Gateway API routes handle their own auth (Bearer token)
        if path.startswith("/v1/") or path in ("/health", "/gateway/status"):
            return await call_next(request)
        # Probe agent uses its own X-Probe-Token header
        if path == "/api/probe/report":
            return await call_next(request)
        # Probe install assets (agent.py + install.sh) are public — token is in URL
        if path.startswith("/probe/"):
            return await call_next(request)
        # Publicly readable endpoints (no auth — safe to expose)
        if path in ("/stats", "/api/public/stats"):
            return await call_next(request)
        if request.cookies.get(auth_cookie) != auth_token:
            # Don't audit every redirect (browsers retry, gets noisy), only the
            # ones with an *attempted* but wrong cookie value — that's the
            # interesting signal.
            if request.cookies.get(auth_cookie):
                audit_fn("auth_bad_cookie", request)
            return RedirectResponse("/login", status_code=302)
        # Authenticated. Guard /api/account/{filename}/* against path traversal:
        # ``filename`` is a URL path segment that downstream handlers concatenate
        # into the ``accounts/`` directory. A panel user (or anyone who guesses
        # the panel password) could otherwise write to ``data/secrets.json`` by
        # POSTing ``/api/account/..%2F..%2Fdata%2Fsecrets/login``.
        if path.startswith("/api/account/"):
            # path is like /api/account/{filename}/...
            segs = path.split("/", 4)   # ['', 'api', 'account', filename, rest?]
            if len(segs) >= 4 and segs[3]:
                if not safe_filename_fn(segs[3]):
                    audit_fn("path_traversal_blocked", request, filename=segs[3])
                    return JSONResponse(
                        {"error": "invalid account filename"}, status_code=400,
                    )
        return await call_next(request)
