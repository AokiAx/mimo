#!/usr/bin/env python3
"""
MiMo Studio 自动登录 & Cookie 管理
====================================
功能:
  1. 检测 cookie 有效性
  2. 自动登录 (纯 HTTP API，不需要浏览器)
  3. 保存/加载 cookies
  4. 提供 curl-friendly cookie header

用法:
  # 检查 cookie 状态
  python3 mimo_auth.py status

  # 强制重新登录
  python3 mimo_auth.py login

  # 获取当前有效的 cookie header（供 curl 使用）
  python3 mimo_auth.py cookie-header

  # 获取 cookie JSON（供 mimo_ws_client.py 使用）
  python3 mimo_auth.py cookie-json

  # 检查并自动续期（适合 cron）
  python3 mimo_auth.py auto-refresh

环境变量:
  MIMO_EMAIL     - 小米账号邮箱
  MIMO_PASSWORD  - 小米账号密码
"""

import hashlib
import getpass
import json
import os
import random
import re
import sys
import time
import subprocess
import uuid
import requests
from pathlib import Path
from datetime import datetime, timedelta

# 路径配置
ACCOUNTS_DIR = Path(__file__).resolve().parent.parent / "accounts"
AUTH_CONFIG = Path(__file__).resolve().parent.parent / "tmp" / "mimo_auth_config.json"
MIMO_BASE = "https://aistudio.xiaomimimo.com"
XIAOMI_LOGIN = "https://account.xiaomi.com/pass/serviceLogin"
XIAOMI_AUTH = "https://account.xiaomi.com/pass/serviceLoginAuth2"
SID = "xiaomichatbot"
# The real callback URL is NOT /open-apis/user/mi/get — it is /sts?sign=...&followup=...
# obtained from GET /open-apis/v1/genLoginUrl (302 redirect carries the callback).
# We fetch it dynamically so the sign token stays valid.
CALLBACK_FALLBACK = f"{MIMO_BASE}/sts?followup={MIMO_BASE}/#"
XIAOMI_IDENTITY_LIST = "https://account.xiaomi.com/identity/list"
XIAOMI_VERIFY_PHONE = "https://account.xiaomi.com/identity/auth/verifyPhone"
XIAOMI_VERIFY_EMAIL = "https://account.xiaomi.com/identity/auth/verifyEmail"
XIAOMI_SEND_EMAIL_TICKET = "https://account.xiaomi.com/identity/auth/sendEmailTicket"
XIAOMI_SEND_PHONE_TICKET = "https://account.xiaomi.com/identity/auth/sendPhoneTicket"
XIAOMI_RESULT_CHECK = "https://account.xiaomi.com/identity/result/check"

# Cookie 有效期阈值（秒）
COOKIE_WARN_DAYS = 7   # 低于 7 天警告
COOKIE_EXPIRE_BUFFER = 2 * 86400  # 低于 2 天自动刷新


def load_config():
    """加载或创建配置文件"""
    if AUTH_CONFIG.exists():
        with open(AUTH_CONFIG) as f:
            return json.load(f)
    return {}


def save_config(config):
    """保存配置"""
    AUTH_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    with open(AUTH_CONFIG, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_credentials():
    """获取登录凭据（优先环境变量，其次配置文件）"""
    email = os.environ.get("MIMO_EMAIL")
    password = os.environ.get("MIMO_PASSWORD")

    if not email or not password:
        config = load_config()
        email = email or config.get("email")
        password = password or config.get("password")

    return email, password


def _cookie_path(email):
    """根据邮箱生成 cookie 文件路径: accounts/<email>.json"""
    return ACCOUNTS_DIR / f"{email}.json"


def load_cookies(email=""):
    """加载 cookies 文件"""
    path = _cookie_path(email)
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


def save_cookies(email, cookies):
    """保存 cookies 文件"""
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(_cookie_path(email), "w") as f:
        json.dump(cookies, f, indent=2, ensure_ascii=False)


def get_cookie_header(cookies=None, domain_filter="xiaomimimo", email=""):
    """生成 curl 友好的 cookie header"""
    if cookies is None:
        cookies = load_cookies(email)
    parts = []
    for c in cookies:
        if domain_filter in c.get("domain", ""):
            parts.append(f"{c['name']}={c['value']}")
    return "; ".join(parts)


def check_cookie_status(email=""):
    """检查 cookie 状态"""
    cookies = load_cookies(email)
    if not cookies:
        return {"valid": False, "reason": "no_cookies", "cookies": []}

    now = time.time()
    xiaomi_cookies = [c for c in cookies if "xiaomimimo" in c.get("domain", "")]

    if not xiaomi_cookies:
        return {"valid": False, "reason": "no_domain_cookies", "cookies": cookies}

    # 检查过期时间
    min_expiry = float("inf")
    for c in xiaomi_cookies:
        exp = c.get("expires", -1)
        if exp > 0:
            min_expiry = min(min_expiry, exp)

    if min_expiry == float("inf"):
        # 全是 session cookies，无法判断
        return {"valid": True, "reason": "session_only", "expires_in_days": None, "cookies": cookies}

    remaining = min_expiry - now
    remaining_days = remaining / 86400

    if remaining <= 0:
        return {"valid": False, "reason": "expired", "cookies": cookies}
    elif remaining < COOKIE_EXPIRE_BUFFER:
        return {"valid": False, "reason": "expiring_soon", "expires_in_days": remaining_days, "cookies": cookies}
    elif remaining_days < COOKIE_WARN_DAYS:
        return {"valid": True, "reason": "warning", "expires_in_days": remaining_days, "cookies": cookies}
    else:
        return {"valid": True, "reason": "ok", "expires_in_days": remaining_days, "cookies": cookies}


def _fetch_callback(session):
    """Fetch the real callback URL from /open-apis/v1/genLoginUrl.

    The server returns a 302 redirect whose Location header contains a
    callback parameter with a signed /sts?sign=... URL.  We must use this
    exact callback (including the sign) when POSTing to serviceLoginAuth2.
    Returns the callback URL string or None on failure.
    """
    try:
        resp = session.get(
            f"{MIMO_BASE}/open-apis/v1/genLoginUrl",
            allow_redirects=False,
            timeout=10,
        )
        location = resp.headers.get("Location", "")
        if not location:
            return None
        # Extract callback= value from the redirect URL
        from urllib.parse import urlparse, parse_qs, unquote
        parsed = urlparse(location)
        params = parse_qs(parsed.query)
        callback = params.get("callback", [None])[0]
        return callback
    except Exception:
        return None


def _xiaomi_get_sign(session, callback_url):
    """Step 1: 获取 _sign 和初始 cookies"""
    session.cookies.set("userId", "", domain="account.xiaomi.com")
    resp = session.get(
        XIAOMI_LOGIN,
        params={"sid": SID, "callback": callback_url, "_json": "true"},
        allow_redirects=True,
        timeout=15,
    )
    text = resp.text.replace("&&&START&&&", "")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None, resp
    return data.get("_sign", ""), resp


def _send_email_code(session, flag=8):
    """触发发送验证码邮件/短信

    浏览器 HAR 显示实际流程分两步:
      1. GET /identity/auth/verifyEmail?_flag=8&_json=true — 准备/初始化
      2. POST /identity/auth/sendEmailTicket — 实际触发发送
    仅调用 GET verifyEmail 不会真正发送邮件。
    """
    url = XIAOMI_VERIFY_PHONE if flag == 4 else XIAOMI_VERIFY_EMAIL
    # Step 1: 初始化
    resp = session.get(
        url,
        params={"_flag": flag, "_json": "true"},
        headers={"x-requested-with": "XMLHttpRequest"},
        timeout=15,
    )
    text = resp.text.replace("&&&START&&&", "")
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        result = {}

    # Step 2: 实际触发发送（HAR entry [262]）
    ts = int(time.time() * 1000)
    resp2 = session.post(
        f"{XIAOMI_SEND_EMAIL_TICKET}?_dc={ts}",
        data={"retry": "0", "icode": "", "_json": "true"},
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "x-requested-with": "XMLHttpRequest",
        },
        timeout=15,
    )
    text2 = resp2.text.replace("&&&START&&&", "")
    try:
        result2 = json.loads(text2)
    except json.JSONDecodeError:
        result2 = {}

    return result2 or result


def _identity_list(session, sid, context):
    """获取安全验证方式（短信/邮箱），获取 userId 等信息
    sid: service id
    context: 从 notificationUrl 的 query 参数中获取
    """
    resp = session.get(
        XIAOMI_IDENTITY_LIST,
        params={"sid": sid, "supportedMask": "0", "_locale": "zh_CN", "context": context, "_json": "true"},
        headers={"x-requested-with": "XMLHttpRequest"},
        timeout=15,
    )
    text = resp.text.replace("&&&START&&&", "")
    try:
        data = json.loads(text)
    except Exception:
        return None, resp
    # flag/options: 4=手机短信, 8=邮箱
    return data, resp


def _verify_code(session, flag, code):
    """提交验证码

    浏览器 HAR 显示 POST /identity/auth/verifyEmail 的 body 不含 context，
    trust=false。
    返回值包含 identityToken 和 _sign（用于后续 result/check）。
    """
    url = XIAOMI_VERIFY_PHONE if flag == 4 else XIAOMI_VERIFY_EMAIL
    ts = int(time.time() * 1000)
    post_data = {
        "_flag": flag,
        "ticket": code,
        "trust": "false",
        "_json": "true",
    }
    resp = session.post(
        f"{url}?_dc={ts}",
        data=post_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "x-requested-with": "XMLHttpRequest",
        },
        timeout=15,
    )
    text = resp.text.replace("&&&START&&&", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise Exception(f"验证码响应解析失败: {text[:200]}")


def _identity_result_check(session, callback, user_id, sid, identity_token, sign):
    """验证通过后调用 result/check 获取最终跳转 URL

    HAR 显示完整流程:
      GET /identity/result/check?callback=<serviceLoginAuth2/end URL>&userId=...
      → 302 to /pass/serviceLoginAuth2/end?...&_authenticationToken=...&_signature=...
      → 302 to /sts?sign=...&followup=...
    """
    # callback 是 serviceLoginAuth2/end?userId=...&sid=...&callback=<原始callback>
    # 但 HAR 显示它是在 verifyEmail 响应中或者由前端构造的
    # 实际上 result/check 的 callback 参数是: serviceLoginAuth2/end?userId=X&sid=Y&callback=<原始callback>
    result_check_url = XIAOMI_RESULT_CHECK
    params = {
        "callback": callback,
        "userId": user_id,
        "sid": sid,
        "tokenType": "pwdLogin",
        "identityToken": identity_token,
        "_sign": sign,
    }
    resp = session.get(
        result_check_url,
        params=params,
        allow_redirects=True,
        timeout=15,
    )
    return resp


def _handle_verification(session, notification_url="", original_callback="", user_id_from_auth=""):
    """处理身份安全验证流程（短信/邮箱验证码）

    完整流程（从 HAR 逆向）:
      1. GET /identity/list → 获取 flag, externalId(userId)
      2. GET /identity/auth/verifyEmail?_flag=8 → 触发发送验证码
      3. 用户输入验证码
      4. POST /identity/auth/verifyEmail → 提交验证码，获取 identityToken, _sign
      5. GET /identity/result/check → 302 到 serviceLoginAuth2/end
      6. 跟随 redirect 链获取 serviceToken
    """
    from urllib.parse import urlparse, parse_qs, quote

    context = ""
    if notification_url:
        parsed = urlparse(notification_url)
        params = parse_qs(parsed.query)
        context = params.get("context", [""])[0]

    if not context:
        raise Exception("无法获取验证上下文(context)")

    # Step 1: 获取验证方式和 userId
    data, resp = _identity_list(session, SID, context)
    if not data or data.get("code") not in (0, 2):
        raise Exception(f"获取验证方式失败: {resp.text[:200]}")

    options = data.get("options", [])
    flag = options[0] if options else data.get("flag", 4)
    user_id = data.get("externalId", "") or user_id_from_auth

    if flag == 4:
        method = "短信"
    elif flag == 8:
        method = "邮箱"
    else:
        method = f"未知({flag})"

    # Step 2: 触发发送验证码（浏览器实际调用 GET verifyEmail）
    print(f"[login] 需要安全验证，验证方式: {method}")
    print(f"[login] 正在发送验证码...")
    send_result = _send_email_code(session, flag)
    if send_result and send_result.get("code") == 0:
        print(f"[login] 验证码已发送到{method}")
    else:
        # 即使发送失败也继续（可能 identity/list 已经触发了）
        print(f"[login] 验证码发送请求已提交（code={send_result})")

    # Step 3: 等待用户输入验证码
    for attempt in range(3):
        code = input(f"[login] 请输入{method}验证码（第{attempt+1}次，还剩{3-attempt}次）: ").strip()
        if not code:
            print("[login] 验证码不能为空")
            continue

        # Step 4: 提交验证码
        verify_data = _verify_code(session, flag, code)

        if verify_data.get("code") == 0:
            identity_token = verify_data.get("identityToken", "")
            verify_sign = verify_data.get("_sign", "")
            location = verify_data.get("location", "")

            if location:
                # 直接返回 location（有些情况下 verifyEmail 直接返回 location）
                print("[login] 验证码验证成功")
                return location

            if identity_token:
                # Step 5: 调用 result/check 获取跳转 URL
                print("[login] 验证码验证成功，获取登录跳转...")

                # 构造 result/check 的 callback:
                # serviceLoginAuth2/end?userId=X&sid=Y&callback=<原始callback>
                end_callback = (
                    f"{XIAOMI_AUTH}/end"
                    f"?userId={user_id}"
                    f"&sid={SID}"
                    f"&callback={quote(original_callback, safe='')}"
                )

                result_resp = _identity_result_check(
                    session, end_callback, user_id, SID, identity_token, verify_sign
                )

                # result/check 返回 302 到 serviceLoginAuth2/end
                # serviceLoginAuth2/end 返回 302 到 sts?sign=...
                # 最终跳转链会设置 serviceToken cookie
                # 返回最终 URL 作为 location
                final_url = result_resp.url
                if "sts" in final_url or "aistudio" in final_url:
                    print("[login] 获取登录跳转成功")
                    return final_url

                # 如果 redirect 链没有正确跟踪，手动提取
                # 尝试从 result/check 响应中获取 redirect
                if result_resp.history:
                    for hist_resp in result_resp.history:
                        loc = hist_resp.headers.get("Location", "")
                        if "sts" in loc or "aistudio" in loc:
                            return loc
                    # 返回最后一个 history 的 location
                    last_loc = result_resp.history[-1].headers.get("Location", "")
                    if last_loc:
                        return last_loc

                raise Exception(f"result/check 未返回有效跳转: final_url={final_url}")

            raise Exception("验证码验证成功但没有返回 location 或 identityToken")

        elif verify_data.get("code") == 87001:
            print("[login] 验证码错误，请重试")
        else:
            desc = verify_data.get("desc", verify_data.get("code", "unknown"))
            raise Exception(f"验证码验证失败: {desc}")

    raise Exception("验证码错误次数过多")


def _xiaomi_authenticate(session, email, password, sign, callback):
    """Step 2: 提交登录表单"""
    pw_hash = hashlib.md5(password.encode()).hexdigest().upper()
    post_data = {
        "sid": SID,
        "hash": pw_hash,
        "callback": callback,
        "qs": f"%3Fsid%3D{SID}%26_json%3Dtrue",
        "user": email,
        "_json": "true",
    }
    if sign:
        post_data["_sign"] = sign

    resp = session.post(
        XIAOMI_AUTH,
        data=post_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "x-requested-with": "XMLHttpRequest",
        },
        allow_redirects=False,
        timeout=15,
    )
    text = resp.text.replace("&&&START&&&", "")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise Exception(f"登录响应解析失败: {text[:200]}")
    return data


def _xiaomi_exchange_token(session, location):
    """Step 3: 用 location URL 换取 serviceToken"""
    resp = session.get(location, allow_redirects=True, timeout=15)
    # serviceToken 在 redirect 链的 Set-Cookie 中
    return resp


def _format_cookies(session):
    """将 session cookies 转换为标准 cookie 文件格式"""
    formatted = []
    for c in session.cookies:
        formatted.append({
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path or "/",
            "expires": c.expires if c.expires else -1,
            "httpOnly": bool(getattr(c, "rest", {}).get("httpOnly", False)),
            "secure": bool(c.secure),
            "sameSite": getattr(c, "rest", {}).get("sameSite", "Lax") or "Lax",
        })
    return formatted


def do_login(email, password):
    """纯 HTTP 登录小米 SSO，返回 xiaomichatbot cookies"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    # Step 0: Fetch the real callback URL from /open-apis/v1/genLoginUrl
    print("[login] 获取 callback URL...")
    callback = _fetch_callback(session)
    if not callback:
        raise Exception("获取 callback URL 失败（genLoginUrl 未返回有效 redirect）")
    print(f"[login] callback: {callback[:80]}...")

    # Step 1: 获取 _sign
    print("[login] 获取登录签名...")
    sign, init_resp = _xiaomi_get_sign(session, callback)
    if not sign:
        raise Exception(f"获取 _sign 失败，响应: {init_resp.text[:200]}")

    # Step 2: 认证
    print("[login] 提交登录信息...")
    auth_data = _xiaomi_authenticate(session, email, password, sign, callback)

    code = auth_data.get("code", 0)
    result = auth_data.get("result", "")
    security_status = auth_data.get("securityStatus", 0)

    if auth_data.get("notificationUrl") and not auth_data.get("location"):
        # 需要安全验证（短信/邮箱验证码），即使 result=="ok" 也可能需要
        location = _handle_verification(
            session,
            notification_url=auth_data.get("notificationUrl", ""),
            original_callback=callback,
            user_id_from_auth=auth_data.get("userId", ""),
        )
    elif result == "ok":
        location = auth_data.get("location", "")
        if not location:
            print(f"[login] debug auth_data: {json.dumps(auth_data, ensure_ascii=False)[:500]}")
            raise Exception("登录成功但没有返回 location URL")
    else:
        desc = auth_data.get("desc", auth_data.get("code", "unknown"))
        raise Exception(f"登录失败: {desc}")

    print("[login] 获取 serviceToken...")
    # Step 3: 用 location 换 serviceToken
    _xiaomi_exchange_token(session, location)

    # 检查是否拿到 serviceToken
    st = None
    for c in session.cookies:
        if c.name == "serviceToken" and "xiaomimimo" in (c.domain or ""):
            st = c.value
            break
    if not st:
        # 也检查通用域名
        for c in session.cookies:
            if c.name == "serviceToken":
                st = c.value
                break

    if not st:
        raise Exception("登录流程完成但未获取到 serviceToken，可能需要验证码")

    cookies = _format_cookies(session)

    # 确保有 xiaomimimo 域的 cookie
    has_mimo_cookie = any("xiaomimimo" in c.get("domain", "") for c in cookies)
    if not has_mimo_cookie:
        # 手动构造 xiaomimimo 域的 serviceToken cookie
        mimo_cookies = []
        for c in session.cookies:
            if c.name in ("serviceToken", "userId"):
                mimo_cookies.append({
                    "name": c.name,
                    "value": c.value,
                    "domain": ".xiaomimimo.com",
                    "path": "/",
                    "expires": c.expires if c.expires else -1,
                    "httpOnly": c.name == "serviceToken",
                    "secure": False,
                    "sameSite": "Lax",
                })
        # xiaomichatbot_ph 需要从 xiaomimimo 页面获取
        cookies.extend(mimo_cookies)

    # 尝试获取 xiaomichatbot_ph
    has_ph = any(c["name"] == "xiaomichatbot_ph" for c in cookies)
    if not has_ph:
        ph_val = _fetch_ph(session, auth_data)
        if ph_val:
            cookies.append({
                "name": "xiaomichatbot_ph",
                "value": ph_val,
                "domain": ".xiaomimimo.com",
                "path": "/",
                "expires": -1,
                "httpOnly": False,
                "secure": False,
                "sameSite": "Lax",
            })
            print("[login] 已获取 xiaomichatbot_ph")

    # 补充 GA cookies（浏览器自动设置，纯 HTTP 不会触发）
    has_ga = any(c["name"] == "_ga" for c in cookies)
    if not has_ga:
        ga_id = str(random.randint(100000000, 999999999))
        ts = int(time.time())
        expiry = ts + 86400 * 365 * 2
        cookies.append({
            "name": "_ga",
            "value": f"GA1.1.{ga_id}.{ts}",
            "domain": ".xiaomi.com", "path": "/",
            "expires": expiry, "httpOnly": False, "secure": False, "sameSite": "Lax",
        })
        cookies.append({
            "name": f"_ga_XWN774PE8J",
            "value": f"GS2.1.s{ts}$o1$g1$t{ts}$j60$l0$h0",
            "domain": ".xiaomi.com", "path": "/",
            "expires": expiry, "httpOnly": False, "secure": False, "sameSite": "Lax",
        })

    return cookies


def _fetch_ph(session, auth_data):
    """尝试获取 xiaomichatbot_ph 值"""
    # 方法1: 访问 MiMo API 看是否自动设置
    try:
        resp = session.get(
            f"{MIMO_BASE}/open-apis/user/mi/get",
            timeout=10,
        )
        for c in session.cookies:
            if c.name == "xiaomichatbot_ph":
                val = c.value
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                return val
    except Exception:
        pass

    # 方法2: 访问 MiMo 首页
    try:
        resp = session.get(MIMO_BASE, timeout=10)
        for c in session.cookies:
            if c.name == "xiaomichatbot_ph":
                val = c.value
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                return val
    except Exception:
        pass

    return None


# === Web-driven login (panel-friendly, non-interactive) ===
#
# The CLI flow above uses ``input()`` for the 2FA verification code, which
# is fine from a terminal but useless from a web request. The two functions
# below (``web_start_login`` / ``web_submit_code``) implement the same SSO
# state machine but suspend at the 2FA step so the panel can collect the
# code in a second HTTP call. Pending sessions live in memory, keyed by a
# random session_id, and expire after ``_PENDING_TTL`` seconds.

_PENDING_LOGINS: "dict[str, dict]" = {}
_PENDING_TTL = 300  # 5 min


def _gc_pending_logins():
    now = time.time()
    for sid in list(_PENDING_LOGINS.keys()):
        if _PENDING_LOGINS[sid]["expires_at"] < now:
            del _PENDING_LOGINS[sid]


def _finish_login(session, location, auth_data):
    """Shared post-2FA / post-no-2FA path: exchange location → serviceToken,
    format cookies, ensure ``xiaomichatbot_ph`` and GA cookies are present."""
    _xiaomi_exchange_token(session, location)

    st = None
    for c in session.cookies:
        if c.name == "serviceToken" and "xiaomimimo" in (c.domain or ""):
            st = c.value
            break
    if not st:
        for c in session.cookies:
            if c.name == "serviceToken":
                st = c.value
                break
    if not st:
        raise Exception("登录流程完成但未获取到 serviceToken")

    cookies = _format_cookies(session)

    has_mimo_cookie = any("xiaomimimo" in c.get("domain", "") for c in cookies)
    if not has_mimo_cookie:
        for c in session.cookies:
            if c.name in ("serviceToken", "userId"):
                cookies.append({
                    "name": c.name, "value": c.value,
                    "domain": ".xiaomimimo.com", "path": "/",
                    "expires": c.expires if c.expires else -1,
                    "httpOnly": c.name == "serviceToken",
                    "secure": False, "sameSite": "Lax",
                })

    if not any(c["name"] == "xiaomichatbot_ph" for c in cookies):
        ph_val = _fetch_ph(session, auth_data)
        if ph_val:
            cookies.append({
                "name": "xiaomichatbot_ph", "value": ph_val,
                "domain": ".xiaomimimo.com", "path": "/",
                "expires": -1, "httpOnly": False, "secure": False,
                "sameSite": "Lax",
            })

    if not any(c["name"] == "_ga" for c in cookies):
        ga_id = str(random.randint(100000000, 999999999))
        ts = int(time.time())
        expiry = ts + 86400 * 365 * 2
        cookies.append({"name": "_ga", "value": f"GA1.1.{ga_id}.{ts}",
                        "domain": ".xiaomi.com", "path": "/", "expires": expiry,
                        "httpOnly": False, "secure": False, "sameSite": "Lax"})
        cookies.append({"name": "_ga_XWN774PE8J",
                        "value": f"GS2.1.s{ts}$o1$g1$t{ts}$j60$l0$h0",
                        "domain": ".xiaomi.com", "path": "/", "expires": expiry,
                        "httpOnly": False, "secure": False, "sameSite": "Lax"})

    return cookies


def web_start_login(email, password):
    """Begin a web-driven login. Returns one of:
      {"status": "ok",         "cookies": [...]}            — no 2FA needed
      {"status": "needs_code", "session_id": "...",
                               "method": "email"|"phone"}    — caller must POST code
      {"status": "error",      "error": "..."}
    """
    _gc_pending_logins()
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        })

        callback = _fetch_callback(session)
        if not callback:
            return {"status": "error", "error": "获取 callback URL 失败"}

        sign, init_resp = _xiaomi_get_sign(session, callback)
        if not sign:
            return {"status": "error", "error": "获取 _sign 失败"}

        auth_data = _xiaomi_authenticate(session, email, password, sign, callback)

        # Branch A — needs 2FA
        if auth_data.get("notificationUrl") and not auth_data.get("location"):
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(auth_data.get("notificationUrl", ""))
            context = parse_qs(parsed.query).get("context", [""])[0]
            if not context:
                return {"status": "error", "error": "无法获取验证上下文"}

            data, _ = _identity_list(session, SID, context)
            if not data or data.get("code") not in (0, 2):
                return {"status": "error", "error": "获取验证方式失败"}

            options = data.get("options", [])
            flag = options[0] if options else data.get("flag", 4)
            user_id = data.get("externalId", "") or auth_data.get("userId", "")

            try:
                _send_email_code(session, flag)
            except Exception:
                # 即使首次发送失败也不阻塞 — 用户可重试或验证码已通过其他方式发送
                pass

            session_id = uuid.uuid4().hex
            _PENDING_LOGINS[session_id] = {
                "session": session,
                "callback": callback,
                "flag": flag,
                "user_id": user_id,
                "auth_data": auth_data,
                "expires_at": time.time() + _PENDING_TTL,
            }
            return {
                "status": "needs_code",
                "session_id": session_id,
                "method": "email" if flag == 8 else ("phone" if flag == 4 else "unknown"),
            }

        # Branch B — direct login (no 2FA)
        if auth_data.get("result") == "ok":
            location = auth_data.get("location", "")
            if not location:
                return {"status": "error", "error": "登录成功但无 location"}
            cookies = _finish_login(session, location, auth_data)
            return {"status": "ok", "cookies": cookies}

        desc = auth_data.get("desc", auth_data.get("code", "unknown"))
        return {"status": "error", "error": f"登录失败: {desc}"}

    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


def web_submit_code(session_id, code):
    """Submit verification code. Returns:
      {"status": "ok",         "cookies": [...]}
      {"status": "needs_code", "session_id": "...", "error": "验证码错误..."}
      {"status": "error",      "error": "..."}                         — fatal
    """
    _gc_pending_logins()
    state = _PENDING_LOGINS.get(session_id)
    if not state:
        return {"status": "error", "error": "session_id 无效或已过期"}
    if time.time() > state["expires_at"]:
        del _PENDING_LOGINS[session_id]
        return {"status": "error", "error": "session 已过期"}

    try:
        session = state["session"]
        flag = state["flag"]
        user_id = state["user_id"]
        callback = state["callback"]
        auth_data = state["auth_data"]

        verify_data = _verify_code(session, flag, code)

        if verify_data.get("code") == 87001:
            return {
                "status": "needs_code", "session_id": session_id,
                "method": "email" if flag == 8 else "phone",
                "error": "验证码错误，请重试",
            }
        if verify_data.get("code") != 0:
            del _PENDING_LOGINS[session_id]
            desc = verify_data.get("desc", verify_data.get("code", "unknown"))
            return {"status": "error", "error": f"验证码验证失败: {desc}"}

        identity_token = verify_data.get("identityToken", "")
        verify_sign = verify_data.get("_sign", "")
        location = verify_data.get("location", "")

        if not location and identity_token:
            from urllib.parse import quote
            end_callback = (
                f"{XIAOMI_AUTH}/end?userId={user_id}&sid={SID}"
                f"&callback={quote(callback, safe='')}"
            )
            result_resp = _identity_result_check(
                session, end_callback, user_id, SID, identity_token, verify_sign,
            )
            location = result_resp.url
            if not ("sts" in location or "aistudio" in location):
                if result_resp.history:
                    for hist_resp in result_resp.history:
                        loc = hist_resp.headers.get("Location", "")
                        if "sts" in loc or "aistudio" in loc:
                            location = loc
                            break

        if not location:
            del _PENDING_LOGINS[session_id]
            return {"status": "error", "error": "验证后未获取跳转 URL"}

        cookies = _finish_login(session, location, auth_data)
        del _PENDING_LOGINS[session_id]
        return {"status": "ok", "cookies": cookies}

    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


# === CLI 命令 ===

def cmd_status():
    """打印 cookie 状态"""
    email, _ = get_credentials()
    if not email:
        email = input("[login] 请输入小米账号邮箱: ").strip()
    if not email:
        print("❌ 未指定邮箱")
        sys.exit(1)

    status = check_cookie_status(email)
    now = datetime.now()

    if status["valid"]:
        if status.get("expires_in_days"):
            print(f"✅ Cookie 有效 | 剩余 {status['expires_in_days']:.1f} 天")
        else:
            print("✅ Cookie 有效 | session-only (无过期时间)")
    else:
        reason = status["reason"]
        if reason == "no_cookies":
            print("❌ 无 cookie 文件")
        elif reason == "no_domain_cookies":
            print("❌ 无 xiaomimimo 域 cookie")
        elif reason == "expired":
            print("❌ Cookie 已过期")
        elif reason == "expiring_soon":
            print(f"⚠️ Cookie 即将过期 | 剩余 {status.get('expires_in_days', 0):.1f} 天")

    # 验证 session 是否仍然有效
    cookie_header = get_cookie_header(email=email)
    if cookie_header:
        import subprocess
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             f"{MIMO_BASE}/open-apis/user/mi/get",
             "-H", f"cookie: {cookie_header}"],
            capture_output=True, text=True, timeout=10
        )
        http_code = result.stdout.strip()
        if http_code == "200":
            print("✅ Session 活跃 (API 验证通过)")
        else:
            print(f"❌ Session 已失效 (HTTP {http_code})")


def cmd_login():
    """执行登录（支持交互式输入账号密码）"""
    email, password = get_credentials()

    if not email:
        email = input("[login] 请输入小米账号邮箱: ").strip()
    if not password:
        password = getpass.getpass("[login] 请输入密码（输入内容不会显示）: ")

    if not email or not password:
        print("❌ 邮箱和密码不能为空")
        sys.exit(1)

    print(f"[login] 使用账号: {email[:3]}***@{email.split('@')[-1]}")
    try:
        cookies = do_login(email, password)
        save_cookies(email, cookies)
        print(f"[login] ✅ 成功！已保存 {len(cookies)} 个 cookies → accounts/{email}.json")

        # 验证
        status = check_cookie_status(email)
        if status.get("expires_in_days"):
            print(f"[login] Cookie 有效期: {status['expires_in_days']:.1f} 天")
    except Exception as e:
        print(f"[login] ❌ 失败: {e}")
        sys.exit(1)


def cmd_cookie_header():
    """输出 cookie header"""
    email, _ = get_credentials()
    if not email:
        email = input("[login] 请输入小米账号邮箱: ").strip()
    if not email:
        print("❌ 未指定邮箱", file=sys.stderr)
        sys.exit(1)

    header = get_cookie_header(email=email)
    if header:
        print(header)
    else:
        print("❌ 无有效 cookie", file=sys.stderr)
        sys.exit(1)


def cmd_cookie_json():
    """输出 cookie JSON"""
    email, _ = get_credentials()
    if not email:
        email = input("[login] 请输入小米账号邮箱: ").strip()
    cookies = load_cookies(email)
    if cookies:
        print(json.dumps(cookies, ensure_ascii=False))
    else:
        print("[]")


def cmd_auto_refresh():
    """自动刷新（适合 cron）"""
    email, password = get_credentials()
    if not email or not password:
        print("❌ 需要刷新但未配置凭据", file=sys.stderr)
        sys.exit(1)

    status = check_cookie_status(email)

    if status["valid"]:
        days = status.get("expires_in_days")
        if days:
            print(f"Cookie 有效，剩余 {days:.1f} 天，无需刷新")
        else:
            print("Cookie 有效 (session-only)")
        return

    print(f"Cookie {status['reason']}，执行自动刷新...")
    try:
        cookies = do_login(email, password)
        save_cookies(email, cookies)
        new_status = check_cookie_status(email)
        print(f"✅ 刷新成功！新有效期: {new_status.get('expires_in_days', '?')} 天")
    except Exception as e:
        print(f"❌ 刷新失败: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    commands = {
        "status": cmd_status,
        "login": cmd_login,
        "cookie-header": cmd_cookie_header,
        "cookie-json": cmd_cookie_json,
        "auto-refresh": cmd_auto_refresh,
    }

    if cmd in commands:
        commands[cmd]()
    else:
        print(f"未知命令: {cmd}")
        print(f"可用命令: {', '.join(commands.keys())}")
        sys.exit(1)


if __name__ == "__main__":
    main()
