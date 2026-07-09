#!/usr/bin/env python3
"""
小米账号邮箱注册脚本 (纯 HTTP，无浏览器)
=========================================
从 HAR 逆向的完整注册流程:
  1. genLoginUrl → 获取 callback + sign
  2. sendEmailRegTicket → 提交加密邮箱/密码 (触发验证码)
  3. captcha 流程 → 解决图形/滑块验证
  4. sms/quota → 发送邮箱验证码
  5. verifyEmailRegTicket → 提交验证码完成注册
  6. serviceLogin → 自动登录
  7. STS → 换取 MiMo session

用法:
  python register_mimo.py                         # 交互式
  python register_mimo.py --email X --password Y  # 半自动（邮箱码可手输）
  python register_mimo.py --auto                  # 全自动：mail.tm 邮箱 + 等验证码
  python register_mimo.py --auto --count 3        # 连注 3 个
  python register_mimo.py --auto --captcha-key KEY  # 图形码走 2Captcha

环境变量:
  MIMO_EMAIL / MIMO_PASSWORD   - 固定邮箱密码（非 --auto 时）
  CAPTCHA_API_KEY / TWOCAPTCHA_KEY - 2Captcha
  MIMO_MAILBOX=mailtm|imap     - 邮箱后端（默认 mailtm）
  MIMO_IMAP_ADDRESS/HOST/USER/PASSWORD/PORT - IMAP catch-all

依赖: pip install curl_cffi pycryptodome requests
可选全自动: 2Captcha key；mail.tm 免 key（域名可能被小米拒）
"""

import base64
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urlparse

# curl_cffi: 伪造真实浏览器 TLS 指纹，绕过小米风控
from curl_cffi.requests import Session as CurlSession

# ── 路径 ──────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent
# When this file is executed as claw/register_mimo.py shim target, still write
# accounts/ under the repo root.
if ROOT_DIR.name == "claw":
    ROOT_DIR = ROOT_DIR.parent
ACCOUNTS_DIR = ROOT_DIR / "accounts"

# ── 常量 ──────────────────────────────────────────────
MIMO_BASE = "https://aistudio.xiaomimimo.com"
SID = "xiaomichatbot"

# 全球版注册 (HK region)
GLOBAL_ACCOUNT = "https://global.account.xiaomi.com"
GLOBAL_REGISTER_URL = f"{GLOBAL_ACCOUNT}/fe/service/register"
GLOBAL_SEND_EMAIL = f"{GLOBAL_ACCOUNT}/pass/sendEmailRegTicket"
GLOBAL_VERIFY_EMAIL = f"{GLOBAL_ACCOUNT}/pass/verifyEmailRegTicket"
GLOBAL_SMS_QUOTA = f"{GLOBAL_ACCOUNT}/pass/sms/quota"
GLOBAL_SERVICE_LOGIN = f"{GLOBAL_ACCOUNT}/pass/serviceLogin"

# 验证码
CAPTCHA_CONFIG = "https://verify.sec.xiaomi.com/captcha/v2/config"
CAPTCHA_DATA = "https://verify.sec.xiaomi.com/captcha/v2/data"
CAPTCHA_VERIFY = "https://verify.sec.xiaomi.com/captcha/v2/recaptcha/verify"
CAPTCHA_SITE_KEY = "8027422fb0eb42fbac1b521ec4a7961f"

# RSA 公钥 (生产环境)
RSA_PUBLIC_KEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCYEVrK/4Mahiv0pUJgTybx4J9P\n"
    "5dUT/Y0PuwMbk+gMU+jrZnBiXGv6/hCH1avIhoBcE535F8nJQQN3UavZdFkYidso\n"
    "XuEnat3+eVTp3FslyhRwIBDF09v4vDhRtxFOT+R7uH7h/mzmyA2/+lfIMWGIrffX\n"
    "prYizbV76+YQKhoqFQIDAQAB\n"
    "-----END PUBLIC KEY-----"
)

# AES-CBC IV (浏览器硬编码)
AES_IV = b"0102030405060708"

ESSENTIAL_COOKIES = {
    "serviceToken", "userId", "cUserId", "xiaomichatbot_ph", "xiaomichatbot_slh",
}


# ── 响应解析 ──────────────────────────────────────────

def _parse(resp) -> dict:
    """解析小米 API 响应 (去掉 &&&START&&& 前缀)"""
    text = resp.text
    if text.startswith("&&&START&&&"):
        text = text[len("&&&START&&&"):]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": resp.text[:500], "status": resp.status_code}


# ── 加密 ──────────────────────────────────────────────

def _random_aes_key(length: int = 16) -> str:
    """生成 16 字符随机 AES key (与浏览器 encryptAes 一致)"""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*"
    return "".join(chars[os.urandom(1)[0] % len(chars)] for _ in range(length))


def _rsa_encrypt(plaintext: str, public_key_pem: str) -> bytes:
    """RSA 加密 (PKCS1_v1.5)"""
    from Crypto.PublicKey import RSA
    from Crypto.Cipher import PKCS1_v1_5
    key = RSA.import_key(public_key_pem)
    cipher = PKCS1_v1_5.new(key)
    return cipher.encrypt(plaintext.encode("utf-8"))


def _aes_cbc_encrypt(plaintext: str, key_str: str, iv_bytes: bytes) -> bytes:
    """AES-128-CBC 加密 (PKCS7 padding)"""
    from Crypto.Cipher import AES
    key_bytes = key_str.encode("utf-8")
    data = plaintext.encode("utf-8")
    # PKCS7 padding
    pad_len = 16 - (len(data) % 16)
    data += bytes([pad_len] * pad_len)
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv_bytes)
    return cipher.encrypt(data)


def encrypt_params(params: dict) -> dict:
    """小米 encryptAes 加密 (对齐 crypto.17efe504.chunk.js)

    浏览器实现关键点:
      - AES-128-CBC + PKCS7, IV = UTF-8("0102030405060708")
      - RSA(PKCS1_v1_5) 加密的是 ``btoa(aesKey)`` (AES key 先 base64 再 RSA),
        **不是** 裸 AES key 字符串 (旧实现会导致服务端解出乱码 → 88205)
      - 返回头名浏览器侧是 EUI, HTTP 头大小写不敏感

    返回:
      {
        "eui": "<RSA(btoa(AES_KEY))>.<base64(param_names)>",
        "encryptedParams": {key: base64(AES(value)), ...}
      }
    """
    aes_key = _random_aes_key(16)

    # 与 window.btoa(aesKey) 一致: 对 16 字节 ASCII key 做标准 base64
    aes_key_b64 = base64.b64encode(aes_key.encode("latin1")).decode("ascii")
    rsa_encrypted = _rsa_encrypt(aes_key_b64, RSA_PUBLIC_KEY)
    rsa_b64 = base64.b64encode(rsa_encrypted).decode()

    # 参数名列表 (base64) — Object.keys(n).join(",")
    param_names_b64 = base64.b64encode(",".join(params.keys()).encode()).decode()

    # AES-CBC 加密每个参数值 (CryptoJS AES.encrypt(...).toString() → ciphertext base64)
    encrypted_params = {}
    for key, value in params.items():
        encrypted = _aes_cbc_encrypt(str(value), aes_key, AES_IV)
        encrypted_params[key] = base64.b64encode(encrypted).decode()

    eui = f"{rsa_b64}.{param_names_b64}"
    return {"eui": eui, "encryptedParams": encrypted_params}


# ── Cookie 管理 ───────────────────────────────────────

def _mimo_cookie(name: str, value: str, expires: int = -1) -> dict:
    return {
        "name": name, "value": value, "domain": ".xiaomimimo.com", "path": "/",
        "expires": expires, "httpOnly": name in ("serviceToken", "cUserId"),
        "secure": False, "sameSite": "Lax",
    }


def save_account(
    email: str,
    cookies: list,
    user_info: dict = None,
    password: str | None = None,
    mailbox_meta: dict | None = None,
):
    """保存账号到 accounts/<email>.json

    ``mailbox_meta`` 供 ck 过期后的恢复策略使用，例如::

        {"kind": "tempmaillol", "recoverable": False, "strategy": "replace"}
        {"kind": "imap", "recoverable": True, "strategy": "relogin"}
    """
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": email,
        "user_id": (user_info or {}).get("userId", ""),
        "user_info": user_info or {},
        "cookies": cookies,
        "exported_at": int(time.time()),
        "source": "register_mimo.py",
    }
    if password:
        payload["password"] = password
    if mailbox_meta:
        payload["mailbox"] = mailbox_meta
    # free temp inboxes die quickly; mark replace strategy by default
    if "mailbox" not in payload:
        dom = (email.split("@")[-1] if "@" in email else "").lower()
        freeish = any(
            x in dom
            for x in (
                "airfryersbg",
                "gardianwaves",
                "actionvspot",
                "icodetensor",
                "web-library",
                "mail.tm",
                "guerrillamail",
            )
        )
        if freeish:
            payload["mailbox"] = {
                "kind": "tempmaillol",
                "recoverable": False,
                "strategy": "replace",
            }
    path = ACCOUNTS_DIR / f"{email}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return path


def get_cookie_header(cookies: list) -> str:
    parts = [f"{c['name']}={c['value']}" for c in cookies if "xiaomimimo" in c.get("domain", "")]
    return "; ".join(parts)


def _ph_value(cookies: list) -> str:
    for c in cookies or []:
        if c.get("name") == "xiaomichatbot_ph":
            return (c.get("value") or "").strip().strip('"')
    return ""


def _agree_user_legal(cookies: list) -> None:
    """POST agreement APIs with required ``xiaomichatbot_ph`` query (browser does this)."""
    from urllib.parse import quote

    cookie_header = get_cookie_header(cookies)
    ph = _ph_value(cookies)
    if not cookie_header:
        print("[reg] Step 9: 跳过协议（无 cookie）")
        return
    print("[reg] Step 9: 同意协议...")
    try:
        s = CurlSession(impersonate="chrome120")
        h = {
            "Cookie": cookie_header,
            "Content-Type": "application/json",
            "Origin": MIMO_BASE,
            "Referer": f"{MIMO_BASE}/",
        }
        q = f"?xiaomichatbot_ph={quote(ph, safe='')}" if ph else ""
        for path, label in (
            (f"/open-apis/agreement{q}", "用户协议"),
            (f"/open-apis/agreement/user/mimo-claw{q}", "免责声明"),
        ):
            r = s.post(f"{MIMO_BASE}{path}", headers=h, json={}, timeout=15)
            try:
                j = r.json()
                msg = j.get("msg") or j.get("message") or r.status_code
                code = j.get("code")
                print(f"[reg]   {label}: code={code} {msg}")
            except Exception:
                print(f"[reg]   {label}: http={r.status_code}")
    except Exception as ex:
        print(f"[reg]   协议签署异常: {ex}")


def fetch_user_info(cookies: list) -> dict:
    header = get_cookie_header(cookies)
    if not header:
        return {}
    try:
        s = CurlSession(impersonate="chrome120")
        r = s.get(
            f"{MIMO_BASE}/open-apis/user/mi/get",
            headers={"Cookie": header, "Content-Type": "application/json"},
            timeout=15,
        )
        j = r.json()
        if j.get("code") == 0 and isinstance(j.get("data"), dict):
            return j["data"]
    except Exception:
        pass
    return {}


# ── 注册流程 ──────────────────────────────────────────

def step1_gen_login_url(session: CurlSession) -> tuple[str, str]:
    """Step 1: 获取 callback URL 和 sign

    Returns: (callback_url, sign)
    """
    print("[reg] Step 1: 获取 callback URL...")
    resp = session.get(f"{MIMO_BASE}/open-apis/v1/genLoginUrl", allow_redirects=False, timeout=10)
    location = resp.headers.get("Location", "")
    if not location:
        raise Exception("genLoginUrl 未返回 redirect")

    parsed = urlparse(location)
    params = parse_qs(parsed.query)
    callback = params.get("callback", [None])[0]
    if not callback:
        raise Exception(f"redirect URL 中无 callback: {location[:200]}")

    # 从 callback URL 中提取 sign
    cb_parsed = urlparse(callback)
    cb_params = parse_qs(cb_parsed.query)
    sign = cb_params.get("sign", [""])[0]

    print(f"[reg] callback: {callback[:80]}...")
    return callback, sign


def step2_get_sign(session: CurlSession, callback: str) -> dict:
    """Step 2: 访问 serviceLogin 获取 _sign 和登录参数"""
    print("[reg] Step 2: 获取 _sign...")
    resp = session.get(
        f"{GLOBAL_ACCOUNT}/pass/serviceLogin",
        params={"callback": callback, "sid": SID, "_group": "DEFAULT"},
        allow_redirects=False,
        timeout=15,
    )

    location = resp.headers.get("Location", "")
    if not location:
        # 尝试带 _json=true
        resp = session.get(
            f"{GLOBAL_ACCOUNT}/pass/serviceLogin",
            params={"sid": SID, "callback": callback, "_json": "true"},
            allow_redirects=True,
            timeout=15,
        )
        text = resp.text.replace("&&&START&&&", "")
        try:
            data = json.loads(text)
            return {"_sign": data.get("_sign", ""), "callback": callback}
        except json.JSONDecodeError:
            raise Exception(f"获取 _sign 失败: {resp.text[:200]}")

    params = parse_qs(urlparse(location).query, keep_blank_values=True)
    values = {k: v[0] for k, v in params.items() if v}
    values.setdefault("callback", callback)
    print(f"[reg] _sign: {values.get('_sign', 'N/A')[:30]}...")
    return values


def step3_send_email_reg(
    session: CurlSession,
    email: str,
    password: str,
    sign: str,
    callback: str,
    captcha_code: str = "",
    region: str = "HK",
) -> dict:
    """Step 3: 提交注册 (加密邮箱/密码)

    sendEmailRegTicket 会:
    - 首次调用: 返回验证码要求 (captcha)
    - 带验证码调用: 触发发送邮箱验证码
    """
    print(f"[reg] Step 3: 提交注册 (region={region})...")

    # 加密参数
    enc = encrypt_params({
        "email": email,
        "password": password,
    })

    # 构造 body
    body = {
        "email": enc["encryptedParams"]["email"],
        "password": enc["encryptedParams"]["password"],
        "region": region,
        "sid": SID,
        "icode": captcha_code,
    }

    # 构造 referer (注册页面 URL)
    qs = quote(f"?callback={quote(callback, safe='')}&sid={SID}", safe="")
    service_param = json.dumps({"checkSafePhone": False, "checkSafeAddress": False, "lsrp_score": 0.0}, separators=(",", ":"))
    register_page_params = {
        "_group": "DEFAULT",
        "_sign": sign,
        "serviceParam": service_param,
        "showActiveX": "false",
        "theme": "",
        "needTheme": "false",
        "bizDeviceType": "",
        "_locale": "zh_CN",
        "source": "",
        "region": region,
        "sid": SID,
        "qs": qs,
        "callback": callback,
    }
    referer = f"{GLOBAL_ACCOUNT}/fe/service/register/email?{urlencode(register_page_params)}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": GLOBAL_ACCOUNT,
        "Referer": referer,
        "eui": enc["eui"],
        "x-requested-with": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }

    resp = session.post(GLOBAL_SEND_EMAIL, data=body, headers=headers, timeout=15)

    data = _parse(resp)
    print(f"[reg] sendEmailRegTicket: status={resp.status_code}, code={data.get('code', 'N/A')}")
    return data


_ddddocr_engine = None


def _local_ocr_captcha(img_bytes: bytes) -> str:
    """Free local OCR via ddddocr (no paid captcha service).

    Returns recognized text or empty string on failure / missing package.
    """
    global _ddddocr_engine
    try:
        import ddddocr  # type: ignore
    except ImportError:
        print("[reg]   ddddocr 未安装：pip install ddddocr  （免费本地识别）")
        return ""
    try:
        if _ddddocr_engine is None:
            # show_ad=False avoids noisy banner
            try:
                _ddddocr_engine = ddddocr.DdddOcr(show_ad=False)
            except TypeError:
                _ddddocr_engine = ddddocr.DdddOcr()
        code = (_ddddocr_engine.classification(img_bytes) or "").strip()
        # Xiaomi icode is usually 4 alnum chars; strip noise
        code = "".join(ch for ch in code if ch.isalnum())
        return code
    except Exception as ex:
        print(f"[reg]   ddddocr 异常: {type(ex).__name__}: {ex}")
        return ""


def step4_handle_captcha(session, captcha_api_key: str = None) -> str:
    """Step 4: 获取并解决验证码

    Priority:
      1. 2Captcha if key given (optional paid)
      2. Free local ddddocr
      3. captcha_answer.txt / MIMO_CAPTCHA_CODE / terminal

    Returns: 验证码文字
    """
    print("[reg] Step 4: 获取验证码...")
    ts = int(time.time() * 1000)

    # 建立 captcha session
    session.get(
        CAPTCHA_CONFIG,
        params={"type": "1", "locale": "zh_CN", "callback": f"miVerify_{ts}"},
        timeout=10,
    )

    # 获取验证码图片
    resp = session.get(f"{GLOBAL_ACCOUNT}/pass/getCode?icodeType=register", timeout=10)
    img_bytes = resp.content
    img_b64 = base64.b64encode(img_bytes).decode()
    # always dump for debug / manual fallback
    captcha_path = ROOT_DIR / "captcha_now.png"
    captcha_path.write_bytes(img_bytes)

    if captcha_api_key:
        # ── 2Captcha 自动识别（可选付费）──
        print("[reg]   → 发送到 2Captcha...")
        try:
            # 上传图片
            upload_resp = session.post(
                "https://2captcha.com/in.php",
                data={"key": captcha_api_key, "method": "base64", "body": img_b64},
                timeout=30,
            )
            if "OK|" not in upload_resp.text:
                print(f"[reg]   ❌ 上传失败: {upload_resp.text}")
                return _manual_captcha(img_bytes)

            captcha_id = upload_resp.text.split("|")[1]
            print(f"[reg]   ⏳ 等待识别 (id={captcha_id})...")

            # 轮询结果 (最多等 30 秒)
            for i in range(15):
                time.sleep(2)
                result_resp = session.get(
                    "https://2captcha.com/res.php",
                    params={"key": captcha_api_key, "action": "get", "id": captcha_id},
                    timeout=10,
                )
                if "OK|" in result_resp.text:
                    code = result_resp.text.split("|")[1]
                    print(f"[reg]   ✅ 识别结果: {code}")
                    return code
                elif result_resp.text == "CAPCHA_NOT_READY":
                    continue
                else:
                    print(f"[reg]   ❌ 识别失败: {result_resp.text}")
                    break

            print("[reg]   ❌ 2Captcha 超时/失败，尝试本地 ddddocr…")
        except Exception as ex:
            print(f"[reg]   ❌ 2Captcha 异常: {ex}，尝试本地 ddddocr…")

    # ── 免费本地 OCR（默认路径）──
    if os.environ.get("MIMO_NO_LOCAL_OCR", "").lower() not in ("1", "true", "yes"):
        code = _local_ocr_captcha(img_bytes)
        if code:
            print(f"[reg]   ✅ ddddocr 识别: {code}")
            return code
        print("[reg]   ddddocr 未识别出内容，走手动兜底…")

    return _manual_captcha(img_bytes)


def _manual_captcha(img_bytes: bytes) -> str:
    """Resolve image captcha without requiring 2Captcha.

    Order:
      1. env ``MIMO_CAPTCHA_CODE`` (one-shot, then cleared by caller if desired)
      2. file ``captcha_answer.txt`` next to captcha image (polled up to 90s)
      3. interactive ``input`` when stdin is a TTY
      4. otherwise fail empty
    """
    captcha_path = ROOT_DIR / "captcha_now.png"
    answer_path = ROOT_DIR / "captcha_answer.txt"
    with open(captcha_path, "wb") as f:
        f.write(img_bytes)
    print(f"[reg]   验证码已保存: {captcha_path}")

    env_code = (os.environ.get("MIMO_CAPTCHA_CODE") or "").strip()
    if env_code:
        print(f"[reg]   使用环境变量 MIMO_CAPTCHA_CODE ({len(env_code)} chars)")
        # one-shot
        os.environ.pop("MIMO_CAPTCHA_CODE", None)
        return env_code

    # Agent / external solver: write answer into captcha_answer.txt
    # Prefer file polling even when stdin claims to be a TTY (agent shells often do).
    prefer_file = os.environ.get("MIMO_CAPTCHA_FILE", "1") not in ("0", "false", "no")
    print(f"[reg]   等待 {answer_path.name}" + (" 或终端输入…" if not prefer_file else " …"))
    deadline = time.time() + 120
    while time.time() < deadline:
        if answer_path.exists():
            try:
                raw = answer_path.read_text(encoding="utf-8").strip()
                code = raw.split()[0] if raw else ""
            except Exception:
                code = ""
            try:
                answer_path.unlink(missing_ok=True)
            except Exception:
                pass
            if code:
                print(f"[reg]   ✅ 从 captcha_answer.txt 读取: {code}")
                return code
        time.sleep(0.5)

    if not prefer_file and sys.stdin.isatty():
        try:
            return input("[reg]   请输入验证码: ").strip()
        except EOFError:
            return ""
    print("[reg]   ❌ 无 CAPTCHA_API_KEY / captcha_answer.txt / 终端输入")
    return ""


def step5_check_sms_quota(session, email: str, region: str = "HK") -> dict:
    """Step 5: 检查邮箱验证码配额 (防限流)

    注意: 验证码邮件由 sendEmailRegTicket 自动触发发送,
    此步仅检查配额，不是发送验证码。
    放在 verifyEmailRegTicket 之前调用更稳妥。
    """
    print("[reg] Step 5: 检查邮箱验证码配额...")
    body = {
        "address": email,
        "templateId": "CI93714_EM_153",
    }
    resp = session.post(
        GLOBAL_SMS_QUOTA,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": GLOBAL_ACCOUNT,
            "x-requested-with": "XMLHttpRequest",
        },
        timeout=15,
    )
    data = _parse(resp)
    print(f"[reg] sms/quota: status={resp.status_code}")
    return data


def step6_verify_email(
    session: CurlSession,
    email: str,
    password: str,
    code: str,
    sign: str,
    callback: str,
    region: str = "HK",
) -> dict:
    """Step 6: 提交邮箱验证码完成注册"""
    print(f"[reg] Step 6: 提交验证码 {code}...")

    enc = encrypt_params({
        "email": email,
        "password": password,
    })

    qs = quote(f"?callback={quote(callback, safe='')}&sid={SID}", safe="")
    service_param = json.dumps({"checkSafePhone": False, "checkSafeAddress": False, "lsrp_score": 0.0}, separators=(",", ":"))

    body = {
        "ticket": code,
        "region": region,
        "email": enc["encryptedParams"]["email"],
        "env": "web",
        "qs": qs,
        "isAcceptLicense": "true",
        "sid": SID,
        "password": enc["encryptedParams"]["password"],
        "policyName": "globalmiaccount",
        "callback": callback,
    }

    register_page_params = {
        "_group": "DEFAULT",
        "_sign": sign,
        "serviceParam": service_param,
        "showActiveX": "false",
        "theme": "",
        "needTheme": "false",
        "bizDeviceType": "",
        "_locale": "zh_CN",
        "source": "",
        "region": region,
        "sid": SID,
        "qs": qs,
        "callback": callback,
    }
    referer = f"{GLOBAL_ACCOUNT}/fe/service/register/email/verify?{urlencode(register_page_params)}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": GLOBAL_ACCOUNT,
        "Referer": referer,
        "eui": enc["eui"],
        "x-requested-with": "XMLHttpRequest",
    }

    resp = session.post(GLOBAL_VERIFY_EMAIL, data=body, headers=headers, timeout=15)

    data = _parse(resp)
    print(f"[reg] verifyEmailRegTicket: status={resp.status_code}, code={data.get('code', 'N/A')}")
    return data


def _cookie_objs(session: CurlSession):
    """curl_cffi: ``for c in session.cookies`` yields *names* (str), not Cookie.

    Always iterate ``session.cookies.jar`` for Cookie objects with .name/.value/.domain.
    """
    jar = getattr(session.cookies, "jar", None)
    if jar is not None:
        return list(jar)
    # fallback: synthesize from get_dict (domain unknown)
    out = []
    get_dict = getattr(session.cookies, "get_dict", None)
    if callable(get_dict):
        for name, value in get_dict().items():
            out.append(type("C", (), {"name": name, "value": value, "domain": "", "expires": -1})())
    return out


def step7_auto_login(session: CurlSession, callback: str) -> str | None:
    """Step 7: 注册完成后自动登录 (serviceLogin → STS)"""
    print("[reg] Step 7: 自动登录...")
    resp = session.get(
        GLOBAL_SERVICE_LOGIN,
        params={"callback": callback, "sid": SID},
        allow_redirects=True,
        timeout=15,
    )

    # 检查是否拿到 serviceToken
    for c in _cookie_objs(session):
        if c.name == "serviceToken" and "xiaomimimo" in (c.domain or ""):
            print("[reg] 已获取 serviceToken")
            return resp.url

    # serviceToken 可能在 redirect 链中
    if resp.history:
        for h in resp.history:
            loc = h.headers.get("Location", "")
            if "sts" in loc or "aistudio" in loc:
                return loc

    return resp.url


def step8_exchange_sts(session: CurlSession, sts_url: str) -> list:
    """Step 8: 访问 STS 换取 MiMo session cookies"""
    print("[reg] Step 8: 换取 MiMo session...")
    if sts_url:
        session.get(sts_url, allow_redirects=True, timeout=15)

    # 收集 .xiaomimimo.com cookies
    cookies = []
    seen = set()
    for c in _cookie_objs(session):
        domain = c.domain or ""
        if c.name in ESSENTIAL_COOKIES and "xiaomimimo" in domain:
            cookies.append(_mimo_cookie(c.name, c.value, getattr(c, "expires", -1) or -1))
            seen.add(c.name)
    for c in _cookie_objs(session):
        if c.name in ("serviceToken", "userId") and c.name not in seen:
            cookies.append(_mimo_cookie(c.name, c.value, getattr(c, "expires", -1) or -1))
            seen.add(c.name)

    # 获取 xiaomichatbot_ph
    if "xiaomichatbot_ph" not in seen:
        try:
            session.get(f"{MIMO_BASE}/open-apis/user/mi/get", timeout=10)
            for c in _cookie_objs(session):
                if c.name == "xiaomichatbot_ph":
                    val = (c.value or "").strip('"')
                    cookies.append(_mimo_cookie("xiaomichatbot_ph", val))
                    break
        except Exception:
            pass

    return cookies


# ── 主流程 ────────────────────────────────────────────

def _gen_password(length: int = 14) -> str:
    """Password that usually passes Xiaomi policy (upper+lower+digit)."""
    import secrets
    import string

    lower = secrets.choice(string.ascii_lowercase)
    upper = secrets.choice(string.ascii_uppercase)
    digit = secrets.choice(string.digits)
    rest = [
        secrets.choice(string.ascii_letters + string.digits)
        for _ in range(max(0, length - 3))
    ]
    chars = list(lower + upper + digit + "".join(rest))
    secrets.SystemRandom().shuffle(chars)
    return "".join(chars)


def _load_mailbox(kind: str, **kw):
    """Import claw/mailbox without requiring claw to be a package."""
    import importlib.util

    path = ROOT_DIR / "claw" / "mailbox.py"
    spec = importlib.util.spec_from_file_location("mimo_mailbox", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load mailbox from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.make_mailbox(kind, **kw)


def _enable_auto_deploy(email: str) -> None:
    """Mark the new account enabled in auto_deploy config (best-effort)."""
    try:
        sys.path.insert(0, str(ROOT_DIR))
        from claw.auto_deploy import load_config, save_config

        cfg = load_config()
        accounts = cfg.setdefault("accounts", {})
        # Prefer email-style key used by accounts/*.json stems
        key = email
        acc = accounts.setdefault(key, {})
        acc["enabled"] = True
        acc.setdefault("cron", "0 * * * *")
        # clear any stale risk tags from a previous run with same name
        for k in ("risk_blocked", "risk_kind", "risk_blocked_reason", "risk_blocked_at"):
            acc.pop(k, None)
        save_config(cfg)
        print(f"[reg] auto_deploy enabled for {key}")
    except Exception as ex:
        print(f"[reg] auto_deploy enable skipped: {type(ex).__name__}: {ex}")


def register(
    email: str,
    password: str,
    region: str = "HK",
    captcha_api_key: str = None,
    email_code_fn=None,
    enable_deploy: bool = False,
) -> dict:
    """完整注册流程

    Args:
        email: 注册邮箱
        password: 注册密码
        region: 区域 (HK/CN/SG/...)
        captcha_api_key: 2Captcha API Key (可选，不传则手动输入验证码)
        email_code_fn: 可调用对象 ``() -> str|None``，全自动读邮箱验证码；
            为 None 时回退到终端 ``input``。
        enable_deploy: 成功后把账号写进 auto_deploy.accounts 并 enabled=true

    Returns:
      {"status": "ok", "cookies": [...], "user_info": {...}, "email": ..., "path": ...}
      {"status": "error", "error": "...", "email": ...}
    """
    # curl_cffi: 伪造 Chrome 120 的 TLS 指纹 (JA3, Akamai fingerprint)
    session = CurlSession(impersonate="chrome120")
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    })

    try:
        # Step 1: 获取 callback
        callback, sign = step1_gen_login_url(session)

        # Step 2: 获取 _sign
        login_params = step2_get_sign(session, callback)
        sign = login_params.get("_sign", sign)

        # Step 3/4: manMachine 风控下必须先拉图再带 icode 提交（空 icode 也会
        # 直接 CAPTCHA_VERIFY_ERROR）。验证码错可刷新重试。
        # 免费路径默认 ddddocr 本地识别（无需付费打码）。
        result = {"code": 87001}
        code = 87001
        captcha_attempts = 0
        rate_limit_waits = 0
        while code in (87006, 87001) and captcha_attempts < 8:
            captcha_attempts += 1
            print(f"[reg] 图形验证码 (attempt {captcha_attempts}/8)")
            if captcha_attempts > 1:
                os.environ.pop("MIMO_CAPTCHA_CODE", None)
            captcha_code = step4_handle_captcha(session, captcha_api_key)
            if not captcha_code:
                return {
                    "status": "error",
                    "error": (
                        "未解决图形验证码。免费方案: pip install ddddocr；"
                        "或把答案写入 captcha_answer.txt"
                    ),
                    "email": email,
                }
            # try case variants on same image code before refreshing
            variants = []
            for v in (captcha_code, captcha_code.upper(), captcha_code.lower()):
                if v and v not in variants:
                    variants.append(v)
            for captcha_try in variants:
                result = step3_send_email_reg(
                    session, email, password, sign, callback,
                    captcha_code=captcha_try, region=region,
                )
                code = result.get("code", -1)
                if code not in (87006, 87001):
                    print(f"[reg] 图形码通过 (used={captcha_try!r})")
                    break
                # only first variant matches current ick/image; further cases need refresh
                desc = result.get("desc") or result.get("reason") or str(result)
                print(f"[reg] 图形码未过 ({captcha_try!r}): {desc}")
                break  # refresh image via next outer loop
            # 85005 = request rejected / rate limit — back off and retry captcha loop
            if code == 85005 and rate_limit_waits < 3:
                rate_limit_waits += 1
                wait_s = 20 * rate_limit_waits
                print(f"[reg] 限流 85005，等待 {wait_s}s 后重试…", flush=True)
                time.sleep(wait_s)
                code = 87001  # re-enter captcha loop
                continue
            if code not in (87006, 87001):
                break

        if code != 0 and code != 70016:
            desc = result.get("desc") or result.get("description") or result.get("raw", str(result))
            tips = result.get("tips") or ""
            # friendlier free-auto diagnostics
            if code == 85005:
                return {
                    "status": "error",
                    "error": f"注册限流/拒绝 (85005 {desc})，请稍后再试",
                    "email": email,
                    "code": code,
                }
            if code == 88205:
                return {
                    "status": "error",
                    "error": (
                        f"邮箱被拒 (88205 {desc}). "
                        "mail.tm/随机 freemail 常被小米判非法；"
                        "请改用自有域名 IMAP catch-all，或 --email 指定真实可收信邮箱"
                    ),
                    "email": email,
                    "code": code,
                }
            if code == 70038:
                return {
                    "status": "error",
                    "error": f"地区不允许邮箱注册 ({tips or desc})，换 --region HK/SG 或改手机注册",
                    "email": email,
                    "code": code,
                }
            return {"status": "error", "error": f"注册提交失败: {desc}", "email": email, "code": code}

        # Step 5: 检查邮箱验证码配额 (在 verifyEmailRegTicket 之前)
        step5_check_sms_quota(session, email, region)

        # Step 6: 等待并提交验证码（自动 or 手动）
        for attempt in range(3):
            if email_code_fn is not None:
                if attempt == 0:
                    code_input = email_code_fn()
                else:
                    # 再等一轮邮件（可能首封延迟）
                    print(f"[reg] 验证码错误/未到，自动再等 (第{attempt+1}次)...")
                    code_input = email_code_fn()
            else:
                code_input = input(f"[reg] 请输入邮箱验证码 (第{attempt+1}次): ").strip()

            if not code_input:
                print("[reg] 验证码不能为空")
                continue

            verify_result = step6_verify_email(
                session, email, password, code_input, sign, callback, region
            )

            if verify_result.get("code") == 0:
                print("[reg] 注册成功！")
                break
            elif verify_result.get("code") == 87001:
                print("[reg] 验证码错误，请重试")
                continue
            else:
                desc = verify_result.get("desc", str(verify_result))
                return {"status": "error", "error": f"验证码验证失败: {desc}", "email": email}
        else:
            return {"status": "error", "error": "验证码错误次数过多", "email": email}

        # Step 7: 自动登录
        sts_url = step7_auto_login(session, callback)

        # Step 8: 换取 MiMo session
        cookies = step8_exchange_sts(session, sts_url)

        if not cookies:
            return {"status": "error", "error": "未获取到 MiMo cookies", "email": email}

        # 验证并保存
        user_info = fetch_user_info(cookies)
        path = None
        if user_info:
            # Step 9: 同意用户协议（需 query xiaomichatbot_ph=，否则 401）
            _agree_user_legal(cookies)
            user_info = fetch_user_info(cookies) or user_info

            path = save_account(email, cookies, user_info, password=password)
            uid = user_info.get("userId", "")
            print(f"[reg] 保存成功: {path} (userId={uid})")
            if enable_deploy:
                _enable_auto_deploy(email)
        else:
            print("[reg] 警告: 无法获取用户信息，cookies 可能无效")

        return {
            "status": "ok",
            "email": email,
            "password": password,
            "cookies": cookies,
            "user_info": user_info,
            "path": str(path) if path else None,
        }

    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "email": email}


def register_auto(
    *,
    region: str = "HK",
    captcha_api_key: str | None = None,
    mailbox_kind: str = "tempmaillol",
    password: str | None = None,
    mail_timeout: int = 180,
    enable_deploy: bool = False,
    imap: dict | None = None,
) -> dict:
    """全自动：建邮箱 → 注册 → 等验证码 → 存 accounts/。

    默认 ``tempmaillol``：实测小米可收验证码；mail.tm 等常 70075 风险邮箱。
    """
    password = password or _gen_password()
    kind = (mailbox_kind or "tempmaillol").lower()
    kw = dict(imap or {})
    print(f"[reg] auto mailbox={kind}")
    box = _load_mailbox(kind, **kw)
    email = box.address
    print(f"[reg] auto email={email}")
    print(f"[reg] auto password={password}")

    def _wait_code():
        return box.wait_code(timeout=mail_timeout)

    result = register(
        email,
        password,
        region=region,
        captcha_api_key=captcha_api_key,
        email_code_fn=_wait_code,
        enable_deploy=enable_deploy,
    )
    result.setdefault("email", email)
    result.setdefault("password", password)
    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description="小米账号邮箱注册（支持全自动）")
    parser.add_argument("--email", help="注册邮箱（非 --auto 时）")
    parser.add_argument("--password", help="注册密码；--auto 时默认随机生成")
    parser.add_argument("--region", default="HK", help="注册地区 (HK/CN/SG/...)")
    parser.add_argument(
        "--captcha-key",
        default=os.environ.get("CAPTCHA_API_KEY")
        or os.environ.get("TWOCAPTCHA_KEY")
        or os.environ.get("CAPTCHA_KEY"),
        help="2Captcha API Key（也可用环境变量 CAPTCHA_API_KEY）",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="全自动：tempmail.lol 收验证码 + ddddocr 图形码（免费）",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="--auto 时连续注册个数（默认 1）",
    )
    parser.add_argument(
        "--mailbox",
        default=os.environ.get("MIMO_MAILBOX", "tempmaillol"),
        choices=("tempmaillol", "mailtm", "imap"),
        help="邮箱后端：tempmaillol（推荐免费）/ mailtm / imap",
    )
    parser.add_argument("--mail-timeout", type=int, default=180, help="等邮箱验证码秒数")
    parser.add_argument(
        "--enable-deploy",
        action="store_true",
        help="成功后把账号加入 auto_deploy 并 enabled=true",
    )
    # IMAP options
    parser.add_argument("--imap-address", default=os.environ.get("MIMO_IMAP_ADDRESS"))
    parser.add_argument("--imap-host", default=os.environ.get("MIMO_IMAP_HOST"))
    parser.add_argument("--imap-user", default=os.environ.get("MIMO_IMAP_USER"))
    parser.add_argument("--imap-password", default=os.environ.get("MIMO_IMAP_PASSWORD"))
    parser.add_argument(
        "--imap-port", type=int, default=int(os.environ.get("MIMO_IMAP_PORT") or "993")
    )
    args = parser.parse_args()

    captcha_key = args.captcha_key
    if args.auto and not captcha_key:
        print(
            "[reg] 免费模式: 图形码 ddddocr + 邮箱 tempmail.lol（默认），"
            "无需付费打码。mail.tm/yopmail 等常 70075 风险邮箱。"
        )

    if args.auto:
        imap_kw = None
        if args.mailbox == "imap":
            need = {
                "address": args.imap_address,
                "host": args.imap_host,
                "user": args.imap_user,
                "password": args.imap_password,
            }
            if not all(need.values()):
                print(
                    "IMAP 模式需要 --imap-address/host/user/password "
                    "或 MIMO_IMAP_* 环境变量"
                )
                sys.exit(2)
            imap_kw = {**need, "port": args.imap_port}

        ok = fail = 0
        results = []
        for i in range(max(1, args.count)):
            print(f"\n======== auto register {i + 1}/{args.count} ========")
            # For IMAP catch-all, generate a unique local part if address has *
            password = args.password or _gen_password()
            if args.mailbox == "imap" and imap_kw and "*" in (imap_kw.get("address") or ""):
                # expand catch-all template user+tag@domain
                base = imap_kw["address"].replace("*", uuid.uuid4().hex[:10])
                # register_auto always creates mailtm box; for imap we pass fixed address via custom path
                box = _load_mailbox(
                    "imap",
                    address=base,
                    host=imap_kw["host"],
                    user=imap_kw["user"],
                    password=imap_kw["password"],
                    port=imap_kw["port"],
                )
                email = box.address
                print(f"[reg] auto email={email}")
                result = register(
                    email,
                    password,
                    region=args.region,
                    captcha_api_key=captcha_key,
                    email_code_fn=lambda b=box: b.wait_code(timeout=args.mail_timeout),
                    enable_deploy=args.enable_deploy,
                )
                result["password"] = password
            else:
                result = register_auto(
                    region=args.region,
                    captcha_api_key=captcha_key,
                    mailbox_kind=args.mailbox,
                    password=password,
                    mail_timeout=args.mail_timeout,
                    enable_deploy=args.enable_deploy,
                    imap=imap_kw,
                )
            results.append(result)
            if result.get("status") == "ok":
                ok += 1
                uid = (result.get("user_info") or {}).get("userId", "?")
                print(f"[reg] OK email={result.get('email')} userId={uid}")
            else:
                fail += 1
                print(f"[reg] FAIL email={result.get('email')} err={result.get('error')}")
            if i + 1 < args.count:
                time.sleep(2)

        print(f"\n======== done ok={ok} fail={fail} ========")
        sys.exit(0 if fail == 0 else 1)

    # ── 半自动 / 交互 ──
    email = args.email or os.environ.get("MIMO_EMAIL") or input("[reg] 邮箱: ").strip()
    password = args.password or os.environ.get("MIMO_PASSWORD")
    if not password:
        import getpass

        password = getpass.getpass("[reg] 密码: ")

    if not email or not password:
        print("邮箱和密码不能为空")
        sys.exit(1)

    result = register(
        email,
        password,
        args.region,
        captcha_key,
        enable_deploy=args.enable_deploy,
    )

    if result["status"] == "ok":
        uid = result.get("user_info", {}).get("userId", "?")
        print(f"\n注册成功! userId={uid}")
    else:
        print(f"\n注册失败: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
