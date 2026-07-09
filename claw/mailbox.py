#!/usr/bin/env python3
"""
可插拔验证码邮箱读取
====================
为半自动注册器提供「自动读邮箱验证码」能力。两种后端，接口一致:

  - mail.tm : 免费临时邮箱, 无需 key。零成本, 但域名可能被小米拒/邮件不达。
  - imap    : 自有域名 catch-all 或任意 IMAP 邮箱。可靠, 验证码必到。

用法 (库):
    from mailbox import make_mailbox
    box = make_mailbox("mailtm")                 # 随机临时地址
    print(box.address)                           # 拿去注册
    code = box.wait_code(timeout=120)            # 阻塞等验证码

    box = make_mailbox("imap", address="me@dom.com",
                        host="imap.gmail.com", user="me@gmail.com", password="app-pw")
    code = box.wait_code(timeout=120)

自测:
    python claw/mailbox.py mailtm        # 建临时邮箱, 打印地址, 等 60s 看能否收码
"""

import re
import sys
import time
import imaplib
import email as emaillib
from email.header import decode_header
from secrets import token_hex

import requests

# 小米验证码是 6 位数字 (HAR: ticket=721258)
DEFAULT_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
MAILTM_API = "https://api.mail.tm"
POLL_INTERVAL = 3  # 秒


def _members(j) -> list:
    """mail.tm 历史上用 Hydra 包装 (``hydra:member``)，现已改为返回纯 list。两种都兼容。"""
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        return j.get("hydra:member") or j.get("member") or []
    return []


def _extract_code(text: str, code_re: re.Pattern) -> str | None:
    """从邮件正文/标题里抠出验证码。优先取靠近 'code/验证码' 字样的数字。"""
    if not text:
        return None
    # 优先: 关键词附近的数字
    for kw in ("verification code", "verification", "验证码", "code", "Mi Account"):
        idx = text.lower().find(kw.lower())
        if idx != -1:
            m = code_re.search(text, idx)
            if m:
                return m.group(1)
    # 兜底: 全文第一个 6 位数
    m = code_re.search(text)
    return m.group(1) if m else None


class Mailbox:
    """统一接口。子类实现 address / _fetch_texts。"""

    address: str = ""

    def _fetch_texts(self) -> list[str]:
        """返回当前收件箱所有(新)邮件的 文本块 列表 (标题+正文拼一起)。"""
        raise NotImplementedError

    def wait_code(self, timeout: int = 120, code_re: re.Pattern = DEFAULT_CODE_RE) -> str | None:
        """轮询直到拿到验证码或超时。"""
        deadline = time.time() + timeout
        print(f"[mail] 等待验证码 ({self.address}) ，最长 {timeout}s ...")
        while time.time() < deadline:
            try:
                for text in self._fetch_texts():
                    code = _extract_code(text, code_re)
                    if code:
                        print(f"[mail] ✅ 命中验证码: {code}")
                        return code
            except Exception as ex:
                print(f"[mail] 轮询异常(忽略继续): {type(ex).__name__}: {ex}")
            time.sleep(POLL_INTERVAL)
        print("[mail] ❌ 等待验证码超时")
        return None


# ── mail.tm 免费临时邮箱 ──────────────────────────────
class MailTmBox(Mailbox):
    def __init__(self, password: str | None = None):
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self.password = password or token_hex(12)
        self.address = self._create_account()
        self.token = self._get_token()
        self.s.headers["Authorization"] = f"Bearer {self.token}"
        self._seen: set[str] = set()

    def _domain(self) -> str:
        r = self.s.get(f"{MAILTM_API}/domains", timeout=15)
        r.raise_for_status()
        active = [d for d in _members(r.json()) if d.get("isActive")]
        if not active:
            raise RuntimeError("mail.tm 无可用域名")
        return active[0]["domain"]

    def _create_account(self) -> str:
        domain = self._domain()
        local = "u" + token_hex(8)
        addr = f"{local}@{domain}"
        r = self.s.post(f"{MAILTM_API}/accounts",
                        json={"address": addr, "password": self.password}, timeout=15)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"mail.tm 建号失败 {r.status_code}: {r.text[:200]}")
        return addr

    def _get_token(self) -> str:
        r = self.s.post(f"{MAILTM_API}/token",
                        json={"address": self.address, "password": self.password}, timeout=15)
        r.raise_for_status()
        return r.json()["token"]

    def _fetch_texts(self) -> list[str]:
        r = self.s.get(f"{MAILTM_API}/messages", timeout=15)
        r.raise_for_status()
        out = []
        for msg in _members(r.json()):
            mid = msg.get("id")
            if not mid or mid in self._seen:
                continue
            self._seen.add(mid)
            full = self.s.get(f"{MAILTM_API}/messages/{mid}", timeout=15).json()
            body = full.get("text") or " ".join(full.get("html") or [])
            out.append(f"{msg.get('subject','')}\n{body}")
        return out


# ── IMAP (自有域名 / Gmail 应用密码) ──────────────────
class ImapBox(Mailbox):
    def __init__(self, address: str, host: str, user: str, password: str,
                 port: int = 993, mailbox: str = "INBOX"):
        self.address = address
        self.host, self.port = host, port
        self.user, self.password, self.mailbox = user, password, mailbox
        self._seen: set[bytes] = set()
        self._start = time.time()

    def _fetch_texts(self) -> list[str]:
        out = []
        M = imaplib.IMAP4_SSL(self.host, self.port)
        try:
            M.login(self.user, self.password)
            M.select(self.mailbox)
            # 只看发给本地址、最近的未读
            typ, data = M.search(None, "TO", self.address)
            ids = data[0].split() if data and data[0] else []
            for mid in ids[-10:]:
                if mid in self._seen:
                    continue
                self._seen.add(mid)
                typ, msg_data = M.fetch(mid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    continue
                msg = emaillib.message_from_bytes(msg_data[0][1])
                subj = "".join(
                    (p.decode(enc or "utf-8", "replace") if isinstance(p, bytes) else p)
                    for p, enc in decode_header(msg.get("Subject", ""))
                )
                body = self._body(msg)
                out.append(f"{subj}\n{body}")
        finally:
            try:
                M.logout()
            except Exception:
                pass
        return out

    @staticmethod
    def _body(msg) -> str:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        return part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8", "replace")
                    except Exception:
                        continue
            return ""
        try:
            return msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", "replace")
        except Exception:
            return str(msg.get_payload())


# ── tempmail.lol 免费临时邮箱（实测小米可收验证码）────
class TempMailLolBox(Mailbox):
    """https://tempmail.lol free API — Xiaomi accepts these domains (code=0)
    while classic disposable lists often hit 70075.
    """

    def __init__(self):
        r = requests.post("https://api.tempmail.lol/v2/inbox/create", timeout=20)
        r.raise_for_status()
        j = r.json()
        self.address = j.get("address") or j.get("email") or ""
        self.token = j.get("token") or ""
        if not self.address or not self.token:
            raise RuntimeError(f"tempmail.lol 建号失败: {j}")
        self._seen: set[str] = set()

    def _fetch_texts(self) -> list[str]:
        r = requests.get(
            "https://api.tempmail.lol/v2/inbox",
            params={"token": self.token},
            timeout=15,
        )
        r.raise_for_status()
        j = r.json()
        emails = j.get("emails") or j.get("messages") or []
        out = []
        for m in emails:
            mid = str(m.get("_id") or m.get("id") or m.get("date") or m.get("subject") or "")
            if mid and mid in self._seen:
                continue
            if mid:
                self._seen.add(mid)
            # body may be placeholder; code often sits in html
            text = "\n".join(
                str(m.get(k) or "")
                for k in ("subject", "body", "text", "html", "content")
            )
            out.append(text)
        return out


def make_mailbox(kind: str = "tempmaillol", **kw) -> Mailbox:
    kind = (kind or "tempmaillol").lower().replace("_", "").replace("-", "").replace(".", "")
    if kind in ("tempmaillol", "tempmail", "lol"):
        return TempMailLolBox()
    if kind == "mailtm":
        return MailTmBox(password=kw.get("password"))
    if kind == "imap":
        return ImapBox(**kw)
    raise ValueError(f"未知邮箱后端: {kind} (支持 tempmaillol / mailtm / imap)")


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "tempmaillol"
    box = make_mailbox(kind)
    print(f"[selftest] 临时地址: {box.address}")
    print("[selftest] API 通。等 60s 看有没有邮件...")
    code = box.wait_code(timeout=60)
    print(f"[selftest] 结果: {code or '(60s 内无邮件, 属正常)'}")
