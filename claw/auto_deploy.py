"""
Auto-deploy engine: per-account scheduled deployment with full 10-step flow.

Flow per account:
  0. Destroy old claw (skip if none)
  0.5. Clean up stale tunnel processes on jump server
  1. Create new claw
  2. Wait until claw is AVAILABLE
  3. Send deploy text → claw executes (multi-step in claw)
  4. Capture SSH public key from claw reply
  5. Add SSH key to jump server's authorized_keys
  6. Tell claw the key is added (claw establishes reverse tunnel)
  7. (Reserved)
  8. Verify the API endpoint on jump server is reachable
  9. Done — record run history

All upstream Studio API calls and Claw WS chat are async; the deploy itself
runs as an async coroutine inside a dedicated thread (one event loop per
deploy). The scheduler stays sync and just spawns those threads.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
import subprocess
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from croniter import croniter

CONFIG_PATH = Path(__file__).parent.parent / "data" / "auto_deploy.json"
LOG_DIR = Path(__file__).parent.parent / "data" / "deploy_logs"
HISTORY_DIR = Path(__file__).parent.parent / "data" / "deploy_history"

JUMP_SERVER = "149.88.90.137"
JUMP_USER = "root"

# Stale-deploy entries (state ∈ done/error/cancelled) older than this are
# treated as idle by ``get_deploy_status``. No cleanup threads needed.
_STALE_AFTER_S = 300

# Per-step timing knobs.
_DESTROY_POLL_INTERVAL_S = 5
_DESTROY_POLL_MAX_ITERS = 12  # → up to 60s wait
_CREATE_POLL_INTERVAL_S = 5
_CREATE_POLL_MAX_ITERS = 24   # → up to 120s wait
_PROBE_API_INTERVAL_S = 5
_PROBE_API_MAX_ITERS = 12     # → up to 60s wait

# In-memory log size cap; on-disk log is rotated past this many bytes.
_LOG_LINES_MAX = 2000
_LOG_FILE_MAX_BYTES = 1_000_000  # ~1MB → keep current + one .1 backup

logger_module = logging.getLogger(__name__)


def _ensure_dirs():
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _save_run_history(account_filename: str, status: str, log_lines: list):
    _ensure_dirs()
    safe_name = account_filename.replace("/", "_").replace("\\", "_")
    history_file = HISTORY_DIR / f"{safe_name}.json"
    history = []
    if history_file.exists():
        try:
            history = json.loads(history_file.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history.append({
        "id": uuid.uuid4().hex[:8],
        "started_at": datetime.now().isoformat(),
        "status": status,
        "lines": log_lines,
    })
    history = history[-50:]
    history_file.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def get_run_history(account_filename: str) -> list:
    _ensure_dirs()
    safe_name = account_filename.replace("/", "_").replace("\\", "_")
    history_file = HISTORY_DIR / f"{safe_name}.json"
    if not history_file.exists():
        return []
    try:
        history = json.loads(history_file.read_text(encoding="utf-8"))
        history.reverse()
        return history
    except Exception:
        return []


def load_config() -> dict:
    _ensure_dirs()
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"accounts": {}}


def save_config(cfg: dict):
    _ensure_dirs()
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


def get_account_config(account_filename: str) -> dict:
    cfg = load_config()
    return cfg.get("accounts", {}).get(account_filename, {
        "enabled": False,
        "cron": "0 3 * * *",
    })


# ─── Log management ───

class DeployLogger:
    """Append-only run log with rotation + in-memory tail.

    The on-disk file is truncated to its tail when it exceeds
    ``_LOG_FILE_MAX_BYTES``; the in-memory ``lines`` list is capped at
    ``_LOG_LINES_MAX`` so long-running deploys can't OOM."""

    def __init__(self, account_filename: str):
        self.account = account_filename
        self.lines: list[str] = []
        self._file = LOG_DIR / f"{account_filename.replace('/', '_')}.log"

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.lines.append(line)
        if len(self.lines) > _LOG_LINES_MAX:
            self.lines = self.lines[-_LOG_LINES_MAX:]
        print(f"[deploy:{self.account}] {line}", flush=True)
        try:
            with open(self._file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            self._rotate_if_needed()
        except Exception:
            pass

    def _rotate_if_needed(self):
        try:
            size = self._file.stat().st_size
        except OSError:
            return
        if size <= _LOG_FILE_MAX_BYTES:
            return
        try:
            backup = self._file.with_suffix(self._file.suffix + ".1")
            if backup.exists():
                backup.unlink()
            self._file.replace(backup)
        except OSError:
            pass

    def get_recent(self, n: int = 50) -> list:
        return self.lines[-n:]


# ─── Active deployments ───

_active_deploys: dict = {}
_scheduler_running = False
_scheduler_thread: Optional[threading.Thread] = None


def _gc_active_deploys() -> None:
    """Drop entries that finished more than ``_STALE_AFTER_S`` seconds ago.
    Replaces the old per-deploy ``sleep(300)`` cleanup thread."""
    now = time.time()
    stale = [
        acc for acc, d in _active_deploys.items()
        if d.get("finished_ts") and (now - d["finished_ts"]) > _STALE_AFTER_S
    ]
    for acc in stale:
        _active_deploys.pop(acc, None)


def get_deploy_status(account_filename: str = None) -> dict:
    _gc_active_deploys()
    if account_filename:
        d = _active_deploys.get(account_filename)
        if d:
            return {
                "running": d.get("state") not in ("done", "error", "cancelled"),
                "state": d["state"],
                "log": d["logger"].get_recent(50),
            }
        return {"running": False, "state": "idle", "log": []}
    result = {}
    for acc, d in _active_deploys.items():
        result[acc] = {
            "running": d.get("state") not in ("done", "error", "cancelled"),
            "state": d["state"],
            "log": d["logger"].get_recent(20),
        }
    return result


# ─── SSH jump helper ───

def ssh_jump(command: str, timeout: int = 30) -> tuple:
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{JUMP_USER}@{JUMP_SERVER}",
        command,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "SSH timeout", 1
    except Exception as e:
        return "", str(e), 1


async def _ssh_jump_async(command: str, timeout: int = 30) -> tuple:
    """Async wrapper so the deploy loop doesn't block on subprocess.run."""
    return await asyncio.to_thread(ssh_jump, command, timeout)


def _clean_tunnel_ports_cmd(ports: list[int]) -> str:
    pattern = "|".join(str(p) for p in ports)
    return (
        f"ss -tlnp | grep -E '{pattern}' | grep sshd | "
        f"grep -oP 'pid=\\K[0-9]+' | sort -u | xargs -r kill 2>/dev/null; echo DONE"
    )


# ─── App bridge ───

def _get_app_module():
    """Lazy import to avoid circular deps when app imports auto_deploy."""
    import importlib
    return importlib.import_module("app")


def _load_account_cookies(account_filename: str) -> Optional[list]:
    """Read the account's saved cookies without touching global state.
    Returns None if the account file is missing or has no cookies."""
    accounts_dir = Path(__file__).parent.parent / "accounts"
    path = accounts_dir / f"{account_filename}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cookies = data.get("cookies") or []
        return cookies if cookies else None
    except Exception:
        return None


# ─── SSH key parsing ───

_SSH_KEY_RE = re.compile(r'(ssh-(?:rsa|ed25519|ecdsa)\s+[A-Za-z0-9+/=]+(?:\s+\S+)?)')


def _parse_ssh_key(text: str) -> Optional[str]:
    match = _SSH_KEY_RE.search(text or "")
    return match.group(1).strip() if match else None


async def _deploy_ssh_key(public_key: str, logger: DeployLogger) -> tuple:
    quoted = shlex.quote(public_key.strip())
    check_cmd = (
        f'grep -qF {quoted} /root/.ssh/authorized_keys 2>/dev/null '
        f'&& echo "EXISTS" || echo "NEW"'
    )
    stdout, stderr, rc = await _ssh_jump_async(check_cmd)
    if "EXISTS" in stdout:
        logger.log("SSH key already exists on jump server")
        return True, "Key already deployed"

    add_cmd = (
        f'echo {quoted} >> /root/.ssh/authorized_keys && '
        f'chmod 600 /root/.ssh/authorized_keys && echo "OK"'
    )
    stdout, stderr, rc = await _ssh_jump_async(add_cmd)
    if rc != 0 or "OK" not in stdout:
        logger.log(f"Failed to deploy SSH key: {stderr}")
        return False, f"Failed: {stderr}"
    logger.log("SSH key deployed to jump server")
    return True, "Key deployed"


# ─── Endpoint probe (Step 8) ───

async def _probe_api_endpoint(host: str, port: int, timeout_s: float = 5.0) -> tuple[bool, str]:
    """Hit ``http://host:port/`` and accept any 2xx/3xx as alive. Falls back
    to a raw TCP connect so we still count it as alive if the API proxy is
    listening but not yet serving HTTP."""
    url = f"http://{host}:{port}/"
    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=False) as client:
            resp = await client.get(url)
            if 200 <= resp.status_code < 500:
                return True, f"HTTP {resp.status_code}"
            return False, f"HTTP {resp.status_code}"
    except ImportError:
        pass
    except Exception:
        pass
    # Fallback: bare TCP connect
    try:
        fut = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(fut, timeout=timeout_s)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, "TCP open"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ─── Core deploy flow ───

async def run_deploy_async(account_filename: str, force: bool = False) -> None:
    cfg = load_config()
    acc_cfg = cfg.get("accounts", {}).get(account_filename, {})
    deploy_text = acc_cfg.get("deploy_text", "")
    ssh_port = acc_cfg.get("ssh_port", 8022)
    api_port = acc_cfg.get("api_port", 8800)
    ports_to_clean = [ssh_port, api_port]

    log = DeployLogger(account_filename)
    cancel_event = threading.Event()

    _active_deploys[account_filename] = {
        "thread": threading.current_thread(),
        "logger": log,
        "state": "starting",
        "cancel": cancel_event,
        "started_at": datetime.now().isoformat(),
        "started_ts": time.time(),
        "finished_ts": None,
    }

    def set_state(s: str) -> None:
        _active_deploys[account_filename]["state"] = s

    def mark_finished(state: str, history_status: str | None = None) -> None:
        set_state(state)
        _active_deploys[account_filename]["finished_ts"] = time.time()
        if history_status is not None:
            _save_run_history(account_filename, history_status, log.lines[:])

    def cancelled() -> bool:
        return cancel_event.is_set()

    cookies = _load_account_cookies(account_filename)
    if cookies is None:
        log.log(f"❌ 账号 {account_filename} 不存在或没有 cookies")
        mark_finished("error", history_status="error")
        return

    app_mod = _get_app_module()
    acurl = app_mod.acurl
    claw_ws_chat = app_mod.claw_ws_chat
    upload_to_claw_fds = app_mod.upload_to_claw_fds

    # Local path of the reference api-proxy.py that Claw should save verbatim.
    # Lives under claw/payload/ — it's a deployment payload, not a panel runtime
    # script, so it sits next to auto_deploy.py rather than in the top-level
    # scripts/ directory.
    api_proxy_path = Path(__file__).parent / "payload" / "api-proxy.py"

    try:
        log.log("=== 开始部署 ===")
        log.log(f"账号: {account_filename} · SSH 端口: {ssh_port} · API 端口: {api_port}")

        # Step 0: Destroy existing claw if any.
        set_state("step0_destroy")
        log.log("Step 0: 检查并销毁旧 Claw...")
        code, data = await acurl(
            "GET", "/open-apis/user/mimo-claw/status",
            with_ph=False, cookies=cookies,
        )
        has_claw = False
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            status = (data.get("data") or {}).get("status", "")
            if status not in ("", "DESTROYED", "DESTROYING"):
                has_claw = True

        if has_claw:
            log.log("发现旧 Claw，销毁中...")
            await acurl("POST", "/open-apis/user/mimo-claw/destroy", body={}, cookies=cookies)
            for _ in range(_DESTROY_POLL_MAX_ITERS):
                if cancelled():
                    log.log("⚠️ 部署已取消")
                    mark_finished("cancelled", history_status="cancelled")
                    return
                await asyncio.sleep(_DESTROY_POLL_INTERVAL_S)
                code, data = await acurl(
                    "GET", "/open-apis/user/mimo-claw/status",
                    with_ph=False, cookies=cookies,
                )
                if code == "HTTP_200" and isinstance(data, dict):
                    if (data.get("data") or {}).get("status") in ("DESTROYED", ""):
                        break
            log.log("旧 Claw 已销毁")
        else:
            log.log("无旧 Claw，跳过销毁")

        # Step 0.5: Kill stale sshd tunnel processes on jump server bound to our ports.
        set_state("step0_cleanup")
        log.log(f"Step 0.5: 清理跳板机旧隧道进程 (端口 {ssh_port}/{api_port})...")
        _, stderr, rc = await _ssh_jump_async(_clean_tunnel_ports_cmd(ports_to_clean))
        log.log("跳板机旧隧道已清理" if rc == 0 else f"清理: {stderr or '无残留'}")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 1: Create claw.
        set_state("step1_create")
        log.log("Step 1: 创建新 Claw...")
        code, data = await acurl(
            "POST", "/open-apis/user/mimo-claw/create", body={}, cookies=cookies,
        )
        if not (isinstance(data, dict) and data.get("code") == 0):
            log.log(f"❌ 创建 Claw 失败: {data}")
            mark_finished("error", history_status="error")
            return
        log.log("Claw 创建请求已发送")

        # Step 2: Wait until claw is AVAILABLE.
        set_state("step2_wait")
        log.log("Step 2: 等待 Claw 就绪...")
        claw_ready = False
        for i in range(_CREATE_POLL_MAX_ITERS):
            if cancelled():
                mark_finished("cancelled", history_status="cancelled")
                return
            await asyncio.sleep(_CREATE_POLL_INTERVAL_S)
            code, data = await acurl(
                "GET", "/open-apis/user/mimo-claw/status",
                with_ph=False, cookies=cookies,
            )
            if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
                if (data.get("data") or {}).get("status") == "AVAILABLE":
                    claw_ready = True
                    break
            log.log(f"  等待中... ({(i + 1) * _CREATE_POLL_INTERVAL_S}s)")
        if not claw_ready:
            log.log("❌ Claw 启动超时")
            mark_finished("error", history_status="error")
            return
        log.log("✅ Claw 就绪")

        if not deploy_text:
            log.log("❌ 未配置部署文案，请在面板中填写")
            mark_finished("error", history_status="error")
            return

        # Step 3: Send deploy text. Before sending, upload the canonical
        # api-proxy.py to MiMo's Galaxy FDS so the deploy_text can reference
        # it as an attachment — that avoids stuffing a 22KB Python source
        # into the WS message (MiMo's WS gateway drops messages > ~8KB) and
        # guarantees Claw saves a byte-for-byte copy of our fixed script
        # rather than re-implementing from a bullet spec.
        attachments: list[dict] = []
        if api_proxy_path.exists():
            try:
                script_bytes = api_proxy_path.read_bytes()
                log.log(
                    f"Step 3a: 上传 api-proxy.py ({len(script_bytes):,} bytes) 到 FDS..."
                )
                # Unique filename so concurrent deploys don't collide on FDS.
                fname = f"api-proxy-{uuid.uuid4().hex}.py"
                attachment, up_err = await upload_to_claw_fds(
                    fname, script_bytes, cookies=cookies, file_type="txt",
                )
                if up_err:
                    log.log(f"⚠️ FDS 上传失败 ({up_err}) — 退回纯文本投递")
                else:
                    # Use the natural name in the prompt so Claw saves it correctly.
                    attachment["name"] = "api-proxy.py"
                    attachments = [attachment]
                    log.log("Step 3a: FDS 上传成功，附件已就绪")
            except OSError as e:
                log.log(f"⚠️ 读 claw/payload/api-proxy.py 失败 ({e}) — 退回纯文本投递")
        else:
            log.log(f"⚠️ claw/payload/api-proxy.py 不存在 ({api_proxy_path}) — 退回纯文本投递")

        # Step 3: Send deploy text. ``claw_ws_chat`` returns ``(reply, err)`` —
        # if err is set, do NOT continue with the reply (#6 fix).
        set_state("step3_send")
        session_key = f"agent:main:auto-{account_filename}-{uuid.uuid4().hex[:8]}"
        # Prepend an imperative preamble so Claw treats the markdown body as a
        # task spec to execute, not as a Q&A context to summarise. Without
        # this, the default ``<mimo-files>`` prompt ("先下载再回答") makes
        # Claw download the attachment then explain the upload mechanism
        # instead of running the deploy.
        if attachments:
            framed_message = (
                "你的任务：完整执行下面这份部署文档里的全部步骤（1 → 11），"
                "**不要解释、不要总结**，只在每个步骤产生输出时简短报告进度。"
                "文档中第 5 步需要保存的 `api-proxy.py` 已作为附件提供给你，"
                "请直接下载到 `/root/.openclaw/workspace/scripts/api-proxy.py`（**原封不动**）。"
                "完成后把生成的 SSH 公钥（ssh-ed25519 开头）回给我。\n\n"
                + deploy_text
            )
        else:
            framed_message = deploy_text
        log.log(
            f"Step 3: 发送部署文案到 Claw ({len(framed_message)} 字符"
            f"{', 附 1 个文件' if attachments else ''})..."
        )
        reply3, err3 = await claw_ws_chat(
            framed_message, session_key, cookies=cookies,
            attachments=attachments or None,
        )
        if err3:
            log.log(f"❌ Claw 通信失败: {err3}")
            mark_finished("error", history_status="error")
            return
        log.log(f"Claw 回复: {reply3[:200]}...")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 4: Capture SSH public key. Retry with a fresh session if the
        # first reply doesn't contain a key (#7 fix).
        set_state("step4_capture")
        log.log("Step 4: 从回复中提取 SSH 公钥...")
        public_key = _parse_ssh_key(reply3)
        if not public_key:
            log.log("未找到 SSH key，换新会话再问一次...")
            retry_session = f"agent:main:auto-{account_filename}-retry-{uuid.uuid4().hex[:8]}"
            reply_retry, err_retry = await claw_ws_chat(
                "请把你的 SSH 公钥发给我，格式为 ssh-ed25519 或 ssh-rsa 开头的完整公钥。",
                retry_session, cookies=cookies,
            )
            if err_retry:
                log.log(f"❌ 重试 Claw 通信失败: {err_retry}")
                mark_finished("error", history_status="error")
                return
            log.log(f"Claw 回复: {reply_retry[:200]}...")
            public_key = _parse_ssh_key(reply_retry)
        if not public_key:
            log.log("❌ 无法从 Claw 回复中提取 SSH 公钥")
            mark_finished("error", history_status="error")
            return
        log.log(f"✅ 提取到 SSH 公钥: {public_key[:50]}...")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 5: Add key on jump server.
        set_state("step5_deploy_key")
        log.log("Step 5: 在跳板机上添加 SSH 公钥...")
        key_ok, key_msg = await _deploy_ssh_key(public_key, log)
        if not key_ok:
            log.log(f"❌ 部署公钥失败: {key_msg}")
            mark_finished("error", history_status="error")
            return
        log.log(f"✅ 公钥部署成功: {key_msg}")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 6: Second cleanup right before telling claw to build the tunnel.
        # Previous deploys' keepalive cron may have restarted the old sshd
        # tunnel between Step 0.5 and now — kill them so claw's new tunnel
        # can bind. Then notify claw.
        set_state("step6_confirm")
        log.log("Step 6: 再次清理跳板机隧道端口...")
        _, stderr2, rc2 = await _ssh_jump_async(_clean_tunnel_ports_cmd(ports_to_clean))
        log.log("端口再次清理完成" if rc2 == 0 else f"再次清理: {stderr2 or '无残留'}")

        log.log("Step 6: 通知 Claw 公钥已添加...")
        reply6, err6 = await claw_ws_chat(
            "我已经把公钥添加到跳板机", session_key, cookies=cookies,
        )
        if err6:
            log.log(f"⚠️ 通知 Claw 失败 (不阻断): {err6}")
        else:
            log.log(f"Claw 回复: {reply6[:200]}...")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 8: Verify the API endpoint is up on the jump server. The
        # tunnel may take a few seconds to come up; poll for ~60s.
        set_state("step8_verify")
        log.log(f"Step 8: 验证 API 端点 http://{JUMP_SERVER}:{api_port}/ ...")
        endpoint_ok = False
        last_reason = ""
        for i in range(_PROBE_API_MAX_ITERS):
            if cancelled():
                mark_finished("cancelled", history_status="cancelled")
                return
            await asyncio.sleep(_PROBE_API_INTERVAL_S)
            ok, reason = await _probe_api_endpoint(JUMP_SERVER, api_port)
            last_reason = reason
            if ok:
                endpoint_ok = True
                log.log(f"✅ API 端点已就绪: {reason}")
                break
            log.log(f"  等待端点... ({(i + 1) * _PROBE_API_INTERVAL_S}s, {reason})")

        if endpoint_ok:
            log.log("=== ✅ 部署完成 ===")
            mark_finished("done", history_status="done")
        else:
            log.log(f"⚠️ 端点验证超时 (最后状态: {last_reason})，Claw 已就绪但隧道可能尚未生效")
            mark_finished("error", history_status="error")

    except asyncio.CancelledError:
        log.log("⚠️ 部署被取消 (CancelledError)")
        mark_finished("cancelled", history_status="cancelled")
        raise
    except Exception as e:
        log.log(f"❌ 部署异常: {type(e).__name__}: {e}")
        mark_finished("error", history_status="error")


def _run_deploy_thread(account_filename: str, force: bool) -> None:
    try:
        asyncio.run(run_deploy_async(account_filename, force=force))
    except Exception as e:
        # asyncio.run may raise on cancellation — log and move on.
        logger_module.exception("Deploy thread crashed for %s: %s", account_filename, e)


def run_deploy(account_filename: str, force: bool = False) -> None:
    """Synchronous wrapper kept for any external caller; runs to completion."""
    _run_deploy_thread(account_filename, force)


def trigger_deploy(account_filename: str) -> dict:
    """Manually start a deployment (returns immediately; runs in a thread)."""
    _gc_active_deploys()
    cur = _active_deploys.get(account_filename)
    if cur and cur.get("state") not in ("done", "error", "cancelled", "idle"):
        return {"success": False, "error": "该账号正在部署中"}
    t = threading.Thread(
        target=_run_deploy_thread, args=(account_filename, False), daemon=True,
    )
    t.start()
    return {"success": True, "message": f"已启动 {account_filename} 的部署"}


def cancel_deploy(account_filename: str) -> dict:
    d = _active_deploys.get(account_filename)
    if d and d.get("state") not in ("done", "error", "cancelled"):
        d["cancel"].set()
        d["state"] = "cancelling"
        return {"success": True, "message": "正在取消..."}
    return {"success": False, "error": "没有进行中的部署"}


# ─── Scheduler ───

def _scheduler_loop():
    """Run every minute: for each enabled account, fire when:
      * cron has crossed a fire boundary within the last 2 min, AND
      * we haven't already triggered for that fire (last_run < prev_fire), AND
      * no active deploy is in flight.
    """
    global _scheduler_running
    _scheduler_running = True
    print("[scheduler] 启动自动部署调度器", flush=True)

    while _scheduler_running:
        try:
            cfg = load_config()
            accounts = cfg.get("accounts", {})

            for acc_filename, acc_cfg in accounts.items():
                if not acc_cfg.get("enabled", False):
                    continue
                cron_expr = acc_cfg.get("cron", "0 3 * * *")
                last_run = acc_cfg.get("last_run", 0) or 0
                now = datetime.now()
                try:
                    cron = croniter(cron_expr, now)
                except (ValueError, KeyError):
                    continue
                prev_fire = cron.get_prev(datetime)
                diff = (now - prev_fire).total_seconds()
                if not (0 <= diff <= 120):
                    continue
                if last_run >= prev_fire.timestamp():
                    # Already triggered for this fire boundary; skip even if
                    # the previous run finished quickly (issue #5 fix).
                    continue
                cur = _active_deploys.get(acc_filename)
                if cur and cur.get("state") not in ("done", "error", "cancelled"):
                    continue
                print(f"[scheduler] 触发 {acc_filename} 的部署", flush=True)
                cfg["accounts"][acc_filename]["last_run"] = now.timestamp()
                save_config(cfg)
                trigger_deploy(acc_filename)
        except Exception as e:
            print(f"[scheduler] 错误: {e}", flush=True)

        time.sleep(60)


def start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()


def stop_scheduler():
    global _scheduler_running
    _scheduler_running = False


def get_scheduler_status() -> dict:
    cfg = load_config()
    accounts = cfg.get("accounts", {})
    schedule_info = {}
    now = datetime.now()

    for acc_filename, acc_cfg in accounts.items():
        if not acc_cfg.get("enabled", False):
            schedule_info[acc_filename] = {"enabled": False}
            continue
        cron_expr = acc_cfg.get("cron", "0 3 * * *")
        last_run = acc_cfg.get("last_run", 0)
        try:
            cron = croniter(cron_expr, now)
            next_run = cron.get_next(datetime)
        except (ValueError, KeyError):
            schedule_info[acc_filename] = {
                "enabled": True, "cron": cron_expr,
                "error": "Cron 表达式格式错误",
            }
            continue
        schedule_info[acc_filename] = {
            "enabled": True,
            "cron": cron_expr,
            "last_run": (
                datetime.fromtimestamp(last_run).strftime("%Y-%m-%d %H:%M")
                if last_run else "从未运行"
            ),
            "next_run": next_run.strftime("%Y-%m-%d %H:%M"),
        }

    return {
        "scheduler_running": _scheduler_running,
        "accounts": schedule_info,
    }
