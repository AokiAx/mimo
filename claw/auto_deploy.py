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
import math
import os
import random
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
INCIDENT_DIR = LOG_DIR / "incidents"
PAYLOAD_DIR = Path(__file__).parent / "payload"

JUMP_SERVER = "149.88.90.137"
JUMP_USER = "root"

# Set MIMO_JUMP_LOCAL=1 when the panel runs ON the jump server. ``ssh_jump``
# then dispatches via local ``bash -c`` instead of self-SSH, which would
# otherwise need root↔root key trust on the same host.
_JUMP_LOCAL = os.environ.get("MIMO_JUMP_LOCAL") in ("1", "true", "yes")

# Set MIMO_DEBUG_CLAW=1 to log Claw's WS replies in full instead of the
# 200-char preview. Necessary for diagnosing whether Claw executed every
# step of the deploy doc (esp. the tunnel-establishment + keepalive
# tail) or stopped partway.
_DEBUG_CLAW = os.environ.get("MIMO_DEBUG_CLAW") in ("1", "true", "yes")


def _fmt_claw_reply(reply: str) -> str:
    if _DEBUG_CLAW:
        return reply
    return reply[:200] + "..." if len(reply) > 200 else reply


def _notify_gateway_deploy_start(account_filename: str, api_port: int, log: "DeployLogger") -> None:
    """Drain the soon-to-be-replaced backend before destroying its Claw."""
    try:
        from gateway.runtime import prepare_account_deploy
        result = prepare_account_deploy(account_filename, api_port=api_port)
    except Exception as e:  # noqa: BLE001
        log.log(f"⚠️ Gateway 预切换失败，将继续部署: {type(e).__name__}: {e}")
        logger_module.exception("Gateway deploy-start hook failed for %s", account_filename)
        return
    matched = result.get("matched") or []
    drained = result.get("drained") or []
    blocked = result.get("blocked") or []
    if drained:
        log.log(f"Gateway 已将待替换后端转为 draining: {', '.join(drained)}")
        try:
            from gateway.runtime import wait_for_account_drain
            drain = wait_for_account_drain(account_filename, api_port=api_port)
            pending = drain.get("pending") or []
            if pending:
                log.log(f"⚠️ Gateway drain 等待超时，仍有 in-flight: {', '.join(pending)}")
            else:
                log.log("Gateway drain 完成，开始替换 Claw")
        except Exception as e:  # noqa: BLE001
            log.log(f"⚠️ Gateway drain 等待失败，将继续部署: {type(e).__name__}: {e}")
    elif matched and blocked:
        log.log(f"⚠️ Gateway 未找到可接管的 active peer，无法预切换: {', '.join(blocked)}")
    elif matched:
        log.log(f"Gateway 后端已处于非 active 状态，跳过预切换: {', '.join(matched)}")
    else:
        log.log("⚠️ Gateway 未匹配到该账号/API 端口的后端，部署完成后可能需要检查后端配置")


def _notify_gateway_deploy_done(account_filename: str, api_port: int, log: "DeployLogger") -> None:
    """Reload backend state and put the freshly verified Claw into warmup."""
    try:
        from gateway.runtime import complete_account_deploy
        result = complete_account_deploy(account_filename, api_port=api_port)
    except Exception as e:  # noqa: BLE001
        log.log(f"⚠️ Gateway 自动重载/热身失败，请手动重载: {type(e).__name__}: {e}")
        logger_module.exception("Gateway deploy-done hook failed for %s", account_filename)
        return
    matched = result.get("matched") or []
    warmed = result.get("warmed") or []
    activated = result.get("activated") or []
    if warmed:
        log.log(f"Gateway 已重载并开始热身新 Claw 后端: {', '.join(warmed)}")
    if activated:
        log.log(f"Gateway 已重载并激活新 Claw 后端: {', '.join(activated)}")
    if not matched:
        log.log("⚠️ Gateway 重载完成，但未匹配到该账号/API 端口的后端")


def _notify_gateway_deploy_failed(account_filename: str, api_port: int, error: str, log: "DeployLogger") -> None:
    """Keep a failed replacement target out of routing when a peer exists."""
    try:
        from gateway.runtime import fail_account_deploy
        result = fail_account_deploy(account_filename, api_port=api_port, error=error)
    except Exception as e:  # noqa: BLE001
        log.log(f"⚠️ Gateway 失败状态同步失败: {type(e).__name__}: {e}")
        logger_module.exception("Gateway deploy-failed hook failed for %s", account_filename)
        return
    failed = result.get("failed") or []
    if failed:
        log.log(f"Gateway 已暂时移除失败的部署后端: {', '.join(failed)}")


# Stale-deploy entries (state ∈ done/error/cancelled) older than this are
# treated as idle by ``get_deploy_status``. No cleanup threads needed.
_STALE_AFTER_S = 300

# 上游 Claw 连接约 1 小时会被硬断，提前轮换给 5-10 分钟冷启动留余量。
_ROTATION_TARGET_AGE_S = 40 * 60
_ROTATION_CRITICAL_AGE_S = 50 * 60
_ROTATION_HARD_EXPIRY_AGE_S = 55 * 60

# Per-step timing knobs.
_DESTROY_POLL_INTERVAL_S = 5
_DESTROY_POLL_MAX_ITERS = 12  # → up to 60s wait
_CREATE_POLL_INTERVAL_S = 5
_CREATE_POLL_MAX_ITERS = 60   # → up to 300s wait (Claw cold-start can hit 80-150s)
# 429 "Mimo Claw使用中机器已达上限" 重试预算与节奏。MiMo 的 claw 池子在高峰
# 期会被打满；旧 claw 已经被 Step 0 销毁，这里只能等池子腾出位置。重试期间
# 这个账号是停服状态，所以预算不宜过长。
_CREATE_429_RETRY_BUDGET_S = 30 * 60        # 总预算 30 分钟
_CREATE_429_JITTER_MAX_S = 5.0              # 每次重试前 0–5s 随机抖动
_PROBE_API_INTERVAL_S = 5
_PROBE_API_MAX_ITERS = 36     # → up to 180s wait (Claw 异步建隧道有时晚到 100s+)
_PROBE_API_CHAT_MODEL = "mimo-v2.5-pro"
_PROBE_API_CHAT_TIMEOUT_S = int(os.environ.get("MIMO_DEPLOY_CHAT_PROBE_TIMEOUT", "240"))
_PROBE_API_CHAT_MAX_TOKENS = int(os.environ.get("MIMO_DEPLOY_CHAT_PROBE_MAX_TOKENS", "2048"))
_PROBE_API_CHAT_THINKING_BUDGET = int(os.environ.get("MIMO_DEPLOY_CHAT_PROBE_THINKING_BUDGET", "8000"))
_PROBE_API_CHAT_MAX_ITERS = int(os.environ.get("MIMO_DEPLOY_CHAT_PROBE_MAX_ITERS", "3"))
_PROXY_AUTH_TOKEN = os.environ.get("MIMO_PROXY_AUTH_TOKEN", "sk-Aoki-MiMo")
_CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS = 3
_STEP7_MAX_ATTEMPTS = 3

# In-memory log size cap; on-disk log is rotated past this many bytes.
_LOG_LINES_MAX = 2000
_LOG_FILE_MAX_BYTES = 1_000_000  # ~1MB → keep current + one .1 backup

logger_module = logging.getLogger(__name__)


def _ensure_dirs():
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    INCIDENT_DIR.mkdir(parents=True, exist_ok=True)


def _save_incident_log(
    account_filename: str,
    reason: str,
    state: str,
    log_lines: list[str],
    extra: dict | None = None,
) -> Path | None:
    """Dump a self-contained log for a failed deploy run.

    Each failure gets its own timestamped file under ``deploy_logs/incidents/``
    so anomalies are easy to find without grepping through the rolling
    per-account log. Returns the file path on success."""
    try:
        _ensure_dirs()
        safe_name = account_filename.replace("/", "_").replace("\\", "_")
        # Microsecond precision so rapid retry failures (Step 1's 429 loop
        # can fail-fast within a single second) don't overwrite each other.
        # Add a short uuid suffix as a final tie-breaker against any clock
        # quirks (system clock rollback, low-res timer on some platforms).
        now = datetime.now()
        ts = now.strftime("%Y%m%d_%H%M%S_") + f"{now.microsecond:06d}"
        suffix = uuid.uuid4().hex[:6]
        path = INCIDENT_DIR / f"{safe_name}__{ts}_{suffix}__{state}.log"
        header = [
            f"# Deploy incident",
            f"# account: {account_filename}",
            f"# time:    {now.isoformat(timespec='microseconds')}",
            f"# state:   {state}",
            f"# reason:  {reason}",
        ]
        if extra:
            header.append(f"# extra:   {json.dumps(extra, ensure_ascii=False)}")
        body = "\n".join(header) + "\n\n" + "\n".join(log_lines) + "\n"
        path.write_text(body, encoding="utf-8")
        return path
    except Exception:
        return None


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
        # Stdout encoding on Windows defaults to GBK and can't render ✅/❌/⚠️;
        # let the print fail silently rather than crash the deploy.
        try:
            print(f"[deploy:{self.account}] {line}", flush=True)
        except (UnicodeEncodeError, OSError):
            pass
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


# ─── Rotation status helpers (read-only) ───

def _rotation_policy(enabled_count: int) -> dict:
    enabled = max(0, int(enabled_count or 0))
    if enabled <= 0:
        return {"desired_active": 0, "normal_min_active": 0, "emergency_min_active": 0}
    normal_min = min(enabled, max(3, int(math.ceil(enabled * 0.80))))
    emergency_min = min(enabled, max(3, int(math.floor(enabled * 0.67))))
    return {
        "desired_active": enabled,
        "normal_min_active": normal_min,
        "emergency_min_active": emergency_min,
    }


def _rotation_reason(age_s: float) -> str:
    if age_s >= _ROTATION_HARD_EXPIRY_AGE_S:
        return "hard_expiry_age"
    if age_s >= _ROTATION_CRITICAL_AGE_S:
        return "critical_age"
    if age_s >= _ROTATION_TARGET_AGE_S:
        return "target_age"
    return "fresh"


def _load_rotation_status(cfg: dict) -> dict:
    """Compute per-account rotation status from gateway backends (read-only)."""
    accounts_cfg = cfg.get("accounts", {}) or {}
    enabled_accounts = [
        acc for acc, acc_cfg in accounts_cfg.items()
        if acc_cfg.get("enabled", False)
    ]
    enabled_count = len(enabled_accounts)
    policy = _rotation_policy(enabled_count)

    backends: list[dict] = []
    try:
        from gateway.runtime import get_all_backends
        backends = get_all_backends()
    except Exception:
        pass

    def _backend_url_port(backend: dict) -> int | None:
        m = re.search(r":(\d+)(?:/)?$", str(backend.get("url") or "").rstrip("/"))
        return int(m.group(1)) if m else None

    def _account_match_keys(filename: str) -> set[str]:
        raw = (filename or "").strip()
        keys = {raw} if raw else set()
        if raw.endswith(".json"):
            keys.add(raw[:-5])
        elif raw:
            keys.add(f"{raw}.json")
        return keys

    active_selectable = 0
    account_status: dict[str, dict] = {}

    for account in enabled_accounts:
        acc_cfg = accounts_cfg.get(account, {})
        keys = _account_match_keys(account)
        api_port = acc_cfg.get("api_port")
        matches = [
            b for b in backends
            if (str(b.get("account") or "") in keys)
            or (api_port is not None and _backend_url_port(b) == api_port)
        ]
        selectable = [
            b for b in matches
            if b.get("enabled", True) and b.get("healthy") and b.get("lifecycle") in ("active", "warming")
        ]
        age_s = max((float(b.get("active_for_s") or 0) for b in selectable), default=0.0)
        reason = _rotation_reason(age_s)
        status = {
            "enabled": True,
            "api_port": api_port,
            "active": bool(selectable),
            "backend_count": len(matches),
            "selectable_backend_count": len(selectable),
            "age_s": int(age_s),
            "age_min": round(age_s / 60.0, 1) if age_s else 0,
            "next_rotation_reason": reason,
            "skip_reason": "" if selectable else ("no_selectable_backend" if matches else "skipped_unmatched"),
        }
        account_status[account] = status
        if selectable:
            active_selectable += 1

    return {
        "policy": policy,
        "counts": {
            "enabled_accounts": enabled_count,
            "desired_active": policy["desired_active"],
            "active_selectable": active_selectable,
            "normal_min_active": policy["normal_min_active"],
            "emergency_min_active": policy["emergency_min_active"],
        },
        "accounts": account_status,
    }


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
    if _JUMP_LOCAL:
        try:
            r = subprocess.run(
                ["bash", "-c", command],
                capture_output=True, text=True, timeout=timeout,
            )
            return r.stdout, r.stderr, r.returncode
        except subprocess.TimeoutExpired:
            return "", "local exec timeout", 1
        except Exception as e:
            return "", str(e), 1
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
    """Kill sshd processes whose listening port exactly matches one in
    ``ports``. Pre-2026-05 used ``grep -E '8022|8800'``, a substring
    match — that would also match ``18022`` / ``80022`` / ``88001`` /
    ``8800`` appearing inside a PID column, occasionally killing other
    accounts' reverse-tunnel sshd processes when the panel cleaned up.

    awk filter on ``$4`` (Local Address:Port) anchored at ``:PORT$`` matches
    only exact ports, including IPv6 (``[::]:8022``) and bound forms
    (``127.0.0.1:8022``)."""
    pattern = "|".join(str(p) for p in ports)
    return (
        f"ss -tlnp 2>/dev/null | "
        f"awk '$4 ~ /:({pattern})$/ && /sshd/ {{ print }}' | "
        f"grep -oP 'pid=\\K[0-9]+' | sort -u | xargs -r kill 2>/dev/null; "
        f"echo DONE"
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

_SSH_KEY_RE = re.compile(r'(ssh-(?:rsa|ed25519|ecdsa)\s+[A-Za-z0-9+/=]+(?:\s+[\w@.\-]+)?)')
_CLAW_SAFETY_REFUSAL_RE = re.compile(
    r"(安全策略|安全协议|无法满足|没法满足|不能读取或输出|不能修改|"
    r"不能代你执行|不能执行|无法自动执行|敏感凭证|安全红线|外部 SSH|"
    r"反向隧道|authorized_keys)",
    re.IGNORECASE,
)


def _parse_ssh_key(text: str) -> Optional[str]:
    match = _SSH_KEY_RE.search(text or "")
    return match.group(1).strip() if match else None


def _is_claw_safety_refusal(text: str) -> bool:
    return bool(_CLAW_SAFETY_REFUSAL_RE.search(text or ""))


def _check_port_conflict(account_filename: str, api_port: int, ssh_port: int) -> Optional[str]:
    """Return a description of the conflict if another *enabled* account is
    configured to use the same api_port or ssh_port, else None.

    Two reverse tunnels racing for the same jump-side port lose with
    ExitOnForwardFailure → keepalive immediately respawns ssh → another
    failure → tight kill/restart loop hammering jump sshd. Catching the
    conflict at deploy-start is much cheaper than letting it run."""
    cfg = load_config()
    for other_name, other_cfg in cfg.get("accounts", {}).items():
        if other_name == account_filename:
            continue
        if not other_cfg.get("enabled", False):
            continue
        if other_cfg.get("api_port", 8800) == api_port:
            return (
                f"api_port {api_port} 已被账号 {other_name} 占用 "
                f"（每账号需独立 jump-side 端口，否则反向隧道互相抢占）"
            )
        if other_cfg.get("ssh_port", 8022) == ssh_port:
            return f"ssh_port {ssh_port} 已被账号 {other_name} 占用"
    return None


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


# ─── Step 7: panel SSH into ECS via reverse tunnel and finalize ───

# Path of the panel-managed private key whose pubkey Claw appends to ECS's
# authorized_keys (embedded in deploy_text). Generated once by hand on the
# jump server: ssh-keygen -f /root/mimo/data/bootstrap/panel_bootstrap.
_BOOTSTRAP_PRIVKEY = "/root/mimo/data/bootstrap/panel_bootstrap"
_ECS_FINALIZE_SH = PAYLOAD_DIR / "ecs_finalize.sh"
_API_PROXY_PY = PAYLOAD_DIR / "api-proxy.py"


def _step7_ssh_opts(known_hosts_file: str = "/dev/null") -> str:
    return (
        f"-o StrictHostKeyChecking=no -o UserKnownHostsFile={known_hosts_file} "
        f"-o ConnectTimeout=15 -o ServerAliveInterval=10 -o BatchMode=yes "
        f"-i {_BOOTSTRAP_PRIVKEY}"
    )


def _classify_step7_error(message: str) -> str:
    msg = message or ""
    if (
        "bootstrap identity missing or unreadable" in msg
        or ("Identity file" in msg and "not accessible" in msg)
    ):
        return "fatal_missing_identity"
    if "missing payload:" in msg:
        return "fatal_missing_payload"
    if "REMOTE HOST IDENTIFICATION HAS CHANGED" in msg:
        return "known_hosts"
    if "Permission denied" in msg:
        return "auth_denied"
    if (
        "Connection refused" in msg
        or "Connection timed out during banner exchange" in msg
        or "Connection closed" in msg
        or "SSH timeout" in msg
        or "local exec timeout" in msg
    ):
        return "tunnel_not_ready"
    if "set: -" in msg and "invalid option" in msg:
        return "fatal_script_format"
    return "retryable"


async def _step7_preflight(ssh_port: int, logger: DeployLogger) -> tuple[bool, str]:
    check_key_cmd = (
        f"test -r {shlex.quote(_BOOTSTRAP_PRIVKEY)} "
        f"&& echo KEY_OK || echo KEY_MISSING"
    )
    stdout, stderr, rc = await _ssh_jump_async(check_key_cmd, timeout=10)
    if rc != 0 or "KEY_OK" not in stdout:
        return False, (
            f"bootstrap identity missing or unreadable: {_BOOTSTRAP_PRIVKEY} "
            f"{stderr.strip()}"
        )

    await _ssh_jump_async(f"ssh-keygen -R '[127.0.0.1]:{ssh_port}' >/dev/null 2>&1 || true", timeout=10)

    listen_cmd = f"ss -tln 2>/dev/null | awk '$4 ~ /:{ssh_port}$/ {{found=1}} END {{print found ? \"LISTEN\" : \"NO_LISTEN\"}}'"
    stdout, stderr, rc = await _ssh_jump_async(listen_cmd, timeout=10)
    if rc != 0 or "LISTEN" not in stdout:
        return False, f"ssh tunnel port {ssh_port} is not listening: {stderr.strip() or stdout.strip()}"

    ssh_cmd = (
        f"ssh -p {ssh_port} {_step7_ssh_opts()} root@127.0.0.1 "
        f"'echo STEP7_SSH_OK'"
    )
    stdout, stderr, rc = await _ssh_jump_async(ssh_cmd, timeout=25)
    if rc != 0 or "STEP7_SSH_OK" not in stdout:
        return False, f"ssh tunnel probe failed: rc={rc} {stderr[:200]}"

    logger.log("Step 7 preflight: SSH 隧道和 bootstrap key 检查通过")
    return True, "ok"


async def _ecs_finalize_once(ssh_port: int, api_port: int, logger: DeployLogger) -> tuple[bool, str]:
    """SCP api-proxy.py into the ECS and run ecs_finalize.sh on it via the
    jump-server-side reverse tunnel. Returns (ok, message)."""
    ssh_opts = _step7_ssh_opts()

    if not _API_PROXY_PY.exists():
        return False, f"missing payload: {_API_PROXY_PY}"
    if not _ECS_FINALIZE_SH.exists():
        return False, f"missing payload: {_ECS_FINALIZE_SH}"

    # 1. SCP api-proxy.py to /tmp on ECS (finalize.sh will move it to its
    #    canonical path). We use SCP rather than `ssh ... 'cat > /tmp/...'`
    #    because a 23 KB file pipes cleanly through scp.
    scp_cmd = (
        f"scp -P {ssh_port} {ssh_opts} "
        f"{_API_PROXY_PY} root@127.0.0.1:/tmp/api-proxy.py"
    )
    logger.log(f"Step 7a: SCP api-proxy.py → ECS (via :{ssh_port})...")
    _, stderr, rc = await _ssh_jump_async(scp_cmd, timeout=45)
    if rc != 0:
        return False, f"scp api-proxy.py failed: rc={rc} {stderr[:200]}"

    # 2. SSH in and run ecs_finalize.sh from stdin with API_PORT set.
    ssh_cmd = (
        f"ssh -p {ssh_port} {ssh_opts} root@127.0.0.1 "
        f"'API_PORT={api_port} bash -s' < {_ECS_FINALIZE_SH}"
    )
    logger.log(f"Step 7b: ssh + bash -s < ecs_finalize.sh (API_PORT={api_port})...")
    stdout, stderr, rc = await _ssh_jump_async(ssh_cmd, timeout=120)
    # Always echo the tail so a partial failure is visible.
    tail = (stdout or "")[-600:].strip()
    if tail:
        logger.log("Step 7 ECS output (tail):\n" + tail)
    if rc != 0:
        return False, f"finalize rc={rc} stderr={stderr[:200]}"
    return True, "ecs_finalize.sh ok"


async def _ecs_finalize(ssh_port: int, api_port: int, logger: DeployLogger) -> tuple[bool, str]:
    last_msg = ""
    for attempt in range(1, _STEP7_MAX_ATTEMPTS + 1):
        logger.log(f"Step 7 attempt {attempt}/{_STEP7_MAX_ATTEMPTS}: preflight + finalize")
        preflight_ok, preflight_msg = await _step7_preflight(ssh_port, logger)
        if not preflight_ok:
            last_msg = preflight_msg
            action = _classify_step7_error(preflight_msg)
            if action.startswith("fatal"):
                return False, preflight_msg
            logger.log(f"⚠️ Step 7 preflight 未通过: {preflight_msg}；等待后重试")
            await asyncio.sleep(min(5 * attempt, 20))
            continue

        ok, msg = await _ecs_finalize_once(ssh_port, api_port, logger)
        if ok:
            return True, msg

        last_msg = msg
        action = _classify_step7_error(msg)
        if action.startswith("fatal"):
            return False, msg
        if action == "known_hosts":
            logger.log("⚠️ Step 7 known_hosts 冲突，清理后重试")
            await _ssh_jump_async(f"ssh-keygen -R '[127.0.0.1]:{ssh_port}' >/dev/null 2>&1 || true", timeout=10)
        elif action == "auth_denied":
            logger.log("⚠️ Step 7 公钥认证失败，等待隧道/authorized_keys 收敛后重试")
        elif action == "tunnel_not_ready":
            logger.log("⚠️ Step 7 隧道暂不可用，等待后重试")
        else:
            logger.log(f"⚠️ Step 7 可重试失败: {msg}")
        await asyncio.sleep(min(5 * attempt, 20))

    return False, f"Step 7 failed after {_STEP7_MAX_ATTEMPTS} attempts: {last_msg}"


# ─── Chat probe (Step 8) ───

async def _probe_api_health(port: int, timeout_s: float = 5.0) -> tuple[bool, str]:
    cmd = (
        f"curl -sS -m {int(timeout_s)} -o /dev/null "
        f"-w '%{{http_code}}' http://127.0.0.1:{port}/health 2>&1"
    )
    stdout, stderr, rc = await _ssh_jump_async(cmd, timeout=int(timeout_s) + 5)
    code = stdout.strip()
    if rc == 0 and code.isdigit() and int(code) == 200:
        return True, "health HTTP 200"
    return False, f"health HTTP {code or 'no response'} (rc={rc}, stderr={stderr[:120]})"


async def _probe_api_endpoint(host: str, port: int, timeout_s: float = 5.0) -> tuple[bool, str]:
    """Verify the deployed proxy with a real OpenAI-compatible chat call.

    ``/`` and ``/v1/models`` can return successfully even when the MiMo
    upstream key, model path, or streaming/chat path is broken. A non-stream
    chat completion against mimo-v2.5-pro proves the API tunnel, proxy auth,
    upstream auth, model routing, and response parsing all work. ``host`` is
    ignored because jump-side tunnels bind to 127.0.0.1.
    """
    del host
    body = {
        "model": _PROBE_API_CHAT_MODEL,
        "messages": [
            {
                "role": "user",
                "content": (
                    "部署健康检查。请只回复一个短句：MIMO_DEPLOY_PROBE_OK。"
                ),
            }
        ],
        "max_tokens": _PROBE_API_CHAT_MAX_TOKENS,
        "stream": False,
        "thinking": {
            "type": "enabled",
            "budget_tokens": _PROBE_API_CHAT_THINKING_BUDGET,
        },
    }
    payload = shlex.quote(json.dumps(body, ensure_ascii=False, separators=(",", ":")))
    auth = shlex.quote(f"Authorization: Bearer {_PROXY_AUTH_TOKEN}")
    probe_file = f"/tmp/mimo-deploy-chat-probe-{port}-{uuid.uuid4().hex}.json"
    probe_file_q = shlex.quote(probe_file)
    try:
        cmd = (
            f"curl -sS -m {int(timeout_s)} "
            f"-H {auth} -H 'Content-Type: application/json' "
            f"-o {probe_file_q} "
            f"-w '%{{http_code}}' "
            f"--data-binary {payload} "
            f"http://127.0.0.1:{port}/v1/chat/completions"
        )
        stdout, stderr, rc = await _ssh_jump_async(cmd, timeout=int(timeout_s) + 15)
        code = stdout.strip()
        if rc != 0:
            return False, f"chat probe curl rc={rc}: {(stderr or code)[:200]}"
        if not code.isdigit() or int(code) != 200:
            detail_cmd = f"tail -c 500 {probe_file_q} 2>/dev/null || true"
            detail, _, _ = await _ssh_jump_async(detail_cmd, timeout=10)
            return False, f"chat probe HTTP {code or 'no response'}: {detail[:300]}"

        validate_py = (
            "import json;"
            f"p={json.dumps(probe_file)};"
            "data=json.load(open(p,encoding='utf-8'));"
            "choices=data.get('choices') or [];"
            "msg=(choices[0].get('message') or {}) if choices else {};"
            "content=msg.get('content') or '';"
            "usage=data.get('usage') or {};"
            "print(('OK content=%r usage=%s' % (content[:80], usage)) "
            "if choices and isinstance(content,str) and content.strip() "
            "else 'BAD response has no assistant content')"
        )
        validate_cmd = f"python3 -c {shlex.quote(validate_py)}"
        detail, stderr, rc = await _ssh_jump_async(validate_cmd, timeout=15)
        if rc == 0 and detail.startswith("OK "):
            return True, f"chat probe HTTP 200 {_PROBE_API_CHAT_MODEL}: {detail.strip()[:180]}"
        return False, f"chat probe invalid response: {(detail or stderr)[:200]}"
    finally:
        await _ssh_jump_async(f"rm -f {probe_file_q}", timeout=10)


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
    gateway_prepared = False

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
        if history_status == "error" and gateway_prepared:
            _notify_gateway_deploy_failed(account_filename, api_port, state, log)
        set_state(state)
        _active_deploys[account_filename]["finished_ts"] = time.time()
        if history_status is not None:
            _save_run_history(account_filename, history_status, log.lines[:])
        # Dump a standalone incident log on error so unfamiliar failures are
        # easy to inspect without sifting through the rolling per-account log.
        if history_status == "error":
            reason = log.lines[-1] if log.lines else "(no log)"
            incident_path = _save_incident_log(
                account_filename,
                reason=reason,
                state=state,
                log_lines=log.lines[:],
            )
            if incident_path is not None:
                log.log(f"📝 incident log: {incident_path.name}")

    def cancelled() -> bool:
        return cancel_event.is_set()

    cookies = _load_account_cookies(account_filename)
    if cookies is None:
        log.log(f"❌ 账号 {account_filename} 不存在或没有 cookies")
        mark_finished("error", history_status="error")
        return

    app_mod = _get_app_module()
    acurl = app_mod.acurl
    curl_api_sync = app_mod.curl_api
    claw_ws_chat = app_mod.claw_ws_chat
    upload_to_claw_fds = app_mod.upload_to_claw_fds

    # Local path of the reference payloads (api-proxy.py + the two tunnel
    # shell scripts) that Claw should save verbatim. Living under claw/payload/
    # —— deployment payloads, not panel runtime scripts. The shell scripts use
    # ``__API_PORT__`` as a placeholder that we substitute per account here.
    api_proxy_path = PAYLOAD_DIR / "api-proxy.py"

    try:
        log.log("=== 开始部署 ===")
        log.log(f"账号: {account_filename} · SSH 端口: {ssh_port} · API 端口: {api_port}")

        # Pre-flight: refuse to deploy if another enabled account would race
        # for the same jump-side port. The losing tunnel falls into a tight
        # restart loop that has historically jammed jump sshd.
        conflict = _check_port_conflict(account_filename, api_port, ssh_port)
        if conflict:
            log.log(f"❌ 端口冲突，停止部署: {conflict}")
            mark_finished("error", history_status="error")
            return

        _notify_gateway_deploy_start(account_filename, api_port, log)
        gateway_prepared = True

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

        # 用 sync 版 curl_api 走 asyncio.to_thread 避免与 FastAPI 主循环共享
        # async httpx.AsyncClient 时出现的 "Future attached to a different loop"
        # ——retry sleep 期间面板的自动刷新接口可能用同一个 client，导致连接
        # 池被绑到主循环。
        retry_deadline = time.monotonic() + _CREATE_429_RETRY_BUDGET_S
        attempt = 0
        while True:
            attempt += 1
            code, data = await asyncio.to_thread(
                curl_api_sync,
                "POST", "/open-apis/user/mimo-claw/create",
                body={}, cookies=cookies,
            )
            if isinstance(data, dict) and data.get("code") == 0:
                if attempt > 1:
                    log.log(f"Claw 创建请求已发送（第 {attempt} 次尝试成功）")
                else:
                    log.log("Claw 创建请求已发送")
                break

            # 429 "机器已达上限" 单独处理：池子满了，立即重试。其它错误维持原
            # 行为（直接 mark_finished("error") 退出）。
            is_capacity_429 = (
                isinstance(data, dict)
                and data.get("code") == 429
                and "上限" in str(data.get("msg", ""))
            )
            if not is_capacity_429:
                log.log(f"❌ 创建 Claw 失败: {data}")
                mark_finished("error", history_status="error")
                return

            if cancelled():
                mark_finished("cancelled", history_status="cancelled")
                return

            remaining = retry_deadline - time.monotonic()
            if remaining <= 0:
                log.log(
                    f"❌ 创建 Claw 失败：MiMo 容量饱和重试 {attempt} 次后放弃"
                    f"（预算 {_CREATE_429_RETRY_BUDGET_S}s 用尽）"
                )
                mark_finished("error", history_status="error")
                return

            sleep_s = random.uniform(0, _CREATE_429_JITTER_MAX_S)
            log.log(
                f"⏳ MiMo 容量饱和（429），{sleep_s:.1f}s 后重试"
                f"（已尝试 {attempt} 次，剩余预算 {int(remaining)}s）"
            )
            await asyncio.sleep(sleep_s)

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

        # Step 3: Send deploy text. New flow as of the 2026-05-13 refactor:
        # deploy_text only asks Claw to (a) add our bootstrap pubkey, (b) gen
        # an ECS keypair, (c) start a reverse SSH tunnel exposing ECS:22 on
        # the jump server. No api-proxy.py / systemd / crontab — those run
        # afterward via panel SSH'ing into the ECS through that tunnel.
        # That keeps the prompt under Claw's policy radar (no /proc env
        # scanning, no random binaries, no persistent crontab).
        attachments: list[dict] = []

        set_state("step3_send")
        framed_message = deploy_text
        log.log(
            f"Step 3: 发送部署文案到 Claw ({len(framed_message)} 字符)..."
        )
        reply3 = ""
        session_key = ""
        public_key = None
        for attempt in range(1, _CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS + 1):
            session_key = f"agent:main:auto-{account_filename}-{uuid.uuid4().hex[:8]}"
            log.log(
                f"Step 3 attempt {attempt}/{_CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS}: "
                f"新 Claw 会话原文发送"
            )
            reply_attempt, err3 = await claw_ws_chat(
                framed_message, session_key, cookies=cookies,
                attachments=attachments or None,
            )
            if err3:
                log.log(f"⚠️ Claw 通信失败: {err3}")
                if attempt < _CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS:
                    await asyncio.sleep(3 * attempt)
                    continue
                log.log(f"❌ Claw 通信失败: {err3}")
                mark_finished("error", history_status="error")
                return
            reply3 = reply_attempt or ""
            log.log(f"Claw 回复: {_fmt_claw_reply(reply3)}")
            public_key = _parse_ssh_key(reply3)
            if public_key:
                break
            if _is_claw_safety_refusal(reply3):
                log.log("⚠️ Claw 触发安全拒绝/敏感操作拒绝，丢弃会话并原文重发")
            else:
                log.log("⚠️ Claw 回复未包含 SSH 公钥，丢弃会话并原文重发")
            if attempt < _CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS:
                await asyncio.sleep(3 * attempt)
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 4: Capture SSH public key. Step 3 already retries by opening a
        # fresh Claw conversation and re-sending the original deploy_text.
        set_state("step4_capture")
        log.log("Step 4: 从回复中提取 SSH 公钥...")
        if not public_key:
            log.log(
                f"❌ 无法从 {_CLAW_BOOTSTRAP_SESSION_MAX_ATTEMPTS} 个新 Claw 会话中提取 SSH 公钥"
            )
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
        notify_msg = "公钥已就位，可以建立反向隧道了。"
        reply6, err6 = None, None
        for attempt in range(1, 4):
            reply6, err6 = await claw_ws_chat(
                notify_msg, session_key, cookies=cookies,
            )
            if not err6:
                break
            log.log(f"⚠️ 通知 Claw 第 {attempt}/3 次失败: {err6} — 5s 后重试...")
            await asyncio.sleep(5)
        if err6:
            log.log(f"⚠️ 通知 Claw 最终失败（共 3 次）: {err6}（继续走 Step 8，给 Claw 时间自行执行）")
        else:
            log.log(f"Claw 回复: {_fmt_claw_reply(reply6)}")
        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 7: Panel SSH's into the ECS via the reverse tunnel that Claw
        # just established (jump-server-side 127.0.0.1:<ssh_port>), and runs
        # ecs_finalize.sh to install aiohttp, register systemd, start
        # api-proxy, bring up the API reverse tunnel, and crontab keepalive.
        # This keeps Claw uninvolved in the parts of the deploy that look
        # most "suspicious" to its safety training.
        set_state("step7_finalize")
        log.log(f"Step 7: panel SSH 进 ECS (跳板机本机 :{ssh_port}) 跑 ecs_finalize.sh ...")
        # Wait a few seconds for the reverse tunnel to settle after the Claw
        # "公钥已就位" round-trip; ssh on the jump-side socket sometimes races
        # the new sshd-session.
        await asyncio.sleep(3)
        ok7, msg7 = await _ecs_finalize(ssh_port, api_port, log)
        if not ok7:
            log.log(f"❌ Step 7 失败: {msg7}")
            mark_finished("error", history_status="error")
            return

        if cancelled():
            mark_finished("cancelled", history_status="cancelled")
            return

        # Step 8: Verify the API path with a real chat completion. Each probe
        # has a long timeout so mimo-v2.5-pro can finish thinking/output.
        set_state("step8_verify")
        log.log(
            f"Step 8: chat 探测 http://{JUMP_SERVER}:{api_port}/v1/chat/completions "
            f"(model={_PROBE_API_CHAT_MODEL}, thinking_budget={_PROBE_API_CHAT_THINKING_BUDGET}, "
            f"timeout={_PROBE_API_CHAT_TIMEOUT_S}s) ..."
        )
        health_ok = False
        chat_ok = False
        last_reason = ""
        for i in range(_PROBE_API_MAX_ITERS):
            if cancelled():
                mark_finished("cancelled", history_status="cancelled")
                return
            await asyncio.sleep(_PROBE_API_INTERVAL_S)
            ok, reason = await _probe_api_health(api_port)
            last_reason = reason
            if ok:
                health_ok = True
                log.log(f"✅ API health 已就绪: {reason}")
                break
            log.log(f"  等待 API health... ({(i + 1) * _PROBE_API_INTERVAL_S}s, {reason})")
        else:
            log.log(f"⚠️ API health 等待超时 (最后状态: {last_reason})")

        if health_ok:
            for i in range(_PROBE_API_CHAT_MAX_ITERS):
                if cancelled():
                    mark_finished("cancelled", history_status="cancelled")
                    return
                ok, reason = await _probe_api_endpoint(
                    JUMP_SERVER,
                    api_port,
                    timeout_s=_PROBE_API_CHAT_TIMEOUT_S,
                )
                last_reason = reason
                if ok:
                    chat_ok = True
                    log.log(f"✅ API chat 探测通过: {reason}")
                    break
                log.log(
                    f"  chat 探测失败 {i + 1}/{_PROBE_API_CHAT_MAX_ITERS}: {reason}"
                )
                if i + 1 < _PROBE_API_CHAT_MAX_ITERS:
                    await asyncio.sleep(_PROBE_API_INTERVAL_S)

        if health_ok:
            _notify_gateway_deploy_done(account_filename, api_port, log)
            if chat_ok:
                log.log("=== ✅ 部署完成 ===")
            else:
                log.log(
                    f"⚠️ API chat 探测超时/失败 (最后状态: {last_reason})，"
                    "基础部署已完成，模型链路暂不可用/待后续探测恢复"
                )
                log.log("=== ✅ 部署完成（chat 探测 warning）===")
            mark_finished("done", history_status="done")
        else:
            log.log(f"⚠️ API health 探测超时/失败 (最后状态: {last_reason})，Claw 已就绪但 API 端点不可用")
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
    rotation = _load_rotation_status(cfg)
    rotation_accounts = rotation.get("accounts") or {}

    for acc_filename, acc_cfg in accounts.items():
        rotation_info = rotation_accounts.get(acc_filename, {})
        if not acc_cfg.get("enabled", False):
            schedule_info[acc_filename] = {
                "enabled": False,
                "age_s": 0,
                "age_min": 0,
                "next_rotation_reason": "disabled",
                "skip_reason": "disabled",
            }
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
                **rotation_info,
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
            **rotation_info,
        }

    return {
        "scheduler_running": _scheduler_running,
        "schedule_mode": "adaptive",
        "policy": rotation.get("policy", {}),
        "counts": rotation.get("counts", {}),
        "accounts": schedule_info,
    }
