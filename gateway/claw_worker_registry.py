"""Claw-worker registry + distributed deploy state machine.

The worker (a mainland-IP container, see agent/worker/) does the region-gated
MiMo parts: create claw + WS chat. The panel does everything else (add key to
jump server, ssh into claw, finalize, verify). This module is the panel side:

  * worker registry  — token-issued workers, online state (like probe nodes)
  * job dispatch      — picks a due ``worker_deploy`` account, hands a job out
  * step handlers     — claw_ready / notified / report, reusing auto_deploy's
                        server-side helpers (_deploy_ssh_key / _ecs_finalize)

Worker-managed accounts are flagged ``worker_deploy: true`` in
data/auto_deploy.json and scheduled by ``worker_cron`` / ``worker_last_run``.
They are INDEPENDENT of the local scheduler's ``enabled`` flag, so the two
deploy paths never collide and the local scheduler is untouched.
"""
from __future__ import annotations

import importlib
import json
import secrets
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data" / "claw_workers.json"
OFFLINE_AFTER_S = 180          # 3x the default 60s poll interval
JOB_STALE_AFTER_S = 30 * 60    # reclaim accounts whose worker died mid-job
DEFAULT_NOTIFY = "密钥已添加完成，请执行后续连接操作。"

_lock = threading.Lock()
_jobs: dict[str, dict] = {}        # job_id -> {account, worker_id, ssh_port, api_port, logger, status, started}
_inflight: dict[str, str] = {}     # account -> job_id


def _ad():
    """Lazy import auto_deploy to avoid import cycles (app imports both)."""
    return importlib.import_module("claw.auto_deploy")


# ─── worker registry (token == id, like probe) ───

def _empty():
    return {"workers": {}, "recent": []}


def _load() -> dict:
    if not DATA_PATH.exists():
        return _empty()
    try:
        d = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        d.setdefault("workers", {})
        d.setdefault("recent", [])
        return d
    except (OSError, json.JSONDecodeError):
        return _empty()


def _save(data: dict) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def list_workers(*, include_token: bool = False) -> list[dict]:
    with _lock:
        data = _load()
    now = time.time()
    out = []
    for wid, w in data["workers"].items():
        last = w.get("last_seen", 0)
        item = {
            "id": wid,
            "name": w.get("name", ""),
            "added_at": w.get("added_at", 0),
            "last_seen": last,
            "online": bool(last and (now - last) < OFFLINE_AFTER_S),
            "meta": w.get("meta", {}),
        }
        if include_token:
            item["token"] = w.get("token", wid)
        out.append(item)
    out.sort(key=lambda x: x["name"])
    return out


def add_worker(name: str) -> dict:
    name = (name or "").strip()
    if not name:
        raise ValueError("name required")
    token = secrets.token_urlsafe(24)
    with _lock:
        data = _load()
        data["workers"][token] = {
            "name": name, "token": token, "added_at": time.time(),
            "last_seen": 0, "meta": {},
        }
        _save(data)
    return {"id": token, "name": name, "token": token}


def delete_worker(worker_id: str) -> bool:
    with _lock:
        data = _load()
        if worker_id in data["workers"]:
            del data["workers"][worker_id]
            _save(data)
            return True
    return False


def regenerate_token(worker_id: str) -> str | None:
    with _lock:
        data = _load()
        w = data["workers"].pop(worker_id, None)
        if not w:
            return None
        token = secrets.token_urlsafe(24)
        w["token"] = token
        data["workers"][token] = w
        _save(data)
        return token


def touch(token: str, meta: dict) -> dict | None:
    """Validate token + update heartbeat. Returns {id,name} or None."""
    if not token:
        return None
    with _lock:
        data = _load()
        w = data["workers"].get(token)
        if not w:
            return None
        w["last_seen"] = time.time()
        if meta:
            w["meta"] = meta
        _save(data)
        return {"id": token, "name": w.get("name", "")}


# ─── job dispatch ───

def _reap_stale():
    now = time.time()
    for jid, job in list(_jobs.items()):
        if now - job.get("started", now) > JOB_STALE_AFTER_S:
            _inflight.pop(job["account"], None)
            _jobs.pop(jid, None)


def _due_worker_accounts(cfg: dict) -> list[str]:
    from croniter import croniter
    now = time.time()
    out = []
    for acc, c in cfg.get("accounts", {}).items():
        if not c.get("worker_deploy"):
            continue
        if acc in _inflight:
            continue
        cron_expr = c.get("worker_cron") or c.get("cron") or "0 3 * * *"
        last = c.get("worker_last_run", 0)
        try:
            base = datetime.fromtimestamp(last) if last else datetime.fromtimestamp(now - 86400)
            nxt = croniter(cron_expr, base).get_next(float)
        except (ValueError, KeyError, OSError):
            continue
        if nxt <= now:
            out.append(acc)
    return out


def claim_job(worker_id: str) -> dict | None:
    """Pick a due worker-managed account, mark in-flight, build a job."""
    ad = _ad()
    with _lock:
        _reap_stale()
        cfg = ad.load_config()
        due = _due_worker_accounts(cfg)
        if not due:
            return None
        acc = due[0]
        acc_cfg = cfg["accounts"][acc]
        cookies = ad._load_account_cookies(acc)
        if not cookies:
            return None
        job_id = uuid.uuid4().hex[:12]
        ssh_port = acc_cfg.get("ssh_port", 8022)
        api_port = acc_cfg.get("api_port", 8800)
        logger = ad.DeployLogger(acc)
        logger.log(f"=== worker 分布式部署 (worker={worker_id[:8]}, job={job_id}) ===")
        _inflight[acc] = job_id
        _jobs[job_id] = {
            "account": acc, "worker_id": worker_id, "ssh_port": ssh_port,
            "api_port": api_port, "logger": logger, "status": "dispatched",
            "started": time.time(),
        }
        return {
            "job_id": job_id, "account": acc,
            "deploy_text": acc_cfg.get("deploy_text", ""),
            "cookies": cookies, "ssh_port": ssh_port, "api_port": api_port,
            "notify_text": acc_cfg.get("notify_text", DEFAULT_NOTIFY),
        }


async def on_claw_ready(job_id: str, public_key: str, worker_log: str = ""):
    """Worker got the ECS pubkey. Add it to jump server + clean tunnel ports."""
    job = _jobs.get(job_id)
    if not job:
        return False, "unknown job"
    ad = _ad()
    log = job["logger"]
    if not public_key:
        return False, "no public_key"
    ok, msg = await ad._deploy_ssh_key(public_key, log)
    if not ok:
        job["status"] = "error"
        return False, msg
    try:
        cmd = ad._clean_tunnel_ports_cmd([job["ssh_port"], job["api_port"]])
        await ad._ssh_jump_async(cmd)
    except Exception as e:
        log.log(f"端口清理异常(忽略): {e}")
    job["status"] = "key_added"
    return True, "key added"


async def on_notified(job_id: str):
    """Worker notified claw → tunnel up. SSH into claw + finalize + verify."""
    job = _jobs.get(job_id)
    if not job:
        return False, "unknown job"
    ad = _ad()
    log = job["logger"]
    ok, msg = await ad._ecs_finalize(job["ssh_port"], job["api_port"], log)
    job["status"] = "finalized" if ok else "error"
    return ok, msg


def on_report(job_id: str, status: str, worker_log: str = "") -> None:
    """Final report from worker. Persist run history + clear in-flight +
    record a recent-result row so the panel can show success/failure."""
    job = _jobs.pop(job_id, None)
    if not job:
        return
    ad = _ad()
    log = job["logger"]
    if worker_log:
        for line in worker_log.splitlines():
            log.lines.append(f"[worker] {line}")
    log.log(f"=== 部署结束: {status} ===")
    hist_status = "success" if status == "done" else "error"
    summary = ""
    for ln in reversed(log.lines):
        s = ln.split("] ", 1)[-1].strip()
        if s and not s.startswith("==="):
            summary = s[:160]
            break
    try:
        ad._save_run_history(job["account"], hist_status, log.lines[:])
        if status == "done":
            cfg = ad.load_config()
            if job["account"] in cfg.get("accounts", {}):
                cfg["accounts"][job["account"]]["worker_last_run"] = time.time()
                ad.save_config(cfg)
    except Exception:
        pass
    _record_recent(job["account"], status, job["worker_id"], job_id, summary)
    _inflight.pop(job["account"], None)


def _record_recent(account: str, status: str, worker_id: str, job_id: str, summary: str) -> None:
    with _lock:
        data = _load()
        data["recent"].insert(0, {
            "account": account, "status": status, "worker_id": worker_id[:8],
            "job_id": job_id, "summary": summary, "finished_at": time.time(),
        })
        data["recent"] = data["recent"][:30]
        _save(data)


def list_recent() -> list[dict]:
    with _lock:
        return _load().get("recent", [])


def list_jobs() -> list[dict]:
    with _lock:
        return [
            {"job_id": jid, "account": j["account"], "status": j["status"],
             "worker_id": j["worker_id"][:8], "started": j["started"]}
            for jid, j in _jobs.items()
        ]
