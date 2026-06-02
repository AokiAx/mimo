"""Claw-worker registry + distributed deploy state machine (WS tunnel mode).

The worker (a mainland-IP container, see agent/worker/) does the region-gated
MiMo parts: create claw + WS chat (template reset + inject the ws-bridge). The
bridge then dials back to this gateway's /ws. The panel does dispatch, drain,
verification (is the account's bridge node online?) and warmup. No jump server,
no SSH — the panel never touches the claw machine directly.

  * worker registry  — token-issued workers, online state (like probe nodes)
  * job dispatch      — picks a due ``worker_deploy`` account, hands a job out
                        (reset message + the per-account bridge inject prompt)
  * step handlers     — verify / report

Worker-managed accounts are flagged ``worker_deploy: true`` in
data/auto_deploy.json and scheduled by ``worker_cron`` / ``worker_last_run``.
They are INDEPENDENT of the local scheduler's ``enabled`` flag, so the two
deploy paths never collide and the local scheduler is untouched.

Live logs: a job's DeployLogger is registered into ``auto_deploy._active_deploys``
under the account, so the existing /api/auto-deploy/status/<account> endpoint and
the panel deploy-log modal stream worker progress just like a local deploy.
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
WORKER_FIRE_WINDOW_S = 180     # only fire within this window after a cron boundary

_lock = threading.Lock()
_jobs: dict[str, dict] = {}        # job_id -> {account, worker_id, logger, status, started}
_inflight: dict[str, str] = {}     # account -> job_id
_force: set[str] = set()           # accounts queued for an immediate (manual) deploy


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
    """Cron-due accounts, matching the local scheduler's semantics: only fire
    within WORKER_FIRE_WINDOW_S after a cron boundary (no catch-up / no
    fire-on-enable), and not again for the same boundary."""
    from croniter import croniter
    now_dt = datetime.now()
    out = []
    for acc, c in cfg.get("accounts", {}).items():
        if not c.get("worker_deploy"):
            continue
        if acc in _inflight or acc in _force:
            continue
        cron_expr = c.get("worker_cron") or c.get("cron") or "0 3 * * *"
        last = c.get("worker_last_run", 0) or 0
        try:
            prev_fire = croniter(cron_expr, now_dt).get_prev(datetime)
        except (ValueError, KeyError):
            continue
        diff = (now_dt - prev_fire).total_seconds()
        if not (0 <= diff <= WORKER_FIRE_WINDOW_S):
            continue
        if last >= prev_fire.timestamp():
            continue
        out.append(acc)
    return out


def force_deploy(account: str) -> bool:
    """Queue an immediate (manual) deploy, bypassing cron. Picked up on the
    next worker poll."""
    cfg = _ad().load_config()
    if account not in cfg.get("accounts", {}):
        return False
    with _lock:
        _force.add(account)
    return True


def claim_job(worker_id: str) -> dict | None:
    """Pick a forced or cron-due worker-managed account, mark in-flight + mark
    the cron boundary fired (worker_last_run=now), build a WS deploy job."""
    ad = _ad()
    if not getattr(ad, "_WS_PUBLIC_URL", ""):
        # No public /ws URL configured → a bridge would have nowhere to dial.
        return None
    with _lock:
        _reap_stale()
        cfg = ad.load_config()
        acc = None
        for f in list(_force):
            if f in cfg.get("accounts", {}) and f not in _inflight:
                acc = f
                _force.discard(f)
                break
        if acc is None:
            due = _due_worker_accounts(cfg)
            acc = due[0] if due else None
        if acc is None:
            return None
        acc_cfg = cfg["accounts"][acc]
        cookies = ad._load_account_cookies(acc)
        if not cookies:
            return None
        # Mark this cron boundary as fired NOW (before dispatch) so polls during
        # the deploy — and after a failure — don't re-fire it (matches the local
        # scheduler, which sets last_run before triggering).
        cfg["accounts"][acc]["worker_last_run"] = time.time()
        ad.save_config(cfg)
        job_id = uuid.uuid4().hex[:12]
        logger = ad.DeployLogger(acc)
        logger.log(f"=== worker 分布式部署 (WS, worker={worker_id[:8]}, job={job_id}) ===")
        _register_active(acc, logger)
        _set_active_state(acc, "worker:dispatched")
        _inflight[acc] = job_id
        _jobs[job_id] = {
            "account": acc, "worker_id": worker_id, "logger": logger,
            "status": "dispatched", "started": time.time(), "log_len": 0,
        }
        # Panel: drain the soon-to-be-replaced backend before the worker
        # recreates the claw. Best-effort.
        try:
            ad._notify_gateway_deploy_start(acc, logger)
        except Exception as e:  # noqa: BLE001
            logger.log(f"⚠️ Gateway 预切换异常(忽略): {e}")
        return {
            "job_id": job_id,
            "account": acc,
            "cookies": cookies,
            "reset_message": ad._CLAW_TEMPLATE_RESET_MESSAGE,
            "inject_prompt": ad._bridge_inject_prompt(acc),
        }


def _absorb_worker_log(job: dict, worker_log: str) -> None:
    """Append only the new tail of the worker's cumulative log to the job
    logger (the worker resends its full buffer each sync)."""
    if not worker_log:
        return
    lines = worker_log.splitlines()
    seen = job.get("log_len", 0)
    for line in lines[seen:]:
        job["logger"].lines.append(f"[worker] {line}")
    job["log_len"] = len(lines)


def on_verify(job_id: str, worker_log: str = "") -> tuple[bool, bool]:
    """Worker injected the bridge and is asking whether the account's node has
    dialed back to /ws yet. Returns (job_known, connected)."""
    job = _jobs.get(job_id)
    if not job:
        return False, False
    _absorb_worker_log(job, worker_log)
    account = job["account"]
    try:
        from gateway.ws_tunnel import tunnel
        connected = tunnel.has_account(account)
    except Exception:
        connected = False
    job["status"] = "verifying" if not connected else "node_online"
    _set_active_state(account, "worker:verifying" if not connected else "worker:node_online")
    if connected:
        job["logger"].log(f"✅ bridge 节点已接入 /ws (account={account})")
    return True, connected


def on_report(job_id: str, status: str, worker_log: str = "") -> None:
    """Final report from worker. On success kick the gateway warmup; on failure
    release the failed backend. Persist run history + clear in-flight + record a
    recent-result row so the panel can show success/failure."""
    job = _jobs.pop(job_id, None)
    if not job:
        return
    ad = _ad()
    log = job["logger"]
    _absorb_worker_log(job, worker_log)
    account = job["account"]
    if status == "done":
        try:
            ad._notify_gateway_deploy_done(account, log)
        except Exception as e:  # noqa: BLE001
            log.log(f"⚠️ Gateway 热身触发异常: {e}")
    else:
        try:
            ad._notify_gateway_deploy_failed(account, status, log)
        except Exception:
            pass
    log.log(f"=== 部署结束: {status} ===")
    hist_status = "success" if status == "done" else "error"
    summary = ""
    for ln in reversed(log.lines):
        s = ln.split("] ", 1)[-1].strip()
        if s and not s.startswith("==="):
            summary = s[:160]
            break
    try:
        ad._save_run_history(account, hist_status, log.lines[:])
    except Exception:
        pass
    _record_recent(account, status, job["worker_id"], job_id, summary)
    _set_active_state(account, "done" if status == "done" else "error", finished=True)
    _inflight.pop(account, None)


# ─── live-log bridge into auto_deploy._active_deploys ───

def _register_active(account: str, logger) -> None:
    """Expose a worker job through the same map the local deploy UI reads, so
    /api/auto-deploy/status/<account> streams worker progress live."""
    import threading as _t
    _ad()._active_deploys[account] = {
        "thread": None,
        "logger": logger,
        "state": "worker:starting",
        "cancel": _t.Event(),
        "started_at": datetime.now().isoformat(),
        "started_ts": time.time(),
        "finished_ts": None,
    }


def _set_active_state(account: str, state: str, *, finished: bool = False) -> None:
    entry = _ad()._active_deploys.get(account)
    if not entry:
        return
    entry["state"] = state
    if finished:
        entry["finished_ts"] = time.time()


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
