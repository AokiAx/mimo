"""Claw health/liveness monitor + cross-account relay driver.

This loop no longer chats with the Claw. Each deployed Claw keeps its own
reverse tunnel and api-proxy alive via the cron'd ``tunnel-keepalive.sh``
installed at deploy time (see ``claw.auto_deploy._ssh_bootstrap_instructions``),
so there is nothing to nudge over WS. What still has to happen from the panel
side — and cannot be done by the Claw itself — is:

  * Liveness: detect when a Claw was reclaimed (4h TTL → ``DESTROYED``) and
    redeploy, unless the account already used today's create quota.
  * Health: probe the forwarded proxy (``/v1/models`` through the tunnel). If it
    stays unreachable for a few cycles while the Claw is still AVAILABLE, the
    local keepalive can't fix it (e.g. key deauthorized / box wedged) → redeploy.
  * Relay: free Claws last ~4h; the fleet opens a **new account Claw every 2h**
    so there is always overlap. Gateway drains each backend in its last 30
    minutes (no new requests) until the Claw is reclaimed.
  * Risk control: poll each account's ``bannedStatus`` and quarantine banned
    accounts before they are ever picked for a deploy; release them after 24h
    once recovered.
  * Auto-reg: when create candidates are empty (all live accounts in daily
    cooldown / serving), register fresh free accounts so the 2h open cadence
    is not stuck waiting for Beijing midnight.

Health is judged by a direct GET on the forwarded ``/v1/models`` (proves the
tunnel is up AND the injected upstream key works), never by parsing Claw text.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Cadence ───
_INTERVAL_MIN_S = 120          # 2 min
_INTERVAL_MAX_S = 240          # 4 min
_TICK_S = 20                   # scheduler granularity
# Consecutive unhealthy cycles (Claw still AVAILABLE) before we give up on the
# claw-side keepalive recovering it and rebuild.
_ESCALATE_AFTER_UNHEALTHY = 2

_MIMO_HARD_TTL_S = int(float(os.environ.get("MIMO_CLAW_TTL_S", 4 * 60 * 60)))
# Fixed open cadence: start one new account Claw this often (overlap with 4h TTL).
_OPEN_INTERVAL_S = int(float(os.environ.get("MIMO_CLAW_OPEN_INTERVAL_S", 2 * 60 * 60)))
# Last N seconds of Claw life: gateway puts backend in draining (no new reqs).
_DRAIN_BEFORE_S = int(float(os.environ.get("MIMO_CLAW_DRAIN_BEFORE_S", 30 * 60)))

# Daily-create quota: a MiMo free account may create only ONE Claw per calendar
# day (resets at Beijing midnight, NOT a rolling 24h window). An account that
# created today is held out of the available pool until the date rolls over.
_BEIJING_TZ = timezone(timedelta(hours=8))

# ─── Per-account state ───
_state: dict[str, dict] = {}
_state_lock = threading.Lock()
_loop_running = False
_loop_thread: Optional[threading.Thread] = None
# Cold-start throttle: last time we fired an initial/relay deploy for an account.
_cold_start_last: dict[str, float] = {}
_COLD_START_RETRY_S = 180

# Auto-reg pool top-up for 24h coverage (~12 creates/day at 2h cadence).
_maintain_last: float = 0.0
_maintain_running: bool = False
_MAINTAIN_CHECK_S = int(float(os.environ.get("MIMO_POOL_MAINTAIN_INTERVAL_S", 30 * 60)))
_MAINTAIN_BATCH = int(float(os.environ.get("MIMO_POOL_MAINTAIN_BATCH", 2)))  # max new regs per cycle
# Hard ceiling so available=0 emergency growth cannot register forever.
_POOL_MAX = int(float(os.environ.get("MIMO_CK_POOL_MAX", 24)))
# When open cadence is starved (0 create candidates), allow an urgent top-up
# even if the regular maintain interval has not elapsed.
_URGENT_TOPUP_COOLDOWN_S = int(float(os.environ.get("MIMO_POOL_URGENT_TOPUP_S", 10 * 60)))
_urgent_topup_last: float = 0.0


# Proactive risk-control scan: poll each enabled account's bannedStatus and
# quarantine banned ones before the loop ever picks them for a deploy. Throttled
# per-account so the loop's fast cadence doesn't hammer /user/mi/get.
_risk_check_last: dict[str, float] = {}
_RISK_CHECK_INTERVAL_S = 30 * 60  # re-check each active account at most every 30 min
# Quarantined (risk-pool) accounts are re-checked less often; released back to
# the active pool if their bannedStatus has recovered to NOT_BANNED.
_quarantine_check_last: dict[str, float] = {}
_QUARANTINE_RECHECK_S = 24 * 60 * 60  # re-check each quarantined account every 24h
# 'create_gate' quarantines (account risk gate at create) are re-checked via a
# FREE create probe (returns RISK without creating anything while still flagged),
# so we can re-check far more often than the bannedStatus path.
_CREATE_GATE_RECHECK_S = 2 * 60 * 60  # re-probe create-risk accounts every 2h


def _enabled_deployed_accounts() -> list[str]:
    """Accounts that are auto-deploy-enabled AND already have a registered
    backend (i.e. a Claw was deployed). We never monitor a never-deployed
    account — there is nothing forwarded to probe yet."""
    out: list[str] = []
    try:
        from claw.auto_deploy import load_config
        cfg = load_config()
    except Exception:
        return out
    accounts_cfg = (cfg.get("accounts") or {})
    try:
        from gateway import backend_store
        backends = backend_store.list_backends()
    except Exception:
        backends = []
    have_backend = {str(b.get("account_id") or "").casefold() for b in backends if b.get("base_url")}
    for acc, acc_cfg in accounts_cfg.items():
        if not acc_cfg.get("enabled"):
            continue
        keys = {acc.casefold(), (acc[:-5] if acc.endswith(".json") else acc + ".json").casefold()}
        if keys & have_backend:
            out.append(acc)
    return out


def _account_is_deploying(account: str) -> bool:
    try:
        from claw.auto_deploy import _active_deploys
        d = _active_deploys.get(account)
        return bool(d and d.get("state") not in ("done", "error", "cancelled"))
    except Exception:
        return False


def _verify_health(account: str) -> Optional[bool]:
    """Health = the forwarded proxy answers /v1/models with HTTP 200. We use
    /v1/models rather than /health on purpose: /health only proves the claw-side
    proxy process is alive, NOT that the injected upstream key works — a backend
    can return /health 200 while every real request 401s on a stale key. A 200
    on /v1/models proves both the tunnel is up AND the key is valid, and it costs
    no tokens. Returns None if we can't even resolve the account's target."""
    try:
        from claw.auto_deploy import _resolve_account_target
        target, err = _resolve_account_target(account)
        if target is None:
            return None
    except Exception:
        return None
    import httpx
    url = f"http://{target['upstream_host']}:{target['remote_api_port']}/v1/models"
    try:
        r = httpx.get(url, timeout=8, trust_env=False)
        return r.status_code == 200
    except Exception:
        return False


def _check_claw_alive(cookies: list) -> Optional[str]:
    """Check Claw status via MiMo API. Returns the status string
    ('AVAILABLE', 'DESTROYED', etc.) or None on error."""
    try:
        import importlib
        app_mod = importlib.import_module("app")
        code, data = asyncio.run(app_mod.acurl(
            "GET", "/open-apis/user/mimo-claw/status",
            with_ph=False, cookies=cookies,
        ))
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            return (data.get("data") or {}).get("status", "")
    except Exception:
        pass
    return None


def _check_account_banned(cookies: list) -> Optional[str]:
    """Return the account's ``bannedStatus`` (openclaw 2026.5.27 field on
    /user/mi/get) — e.g. 'NOT_BANNED' or a ban code. None on error / unknown."""
    try:
        import importlib
        app_mod = importlib.import_module("app")
        code, data = asyncio.run(app_mod.acurl(
            "GET", "/open-apis/user/mi/get",
            with_ph=False, cookies=cookies,
        ))
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            return (data.get("data") or {}).get("bannedStatus", "")
    except Exception:
        pass
    return None


def _all_enabled_accounts() -> list[str]:
    """Every auto-deploy-enabled account (with or without a backend)."""
    try:
        from claw.auto_deploy import load_config
        cfg = load_config()
    except Exception:
        return []
    return [a for a, c in (cfg.get("accounts") or {}).items() if c.get("enabled")]


def _account_in_cooldown(account: str) -> bool:
    """True if the account already created a Claw *today* (Beijing calendar day).

    The free create quota is per calendar day (resets at Beijing midnight), so a
    used account must sit out only until the date rolls over — not a full rolling
    24h. The fleet pulls a different available account in the meantime.
    Unknown/never-created accounts are not in cooldown.
    """
    try:
        from claw.auto_deploy import load_config
        cfg = load_config()
    except Exception:
        return False
    accounts = cfg.get("accounts") or {}
    acc = accounts.get(account) or {}
    last = acc.get("last_create_at")
    if not last:
        # also accept the .json-suffixed key form
        alt = account[:-5] if account.endswith(".json") else account + ".json"
        last = (accounts.get(alt) or {}).get("last_create_at")
    if not last:
        return False
    today = datetime.now(_BEIJING_TZ).date()
    created_day = datetime.fromtimestamp(float(last), _BEIJING_TZ).date()
    return created_day >= today


def _quarantined_accounts() -> list[str]:
    """Accounts currently in the RISK pool (risk_blocked tag set)."""
    try:
        from claw.auto_deploy import load_config
        cfg = load_config()
    except Exception:
        return []
    return [a for a, c in (cfg.get("accounts") or {}).items() if c.get("risk_blocked")]


def _active_backend_ages() -> list[float]:
    """Ages (seconds) of enabled active backends, youngest first.

    Prefer official MiMo expireTime-derived age (HARD_TTL - remain_s). Falls
    back to process-local active_for_s when expire_at is unknown.
    """
    try:
        from gateway.runtime import get_all_backends
        backends = get_all_backends()
    except Exception:
        return []
    ages = []
    hard = float(_MIMO_HARD_TTL_S)
    for b in backends:
        if not (b.get("enabled", True) and b.get("lifecycle") == "active"):
            continue
        remain = b.get("remain_s")
        exp = b.get("expire_at")
        if exp and float(exp) > 0 and remain is not None:
            ages.append(max(0.0, hard - float(remain)))
        else:
            ages.append(float(b.get("active_for_s") or 0))
    ages.sort()  # youngest first
    return ages


def _need_open_new_claw() -> bool:
    """True if fleet should cold-start another account now.

    Policy: open every ``_OPEN_INTERVAL_S`` (default 2h). If nothing is active,
    open immediately. If the youngest active backend is already older than the
    open interval, open the next one.
    """
    ages = _active_backend_ages()
    if not ages:
        return True
    youngest = ages[0]
    return youngest >= float(_OPEN_INTERVAL_S)


def _maybe_top_up_account_pool(*, urgent: bool = False) -> None:
    """Background top-up of auto-reg accounts so 24h create cadence never starves.

    Runs at most every ``_MAINTAIN_CHECK_S`` (or ``_URGENT_TOPUP_COOLDOWN_S`` when
    ``urgent=True``). Registers at most ``_MAINTAIN_BATCH`` accounts per cycle.

    Key policy: **available_for_create == 0 is always a starve signal**, even when
    ``auto_live >= target``. Live-but-cooldown accounts cannot open a new Claw
    today; only a freshly registered account has unused daily create quota.
    Growth is capped at ``_POOL_MAX`` so the pool cannot expand without bound.
    """
    global _maintain_last, _maintain_running, _urgent_topup_last
    now = time.time()
    if _maintain_running:
        return
    if urgent:
        if now - _urgent_topup_last < _URGENT_TOPUP_COOLDOWN_S:
            return
    elif now - _maintain_last < _MAINTAIN_CHECK_S:
        return
    _maintain_last = now
    if urgent:
        _urgent_topup_last = now
    try:
        from claw.account_pool import snapshot
        from claw.ck_lifecycle import POOL_TARGET

        snap = snapshot(probe=True, include_archive=False)
        auto_live = len(snap.get("auto_live") or [])
        available = len(snap.get("available_for_create") or [])
        target = int(POOL_TARGET)
        pool_max = max(target, _POOL_MAX)

        # Healthy: have at least one create candidate and inventory is full.
        if available >= 1 and auto_live >= target:
            logger.info(
                "[activity] pool ok auto_live=%d available=%d target=%d",
                auto_live, available, target,
            )
            return

        # Create-starved: every live account is cooldown/serving/risk. Must
        # register new free accounts even if auto_live already looks "full".
        create_starved = available == 0
        need_for_target = max(0, target - auto_live)
        if create_starved:
            if auto_live >= pool_max:
                logger.warning(
                    "[activity] pool: available=0 but auto_live=%d >= pool_max=%d — stop auto-reg",
                    auto_live, pool_max,
                )
                return
            # Prefer filling up to target; if already at/above target still add a
            # small batch so mid-day cooldown can be bypassed with fresh quota.
            room = max(0, pool_max - auto_live)
            batch = max(1, min(_MAINTAIN_BATCH, room if room > 0 else 1))
            logger.warning(
                "[activity] pool create-starved available=0 auto_live=%d target=%d "
                "pool_max=%d urgent=%s → register batch=%d",
                auto_live, target, pool_max, urgent, batch,
            )
        elif need_for_target > 0:
            batch = max(1, min(_MAINTAIN_BATCH, need_for_target))
            logger.warning(
                "[activity] pool low auto_live=%d available=%d target=%d → register batch=%d",
                auto_live, available, target, batch,
            )
        else:
            return
    except Exception:
        logger.exception("[activity] pool check failed")
        return

    def _worker(n: int = batch) -> None:
        global _maintain_running
        _maintain_running = True
        try:
            from claw.ck_lifecycle import try_replace
            results = try_replace(count=n, dry_run=False, enable_deploy=True)
            ok = sum(1 for r in results if r.get("status") == "ok")
            logger.warning("[activity] pool top-up done ok=%d/%d", ok, n)
        except Exception:
            logger.exception("[activity] pool top-up failed")
        finally:
            _maintain_running = False

    threading.Thread(target=_worker, name="pool-topup", daemon=True).start()


def _available_for_reuse() -> list[str]:
    """Accounts that already have a registered backend and can be redeployed
    via reuse (Claw still AVAILABLE on MiMo side). This is the recovery path
    when all claws have expired and there are no available-for-create accounts.

    Only picks accounts whose MiMo Claw is still AVAILABLE — avoids burning a
    daily-create quota on a reuse attempt.
    """
    try:
        from claw.auto_deploy import load_config, _load_account_cookies
        from gateway import backend_store
        from gateway.runtime import get_all_backends
    except Exception:
        return []

    cfg = load_config()
    accounts = cfg.get("accounts") or {}
    # Accounts with a registered backend (any lifecycle)
    registered = {
        str(b.get("account_id") or "")
        for b in backend_store.list_backends()
        if b.get("account_id")
    }
    candidates = []
    for acc in registered:
        acc_cfg = accounts.get(acc) or {}
        if not acc_cfg.get("enabled", True):
            continue
        if acc_cfg.get("risk_blocked"):
            continue
        if _account_is_deploying(acc):
            continue
        candidates.append(acc)

    # Probe MiMo status: only pick accounts whose Claw is AVAILABLE (reuse)
    out = []
    for acc in candidates:
        cookies = _load_account_cookies(acc)
        if cookies is None:
            continue
        try:
            import importlib
            app_mod = importlib.import_module("app")
            code, data = app_mod.curl_api(
                "GET", "/open-apis/user/mimo-claw/status",
                with_ph=False, cookies=cookies,
            )
            status = ""
            if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
                status = (data.get("data") or {}).get("status", "")
            if status == "AVAILABLE":
                out.append(acc)
        except Exception:
            continue
    return out


def _available_for_create() -> list[str]:
    """Accounts eligible to create a Claw right now, fairest-first.

    Single source: ``claw.account_pool.available_for_create`` —
    enabled + ck live + not risk + not today's cooldown + no active backend.
    Filters out dead cookies (401) so the fleet never burns a create on a
    temp-mail account whose SSO session already expired.
    """
    try:
        from claw.account_pool import available_for_create
        candidates = available_for_create(require_auto_reg=True)
    except Exception:
        # fallback without live probe (legacy behaviour)
        try:
            from claw.auto_deploy import load_config
            cfg = load_config()
            from gateway.runtime import get_all_backends
            backends = get_all_backends()
        except Exception:
            return []
        have_backend = {
            str(b.get("account") or b.get("account_id") or "")
            for b in backends
            if b.get("enabled", True) and b.get("healthy") and b.get("lifecycle") == "active"
        }
        out: list[tuple[float, str]] = []
        for acc, c in (cfg.get("accounts") or {}).items():
            if not c.get("enabled") or c.get("risk_blocked"):
                continue
            keys = {acc, acc[:-5] if acc.endswith(".json") else acc + ".json"}
            if keys & have_backend:
                continue
            if _account_is_deploying(acc) or _account_in_cooldown(acc):
                continue
            out.append((float(c.get("last_create_at") or 0.0), acc))
        out.sort(key=lambda t: t[0])
        candidates = [acc for _, acc in out]
    # still skip mid-deploy; also skip accounts that already have ANY registered
    # backend (even failed/inactive). Re-opening those is reuse/redeploy territory
    # and must not be used for the 2h fleet open cadence (which should create a
    # different account for overlap).
    try:
        from gateway import backend_store
        have_any = {
            str(b.get("account_id") or "")
            for b in backend_store.list_backends()
            if b.get("account_id")
        }
        # also accept .json-less / .json forms
        have_any |= {
            (a[:-5] if a.endswith(".json") else a + ".json")
            for a in list(have_any)
        }
    except Exception:
        have_any = set()
    out = []
    for a in candidates:
        if _account_is_deploying(a):
            continue
        keys = {a, a[:-5] if a.endswith(".json") else a + ".json"}
        if keys & have_any:
            continue
        out.append(a)
    return out


def _scan_risk_and_quarantine() -> None:
    """Proactively detect risk-blocked accounts and disable their auto-deploy
    BEFORE the loop selects them, so a banned account never burns a destroy+
    create cycle (which would only worsen the risk gate). Throttled per-account.

    Quarantining flips ``enabled`` to False, so the account drops out of every
    ``_enabled_*`` list computed later in the same loop iteration — protecting
    all trigger_deploy paths at once.
    """
    try:
        from claw.auto_deploy import _load_account_cookies, quarantine_risk_account
    except Exception:
        return
    now = time.time()
    for acc in _all_enabled_accounts():
        if now - _risk_check_last.get(acc, 0.0) < _RISK_CHECK_INTERVAL_S:
            continue
        cookies = _load_account_cookies(acc)
        if cookies is None:
            continue
        banned = _check_account_banned(cookies)
        _risk_check_last[acc] = now
        if banned and banned != "NOT_BANNED":
            logger.warning("[activity] %s: bannedStatus=%s → 风控隔离 (proactive)", acc, banned)
            try:
                quarantine_risk_account(acc)
            except Exception:
                logger.exception("[activity] %s: proactive quarantine failed", acc)


def _scan_quarantine_for_recovery() -> None:
    """Re-check the RISK pool and release accounts whose risk has cleared.

    Two quarantine kinds, two probes:
      * 'create_gate' (account risk gate at create) — the ONLY signal is the
        create call; probing it is FREE while still flagged (returns RISK and
        creates nothing). Re-checked every _CREATE_GATE_RECHECK_S; released once
        the probe no longer returns RISK. bannedStatus is useless here (it stays
        NOT_BANNED while create-risk-gated), which is exactly why this branch
        exists — the old bannedStatus-only recovery would have released these
        immediately and re-hammered them.
      * 'banned' / legacy — bannedStatus on /user/mi/get; released on
        NOT_BANNED, 24h cadence.
    """
    try:
        from claw.auto_deploy import (
            _load_account_cookies, release_risk_account, load_config,
            probe_create_risk, mark_account_created,
        )
    except Exception:
        return
    now = time.time()
    try:
        accounts_cfg = (load_config().get("accounts") or {})
    except Exception:
        accounts_cfg = {}
    for acc in _quarantined_accounts():
        kind = (accounts_cfg.get(acc) or {}).get("risk_kind", "banned")
        cookies = _load_account_cookies(acc)
        if cookies is None:
            continue
        if kind == "create_gate":
            if now - _quarantine_check_last.get(acc, 0.0) < _CREATE_GATE_RECHECK_S:
                continue
            _quarantine_check_last[acc] = now
            verdict = probe_create_risk(cookies)
            if verdict == "RISK":
                continue  # still flagged — free probe, nothing created
            # RATE is also a non-risk signal (short create throttle after a
            # successful create / burst). Treat like CAPACITY/QUOTA for release.
            if verdict in ("OK", "QUOTA", "CAPACITY", "RATE"):
                logger.warning("[activity] %s: create 风控已解除 (probe=%s) → 放回可用池", acc, verdict)
                try:
                    release_risk_account(acc)
                    if verdict == "OK":
                        # the probe actually created a Claw; record the daily
                        # quota so the relay reuses it instead of re-creating.
                        mark_account_created(acc)
                except Exception:
                    logger.exception("[activity] %s: create-gate release failed", acc)
            # ERROR → leave quarantined, retry next window
            continue
        # legacy / banned: bannedStatus recovery
        if now - _quarantine_check_last.get(acc, 0.0) < _QUARANTINE_RECHECK_S:
            continue
        banned = _check_account_banned(cookies)
        _quarantine_check_last[acc] = now
        if banned == "NOT_BANNED":
            logger.warning("[activity] %s: bannedStatus 已恢复 → 放回可用池", acc)
            try:
                release_risk_account(acc)
            except Exception:
                logger.exception("[activity] %s: risk release failed", acc)


def _run_activity_once(account: str) -> None:
    """One monitoring cycle: confirm the Claw still exists, probe the forwarded
    proxy's health, and escalate to a redeploy when it stays unhealthy. No WS
    chat — the Claw keeps its own tunnel/proxy alive via the cron'd keepalive
    installed at deploy time."""
    try:
        from claw.auto_deploy import _load_account_cookies, trigger_deploy
    except Exception as e:  # noqa: BLE001
        logger.exception("[activity] %s: import failed: %s", account, e)
        return

    cookies = _load_account_cookies(account)
    if cookies is None:
        return

    # Liveness: if the Claw is gone (4h TTL reclaim), redeploy — but not the SAME
    # account twice in one Beijing day (no create quota left); the fleet relays
    # to a different available account instead.
    claw_status = _check_claw_alive(cookies)
    if claw_status in ("DESTROYED", "DESTROYING", ""):
        if _account_in_cooldown(account):
            logger.info(
                "[activity] %s: Claw gone but already created today (quota used) — skip redeploy",
                account,
            )
            return
        logger.warning(
            "[activity] %s: Claw status=%s — triggering redeploy",
            account, claw_status or "empty",
        )
        if not _account_is_deploying(account):
            try:
                trigger_deploy(account)
            except Exception:
                logger.exception("[activity] %s: trigger_deploy failed", account)
            with _state_lock:
                _state.setdefault(account, {})["unhealthy_streak"] = 0
        return

    # Claw is AVAILABLE — probe the forwarded proxy directly (panel-side).
    healthy = _verify_health(account)

    with _state_lock:
        st = _state.setdefault(account, {})
        st["last_healthy"] = healthy
        if healthy is True:
            st["unhealthy_streak"] = 0
        elif healthy is False:
            st["unhealthy_streak"] = int(st.get("unhealthy_streak", 0)) + 1
        streak = int(st.get("unhealthy_streak", 0))

    # Persistently unhealthy while the Claw is still AVAILABLE means the local
    # keepalive can't recover it (e.g. key deauthorized / box wedged). A full
    # redeploy needs a create, which is blocked within the daily cooldown — so
    # if we've already created today, ride it out and let the fleet relay to a
    # fresh available account.
    if streak >= _ESCALATE_AFTER_UNHEALTHY and not _account_is_deploying(account):
        if _account_in_cooldown(account):
            logger.info(
                "[activity] %s: unhealthy %d cycles, already created today — no redeploy (relay covers it)",
                account, streak,
            )
        else:
            logger.warning(
                "[activity] %s: unhealthy %d cycles — redeploy",
                account, streak,
            )
            try:
                trigger_deploy(account)
            except Exception:
                logger.exception("[activity] %s: trigger_deploy failed", account)
            with _state_lock:
                _state.setdefault(account, {})["unhealthy_streak"] = 0


def _schedule_next(account: str) -> None:
    with _state_lock:
        _state.setdefault(account, {})["next_due_ts"] = (
            time.time() + random.uniform(_INTERVAL_MIN_S, _INTERVAL_MAX_S)
        )


def _loop() -> None:
    global _loop_running
    _loop_running = True
    print("[claw-activity] 启动健康监控 / relay 循环", flush=True)
    while _loop_running:
        try:
            # Proactive risk scan FIRST: quarantine banned accounts so they fall
            # out of the enabled lists computed just below (no deploy attempted).
            _scan_risk_and_quarantine()
            # And release any quarantined accounts whose ban has cleared (24h).
            _scan_quarantine_for_recovery()
            now = time.time()
            # Keep auto-reg inventory high enough for 2h open cadence all day.
            _maybe_top_up_account_pool()
            # Hourly one-line fleet snapshot (append logs/fleet.log)
            try:
                hr = int(now) // 3600
                if hr != int(getattr(_loop, "_last_fleet_hour", -1)):
                    _loop._last_fleet_hour = hr  # type: ignore[attr-defined]
                    from claw.fleet_report import report
                    report(write_file=True)
            except Exception:
                logger.debug("[activity] fleet_report skipped", exc_info=True)
            accounts = _enabled_deployed_accounts()
            # Drop state for accounts no longer in scope.
            with _state_lock:
                for gone in [a for a in _state if a not in accounts]:
                    _state.pop(gone, None)
            # Fixed-cadence open: every ~2h start one new account Claw so the
            # previous (4h TTL) still has ≥1.5h of accepting traffic left, and
            # gateway drains the old one in its last 30m.
            try:
                from gateway.runtime import reconcile_backend_ages
                reconcile_backend_ages()
            except Exception:
                logger.debug("[activity] reconcile_backend_ages skipped", exc_info=True)

            in_flight = sum(1 for a in _all_enabled_accounts() if _account_is_deploying(a))
            if not in_flight and _need_open_new_claw():
                ages = _active_backend_ages()
                # Primary: create a fresh Claw on a never-deployed account.
                opened = False
                create_candidates = _available_for_create()
                for acc in create_candidates[:1]:
                    if now - _cold_start_last.get(acc, 0.0) < _COLD_START_RETRY_S:
                        continue
                    _cold_start_last[acc] = now
                    logger.warning(
                        "[activity] %s: open new Claw (cadence=%ds, youngest_age=%.0fs, actives=%d)",
                        acc,
                        _OPEN_INTERVAL_S,
                        ages[0] if ages else -1,
                        len(ages),
                    )
                    try:
                        from claw.auto_deploy import trigger_deploy
                        trigger_deploy(acc)
                    except Exception:
                        logger.exception("[activity] %s: open trigger_deploy failed", acc)
                    opened = True
                    break
                # No create candidates: auto-register fresh accounts (they have
                # unused daily create quota). Do this before emergency reuse so
                # fleet recovery is not stuck waiting for Beijing day roll.
                if not opened and not create_candidates:
                    logger.warning(
                        "[activity] open starved: no create candidates "
                        "(actives=%d) → urgent auto-reg",
                        len(ages),
                    )
                    _maybe_top_up_account_pool(urgent=True)
                # Fallback: zero active backends AND no create candidates →
                # try reusing an existing registered account whose MiMo Claw
                # is still AVAILABLE (no create quota burned).
                if not opened and not ages:
                    for acc in _available_for_reuse()[:1]:
                        if now - _cold_start_last.get(acc, 0.0) < _COLD_START_RETRY_S:
                            continue
                        _cold_start_last[acc] = now
                        logger.warning(
                            "[activity] %s: emergency reuse (no active backends, no create candidates)",
                            acc,
                        )
                        try:
                            from claw.auto_deploy import trigger_deploy
                            trigger_deploy(acc)
                        except Exception:
                            logger.exception("[activity] %s: emergency reuse trigger failed", acc)
                        break

            for acc in accounts:
                with _state_lock:
                    st = _state.setdefault(acc, {})
                    due = st.get("next_due_ts")
                    running = st.get("running")
                if running:
                    continue
                if due is None:
                    # Stagger first runs across the cadence window.
                    _schedule_next(acc)
                    continue
                if now < due:
                    continue
                if _account_is_deploying(acc):
                    _schedule_next(acc)
                    continue

                def _worker(a: str = acc) -> None:
                    with _state_lock:
                        _state.setdefault(a, {})["running"] = True
                    try:
                        _run_activity_once(a)
                    except Exception:
                        logger.exception("[activity] worker crashed for %s", a)
                    finally:
                        with _state_lock:
                            _state.setdefault(a, {})["running"] = False
                        _schedule_next(a)

                threading.Thread(target=_worker, daemon=True).start()
        except Exception as e:  # noqa: BLE001
            print(f"[claw-activity] 错误: {e}", flush=True)
        time.sleep(_TICK_S)


def start_activity():
    global _loop_thread
    if _loop_thread and _loop_thread.is_alive():
        return
    _loop_thread = threading.Thread(target=_loop, daemon=True)
    _loop_thread.start()


def stop_activity():
    global _loop_running
    _loop_running = False
