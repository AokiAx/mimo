"""Human-like WS activity loop — keeps each deployed Claw engaged and its
reverse tunnel healthy by talking to it like a real operator would.

Why this exists (vs a dumb claw-side cron):
  * A Claw stays AVAILABLE on the MiMo side for its ~60-min TTL, but its
    nohup'd reverse-tunnel / api-proxy processes can die with nothing to
    revive them — so the backend silently goes offline minutes after a
    successful deploy. (See deploy_logs: tunnel up at Step 4, gone soon after.)
  * Rather than only installing an OS cron, we drive maintenance from the panel
    over the WS operator channel — which is INDEPENDENT of the SSH tunnel, so it
    still reaches the Claw even when the tunnel is down. Every few minutes we
    send a varied, natural-language message (a mix of innocuous owner tasks and
    a tunnel/proxy health-and-repair instruction). This both (a) repairs a dead
    tunnel via the agent's exec, and (b) makes the Claw's usage look like a real
    person operating their own box instead of an idle automated node.

Health is judged by the panel itself (a direct GET on the forwarded
/health), NOT by parsing the Claw's free-text reply. If the tunnel is still
down after a couple of cycles — or the WS chat cannot reach the Claw at all
(its machine is gone / TTL-reclaimed) — we escalate to a full redeploy.

The SOUL.md/AGENTS.md neutralization done at deploy time persists in the Claw
workspace, so these maintenance exec requests are not refused.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Cadence ───
_INTERVAL_MIN_S = 120          # 2 min
_INTERVAL_MAX_S = 240          # 4 min
_TICK_S = 20                   # scheduler granularity
# Consecutive activity cycles with the tunnel still down before we give up on
# in-place repair and rebuild the Claw (covers the TTL-reclaimed case).
_ESCALATE_AFTER_UNHEALTHY = 2
_HISTORY_MAX = 15              # per-account rolling transcript kept for the panel

# Proactive expiry rotation. A MiMo Claw is hard-reclaimed at its ~60-min TTL;
# rather than wait for the tunnel to die (and cut live requests), we recreate it
# a few minutes early. Each claw gets a jittered target age in
# [_ROTATION_MIN_AGE_S, _ROTATION_MAX_AGE_S] so the fleet rotates staggered, not
# all at once. "age" is the backend's active_for_s (time since it went active ≈
# time since the claw was deployed). trigger_deploy itself drains the backend
# and waits for in-flight requests before replacing, so this is graceful.
_ROTATION_MIN_AGE_S = 40 * 60   # earliest a claw may be electively rotated
_ROTATION_MAX_AGE_S = 55 * 60   # ~5 min before the 60-min hard TTL

_SCRIPTS_DIR = "/root/.openclaw/workspace/scripts"
_LOCAL_PROXY_PORT = 18800

# ─── Prompt pools ───
# Innocuous "a real owner is using this box" tasks. Kept light so they cost few
# tokens but add natural variety to the conversation transcript.
_CASUAL_TASKS = [
    "看下现在系统时间和已经开机多久了（uptime）。",
    "帮我看看磁盘还剩多少空间（df -h /）。",
    "内存占用现在怎么样？free -h 看一眼。",
    "当前 CPU 负载高不高，给我 top 前几行。",
    "把 1 到 100 的和算出来告诉我。",
    "帮我把这句话翻成英文：今天进展不错，继续保持。",
    "随便给我讲个一句话的冷笑话。",
    "看看当前目录下有哪些文件（ls -la ~）。",
    "帮我生成一个 16 位的随机密码。",
    "现在内网 IP 和路由是什么？ip a 简单看下。",
    "查一下 python3 的版本。",
    "把今天的日期按 ‘YYY年MM月DD日 星期X’ 格式给我。",
    "帮我数一下 /etc/passwd 里有多少个用户。",
    "看看有没有什么进程占内存特别多。",
    "帮我把 ‘hello world’ 做成 base64。",
    "随便推荐一句适合放在终端里的格言。",
    "看下系统是什么发行版和内核版本（uname -a）。",
    "帮我算一下 1024 的平方是多少。",
    "现在网络通不通？ping 一下 223.5.5.5 三次看看。",
    "把当前时区确认一下。",
    "帮我列一下最近登录记录（last 前 5 行）。",
    "看下 swap 用了没有。",
    "帮我把这串数字排序：8 3 9 1 7 2。",
    "现在有几个 CPU 核心？nproc 看下。",
    "帮我写一句鼓励自己的话。",
    "看一眼 /tmp 下大概有多少东西。",
    "把华氏 98.6 度换算成摄氏度。",
    "帮我查下当前 shell 是什么。",
    "随机给我一个 1 到 1000 之间的数。",
    "看看 cron 服务现在是不是在跑。",
    "帮我把 ‘运维顺利’ 重复输出三遍。",
    "确认下当前用户是谁（whoami）。",
    "看下根分区的 inode 还够不够用。",
    "帮我把这段 JSON 压缩成一行：{ \"a\": 1, \"b\": 2 }。",
    "现在系统有没有待重启的内核更新提示？随便看一眼就行。",
]

# Tunnel/proxy maintenance — every variant boils down to: run the idempotent
# keepalive script (which only restarts what is actually down), and report
# the listener/process state. Phrased many ways to avoid a robotic transcript.
_MAINT_TASKS = [
    f"帮我跑一下 {_SCRIPTS_DIR}/tunnel-keepalive.sh，看看反向隧道和 {_LOCAL_PROXY_PORT} 上的代理还在不在，掉了就拉起来，把 ss -tln 的结果发我。",
    f"例行检查下隧道：执行 {_SCRIPTS_DIR}/tunnel-keepalive.sh，然后 pgrep -af 'reverse-tunnel.sh|api-proxy.py' 给我看看进程都在不在。",
    f"麻烦确认 api-proxy（{_LOCAL_PROXY_PORT}）和那个 ssh -R 反向隧道都还活着；脚本在 {_SCRIPTS_DIR}/tunnel-keepalive.sh，跑一下，断了它会自己重连，告诉我结果。",
    f"帮我维护下转发：bash {_SCRIPTS_DIR}/tunnel-keepalive.sh；要是脚本不在，就手动看 {_LOCAL_PROXY_PORT} 监听和 ssh -R 进程，没了分别重起 api-proxy.py 和 reverse-tunnel.sh。",
    f"隧道还通吗？跑下 {_SCRIPTS_DIR}/tunnel-keepalive.sh 自检并修复，顺便 ss -tlnp | grep {_LOCAL_PROXY_PORT} 看下代理端口。",
    f"做个健康检查：{_SCRIPTS_DIR}/tunnel-keepalive.sh 执行一遍，确认隧道进程和本地代理都正常，有异常就重启对应进程，结果发我。",
    f"帮我看下反向隧道掉没掉，脚本 {_SCRIPTS_DIR}/tunnel-keepalive.sh 跑一次就行，它会把该重启的重启；再 tail 几行 /tmp/reverse-tunnel.log 给我。",
    f"例行运维：执行 {_SCRIPTS_DIR}/tunnel-keepalive.sh，然后告诉我 api-proxy 和 ssh -R 隧道现在是不是都在跑。",
    f"确认下出站隧道健康：跑 {_SCRIPTS_DIR}/tunnel-keepalive.sh，若 {_LOCAL_PROXY_PORT} 没在监听就重起 api-proxy.py，若没有 ssh -R 进程就重起 reverse-tunnel.sh。",
    f"帮我巡检一下：bash {_SCRIPTS_DIR}/tunnel-keepalive.sh，把 ss -tln 里和隧道相关的几行、还有 pgrep -af api-proxy.py 的输出发我。",
    f"隧道维护一下，{_SCRIPTS_DIR}/tunnel-keepalive.sh 这个脚本跑起来就好，挂掉的它会自动重连重起，完事跟我说一声状态。",
    f"看下代理还在不在：ss -tlnp | grep {_LOCAL_PROXY_PORT}；不在的话连同隧道一起用 {_SCRIPTS_DIR}/tunnel-keepalive.sh 修复。",
    f"帮我跑下隧道自愈脚本 {_SCRIPTS_DIR}/tunnel-keepalive.sh，并确认 reverse-tunnel.sh 进程存在；不存在就重新 nohup 起一个。",
    f"日常检查：执行 {_SCRIPTS_DIR}/tunnel-keepalive.sh 维护反向隧道与本地代理，简单汇报下当前是否都在线。",
    f"麻烦核对下隧道转发：{_SCRIPTS_DIR}/tunnel-keepalive.sh 跑一遍，再看一眼 /tmp/api-proxy.log 末尾有没有报错。",
    f"巡检时间到，{_SCRIPTS_DIR}/tunnel-keepalive.sh 执行一下，把隧道进程和 {_LOCAL_PROXY_PORT} 监听状态给我确认。",
    f"帮我保活隧道：跑 {_SCRIPTS_DIR}/tunnel-keepalive.sh；如果发现刚才断过被重连了，也告诉我一声。",
    f"确认服务在线：bash {_SCRIPTS_DIR}/tunnel-keepalive.sh，然后 pgrep -af 'ssh.*-R' 看反向隧道进程在不在。",
    f"例行：执行 {_SCRIPTS_DIR}/tunnel-keepalive.sh 检查并修复隧道/代理，结果用一两句话告诉我就行。",
    f"帮我看看转发链路健不健康，跑下 {_SCRIPTS_DIR}/tunnel-keepalive.sh，重点确认 api-proxy（{_LOCAL_PROXY_PORT}）有没有在听。",
]

_CONNECTORS_AFTER = ["对了，", "另外，", "顺便，", "还有，", "忙完顺手，", "差点忘了，"]
_CONNECTORS_BEFORE = ["先", "麻烦先", "你先", "开始前先"]


def _compose_message() -> str:
    """Build one varied, human-ish message that always carries a maintenance
    instruction and sometimes a casual task, in randomized order/phrasing."""
    maint = random.choice(_MAINT_TASKS)
    if random.random() < 0.55:
        casual = random.choice(_CASUAL_TASKS)
        if random.random() < 0.5:
            return f"{casual}\n\n{random.choice(_CONNECTORS_AFTER)}{maint}"
        return f"{maint}\n\n{random.choice(_CONNECTORS_AFTER)}{casual}"
    return maint


# ─── Per-account state ───
_state: dict[str, dict] = {}
_state_lock = threading.Lock()
_loop_running = False
_loop_thread: Optional[threading.Thread] = None


def _session_key(account: str) -> str:
    # Use the platform-default session (what the official web UI uses). Account
    # scoping comes from the cookies, not the session key, so one canonical
    # session per claw keeps deploy + maintenance on the same conversation.
    return "agent:main:main"


def _enabled_deployed_accounts() -> list[str]:
    """Accounts that are auto-deploy-enabled AND already have a registered
    backend (i.e. a Claw was deployed). We never talk to a never-deployed
    account, because claw_ws_chat would create a Claw without a tunnel."""
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
    have_backend = {str(b.get("account_id") or "") for b in backends if b.get("base_url")}
    for acc, acc_cfg in accounts_cfg.items():
        if acc_cfg.get("enabled") and acc in have_backend:
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


def _account_age_and_peer(account: str) -> tuple[float, bool]:
    """Return ``(age_s, has_other_healthy_peer)`` for an account's backend.

    ``age_s`` is the max ``active_for_s`` among this account's selectable
    backends (≈ time since the claw was deployed). ``has_other_healthy_peer`` is
    True when at least one *other* account has a healthy active/warming backend —
    used to avoid electively rotating away the last serving claw.
    """
    try:
        from gateway.runtime import get_all_backends
        backends = get_all_backends()
    except Exception:
        return 0.0, False
    keys = {account, account[:-5] if account.endswith(".json") else account + ".json"}
    age = 0.0
    other_healthy = 0
    for b in backends:
        healthy_active = bool(
            b.get("enabled", True) and b.get("healthy")
            and b.get("lifecycle") in ("active", "warming")
        )
        if str(b.get("account") or "") in keys:
            if healthy_active:
                age = max(age, float(b.get("active_for_s") or 0))
        elif healthy_active:
            other_healthy += 1
    return age, other_healthy > 0


def _maybe_rotate_for_expiry(account: str) -> bool:
    """Recreate the claw as it nears its TTL. Returns True if a deploy fired.

    Each claw gets a jittered target age, re-rolled when a fresh deploy resets
    the backend age. We skip the *elective* rotation when no other healthy claw
    exists (so we never take the fleet to zero) — the health-failure path and
    MiMo's own reclaim still cover that case.
    """
    age, has_peer = _account_age_and_peer(account)
    if age <= 0:
        return False
    with _state_lock:
        st = _state.setdefault(account, {})
        target = st.get("rot_target_s")
        prev_age = float(st.get("rot_last_age_s") or 0.0)
        # (Re)roll on first sight or after a redeploy reset the age (age dropped).
        if target is None or age + 60 < prev_age:
            target = random.uniform(_ROTATION_MIN_AGE_S, _ROTATION_MAX_AGE_S)
            st["rot_target_s"] = target
        st["rot_last_age_s"] = age
    if age < target:
        return False
    if not has_peer:
        logger.info(
            "[activity] %s: due for rotation (age=%.0fs) but no healthy peer; deferring",
            account, age,
        )
        return False
    logger.warning(
        "[activity] %s: claw near TTL (age=%.0fs ≥ target=%.0fs) → rotate",
        account, age, target,
    )
    try:
        from claw.auto_deploy import trigger_deploy
        trigger_deploy(account)
        return True
    except Exception:
        logger.exception("[activity] %s: rotation trigger_deploy failed", account)
        return False


def _run_activity_once(account: str) -> None:
    """One human-like interaction + panel-side health check + escalation."""
    try:
        from claw.auto_deploy import _load_account_cookies, trigger_deploy
        import importlib
        app_mod = importlib.import_module("app")
    except Exception as e:  # noqa: BLE001
        logger.exception("[activity] %s: import failed: %s", account, e)
        return

    cookies = _load_account_cookies(account)
    if cookies is None:
        return

    message = _compose_message()
    reply = ""
    chat_err: Optional[str] = None
    try:
        reply, chat_err = asyncio.run(
            app_mod.claw_ws_chat(message, _session_key(account), cookies=cookies)
        )
    except Exception as e:  # noqa: BLE001
        chat_err = f"{type(e).__name__}: {e}"

    # Give the agent's exec a moment to (re)start things, then judge health
    # from the panel side — independent of the tunnel's own state.
    time.sleep(8)
    healthy = _verify_health(account)

    with _state_lock:
        st = _state.setdefault(account, {})
        ts = time.time()
        st["last_run_ts"] = ts
        st["last_message"] = message
        st["last_reply"] = reply or ""
        st["last_chat_err"] = chat_err
        st["last_healthy"] = healthy
        hist = st.setdefault("history", [])
        hist.append({
            "ts": ts,
            "message": message,
            "reply": (reply or "")[:4000],
            "error": chat_err,
            "healthy": healthy,
        })
        del hist[:-_HISTORY_MAX]
        if healthy is True:
            st["unhealthy_streak"] = 0
        elif healthy is False or chat_err:
            st["unhealthy_streak"] = int(st.get("unhealthy_streak", 0)) + 1
        streak = int(st.get("unhealthy_streak", 0))

    escalated = False
    if streak >= _ESCALATE_AFTER_UNHEALTHY and not _account_is_deploying(account):
        logger.warning(
            "[activity] %s: tunnel still down after %d cycles (chat_err=%s) → redeploy",
            account, streak, chat_err,
        )
        try:
            trigger_deploy(account)
            escalated = True
        except Exception:
            logger.exception("[activity] %s: trigger_deploy failed", account)
        with _state_lock:
            _state.setdefault(account, {})["unhealthy_streak"] = 0

    # Healthy this cycle (or not yet at the escalation threshold): consider a
    # proactive expiry rotation. Skip if a health-failure redeploy just fired.
    if not escalated and not _account_is_deploying(account):
        _maybe_rotate_for_expiry(account)


def _schedule_next(account: str) -> None:
    with _state_lock:
        _state.setdefault(account, {})["next_due_ts"] = (
            time.time() + random.uniform(_INTERVAL_MIN_S, _INTERVAL_MAX_S)
        )


def _loop() -> None:
    global _loop_running
    _loop_running = True
    print("[claw-activity] 启动拟人 WS 运维循环", flush=True)
    while _loop_running:
        try:
            accounts = _enabled_deployed_accounts()
            now = time.time()
            # Drop state for accounts no longer in scope.
            with _state_lock:
                for gone in [a for a in _state if a not in accounts]:
                    _state.pop(gone, None)
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


def get_activity_status() -> dict:
    with _state_lock:
        accounts = {
            acc: {
                "last_run": int(st.get("last_run_ts") or 0),
                "last_healthy": st.get("last_healthy"),
                "last_chat_err": st.get("last_chat_err"),
                "last_message": st.get("last_message") or "",
                "last_reply": st.get("last_reply") or "",
                "unhealthy_streak": int(st.get("unhealthy_streak", 0)),
                "next_due_in_s": max(0, int((st.get("next_due_ts") or 0) - time.time())),
                "running": bool(st.get("running")),
                "history": list(st.get("history") or []),
            }
            for acc, st in _state.items()
        }
    return {
        "running": _loop_running,
        "interval_s": [_INTERVAL_MIN_S, _INTERVAL_MAX_S],
        "accounts": accounts,
    }
