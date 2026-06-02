"""MiMo claw-worker — runs on a mainland-China host (NAS/Docker).

Responsibility (and ONLY this):
  1. (re)create claw
  2. talk to claw over WS: template-reset, then inject the ws-bridge so it dials
     back to the panel's /ws (the region-gated MiMo chat MUST come from a
     mainland IP — that's the whole reason this worker exists)
  3. talk to the control panel (long-poll for jobs, stream logs, report result)

It holds NO local state: cookies + the rendered bridge inject prompt come from
the panel per-job. Verification (is the bridge node online?) and warmup happen
panel-side. No jump server, no SSH, no api-proxy.

NAT-friendly: the worker only makes OUTBOUND HTTP to the panel. The poll is a
long-poll, so a panel-side manual trigger reaches the worker near-instantly.

Config via env:
  PANEL_URL        panel base url, e.g. https://panel.example   (required)
  WORKER_TOKEN     per-worker token issued by the panel         (required)
  WORKER_NAME      display name on the panel (default: hostname)
  POLL_INTERVAL    seconds to wait after an idle/empty poll (default 5)
  CLAW_CREATE_BUDGET  seconds to keep retrying claw create (default 600)
  BRIDGE_VERIFY_BUDGET seconds to wait for the bridge node to connect (default 240)
  MIMO_PROXY       optional mainland egress proxy for MiMo (see mimo_client)
  VERIFY_TLS       "0" to disable panel TLS verify (default verify)
"""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import time
import uuid

import httpx

import mimo_client as mc

VERSION = "2.0.0"
PROTOCOL_VERSION = 2

PANEL_URL = (os.environ.get("PANEL_URL") or "").rstrip("/")
TOKEN = os.environ.get("WORKER_TOKEN") or ""
NAME = os.environ.get("WORKER_NAME") or socket.gethostname()
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
CREATE_BUDGET = int(os.environ.get("CLAW_CREATE_BUDGET", "600"))
VERIFY_BUDGET = int(os.environ.get("BRIDGE_VERIFY_BUDGET", "240"))
VERIFY_TLS = os.environ.get("VERIFY_TLS", "1") != "0"
SYNC_URL = f"{PANEL_URL}/api/claw-worker/sync"
MAX_INJECT_ATTEMPTS = 3
VERIFY_INTERVAL = 6
# Must exceed the panel's long-poll hold window (default 25s).
HTTP_TIMEOUT = 60


def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)


async def sync(client: httpx.AsyncClient, phase: str, **fields) -> dict:
    """One call to the panel. Always rides the worker heartbeat (meta)."""
    body = {
        "protocol_version": PROTOCOL_VERSION,
        "phase": phase,
        "worker": {"name": NAME, "version": VERSION, "egress": await mc.egress_info()},
        **fields,
    }
    r = await client.post(SYNC_URL, json=body, headers={"X-Worker-Token": TOKEN})
    r.raise_for_status()
    return r.json()


async def run_job(client: httpx.AsyncClient, job: dict) -> None:
    jid = job["job_id"]
    account = job.get("account", "?")
    cookies = job["cookies"]
    reset_message = job.get("reset_message") or ""
    inject_prompt = job["inject_prompt"]
    buf: list[str] = []

    def jlog(msg):
        log(f"[{account}] {msg}")
        buf.append(msg)

    async def report(status):
        try:
            await sync(client, "report", job_id=jid, status=status, log="\n".join(buf))
        except Exception as e:
            log(f"report failed: {e}")

    # Step 0: destroy any existing claw so its stale bridge doesn't keep
    # registering the same account alongside the fresh one.
    try:
        st = await mc.claw_status(cookies)
    except Exception:
        st = None
    if st and st not in ("DESTROYED", "DESTROYING", "", None):
        jlog(f"destroying old claw (status={st}) ...")
        try:
            await mc.destroy_claw(cookies)
        except Exception as e:
            jlog(f"destroy ignored: {e}")

    # Step 1: create claw (mainland IP).
    jlog("creating claw ...")
    if not await mc.create_claw(cookies, budget_s=CREATE_BUDGET, log=jlog):
        jlog("create claw failed/timeout")
        return await report("error")

    # Step 2: template reset + restart.
    if reset_message:
        jlog("template reset ...")
        reply, err = await mc.claw_chat(
            cookies, reset_message, f"agent:main:reset-{account}-{uuid.uuid4().hex[:8]}")
        if err:
            jlog(f"reset chat error (continuing): {err}")
        elif mc.is_region_blocked(reply):
            jlog("⚠️ region-blocked reply — worker egress is NOT mainland?")
            return await report("error_region")
        else:
            jlog(f"reset reply: {str(reply)[:80]}")

    # Step 3: inject the ws-bridge (dials back to the panel's /ws).
    injected = False
    for attempt in range(1, MAX_INJECT_ATTEMPTS + 1):
        session_key = f"agent:main:wsbridge-{account}-{uuid.uuid4().hex[:8]}"
        jlog(f"injecting ws-bridge (attempt {attempt}/{MAX_INJECT_ATTEMPTS})")
        reply, err = await mc.claw_chat(cookies, inject_prompt, session_key)
        if err:
            jlog(f"inject chat error: {err}")
            await asyncio.sleep(3 * attempt)
            continue
        if mc.is_region_blocked(reply):
            jlog("⚠️ region-blocked reply — worker egress is NOT mainland?")
            return await report("error_region")
        jlog(f"claw reply: {str(reply)[:100]}")
        injected = True
        break
    if not injected:
        jlog("inject failed after retries")
        return await report("error")

    # Step 4: ask the panel whether the account's bridge node has dialed back.
    jlog("waiting for bridge node to connect to /ws ...")
    deadline = time.time() + VERIFY_BUDGET
    connected = False
    while time.time() < deadline:
        try:
            resp = await sync(client, "verify", job_id=jid, log="\n".join(buf))
        except Exception as e:
            jlog(f"verify sync error: {e}")
            await asyncio.sleep(VERIFY_INTERVAL)
            continue
        if not resp.get("ok"):
            jlog(f"verify rejected: {resp.get('error')}")
            break
        if resp.get("connected"):
            connected = True
            jlog("✅ bridge node online")
            break
        await asyncio.sleep(VERIFY_INTERVAL)
    if not connected:
        jlog("bridge node did not connect within budget")
        return await report("error")

    jlog("✅ deploy done (bridge online; gateway warmup validates the model link)")
    return await report("done")


async def main() -> int:
    if not PANEL_URL or not TOKEN:
        log("ERROR: PANEL_URL and WORKER_TOKEN are required")
        return 2
    log(f"claw-worker {VERSION} → {SYNC_URL} (long-poll, proxy={mc.PROXY})")
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, verify=VERIFY_TLS, trust_env=False) as client:
        while True:
            try:
                resp = await sync(client, "poll")
                if resp.get("action") == "deploy" and resp.get("job"):
                    await run_job(client, resp["job"])
                    continue  # immediately poll again in case more are queued
            except httpx.HTTPError as e:
                log(f"panel sync error: {type(e).__name__}: {str(e)[:120]}")
            except Exception as e:
                log(f"unexpected: {type(e).__name__}: {str(e)[:160]}")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
