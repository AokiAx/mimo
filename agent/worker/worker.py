"""MiMo claw-worker — runs on a mainland-China host (NAS/Docker).

Responsibility (and ONLY this):
  1. create claw
  2. talk to claw over WS (send deploy text, get its SSH pubkey, notify it)
  3. talk to the control panel (pull jobs, push results)

It holds NO local state: cookies + deploy text + which account come from the
panel per-job. Everything panel-side (add key to jump server, ssh into claw,
finalize, verify) stays on the panel. The worker only does the region-gated
MiMo parts that must originate from a mainland IP.

NAT-friendly: the worker only makes OUTBOUND HTTP to the panel (poll + report).

Config via env:
  PANEL_URL        panel base url, e.g. https://panel.example   (required)
  WORKER_TOKEN     per-worker token issued by the panel         (required)
  WORKER_NAME      display name on the panel (default: hostname)
  POLL_INTERVAL    idle poll seconds (default 60)
  CLAW_CREATE_BUDGET  seconds to keep retrying claw create (default 600)
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

VERSION = "1.0.0"
PROTOCOL_VERSION = 1

PANEL_URL = (os.environ.get("PANEL_URL") or "").rstrip("/")
TOKEN = os.environ.get("WORKER_TOKEN") or ""
NAME = os.environ.get("WORKER_NAME") or socket.gethostname()
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
CREATE_BUDGET = int(os.environ.get("CLAW_CREATE_BUDGET", "600"))
VERIFY_TLS = os.environ.get("VERIFY_TLS", "1") != "0"
SYNC_URL = f"{PANEL_URL}/api/claw-worker/sync"
MAX_BOOTSTRAP_ATTEMPTS = 3


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
    deploy_text = job["deploy_text"]
    notify_text = job.get("notify_text", "密钥已添加完成，请执行后续连接操作。")
    buf: list[str] = []

    def jlog(msg):
        log(f"[{account}] {msg}")
        buf.append(msg)

    async def report(status):
        try:
            await sync(client, "report", job_id=jid, status=status, log="\n".join(buf))
        except Exception as e:
            log(f"report failed: {e}")

    # Step 1: create claw (mainland IP).
    jlog("creating claw ...")
    if not await mc.create_claw(cookies, budget_s=CREATE_BUDGET, log=jlog):
        jlog("create claw failed/timeout")
        return await report("error")

    # Step 2-3: send deploy text, extract ECS pubkey. Retry in a fresh session.
    public_key = None
    for attempt in range(1, MAX_BOOTSTRAP_ATTEMPTS + 1):
        session_key = f"agent:main:worker-{account}-{uuid.uuid4().hex[:8]}"
        jlog(f"sending deploy text (attempt {attempt}/{MAX_BOOTSTRAP_ATTEMPTS})")
        reply, err = await mc.claw_chat(cookies, deploy_text, session_key)
        if err:
            jlog(f"claw chat error: {err}")
            await asyncio.sleep(3 * attempt)
            continue
        if mc.is_region_blocked(reply):
            jlog("⚠️ region-blocked reply — worker egress is NOT mainland?")
            return await report("error_region")
        public_key = mc.parse_ssh_key(reply)
        if public_key:
            jlog(f"got ECS pubkey: {public_key[:40]}...")
            break
        jlog("reply had no SSH key, retrying in a new session")
        await asyncio.sleep(3 * attempt)
    if not public_key:
        jlog("failed to extract ECS pubkey")
        return await report("error")

    # Handoff to panel: add key to jump server + clean ports.
    jlog("→ panel: add key")
    resp = await sync(client, "claw_ready", job_id=jid, public_key=public_key, log="\n".join(buf))
    if not resp.get("ok"):
        jlog(f"panel add-key failed: {resp.get('error')}")
        return await report("error")

    # Notify claw to (re)establish the reverse tunnel now the key is in place.
    for attempt in range(1, 4):
        reply, err = await mc.claw_chat(cookies, notify_text, session_key)
        if not err:
            jlog(f"claw notified: {str(reply)[:80]}")
            break
        jlog(f"notify attempt {attempt}/3 failed: {err}")
        await asyncio.sleep(5)

    # Handoff to panel: ssh into claw via tunnel + finalize + verify.
    jlog("→ panel: finalize")
    resp = await sync(client, "notified", job_id=jid, log="\n".join(buf))
    if resp.get("ok"):
        jlog(f"✅ deploy done: {str(resp.get('detail',''))[:120]}")
        return await report("done")
    jlog(f"❌ finalize failed: {resp.get('error')}")
    return await report("error")


async def main() -> int:
    if not PANEL_URL or not TOKEN:
        log("ERROR: PANEL_URL and WORKER_TOKEN are required")
        return 2
    log(f"claw-worker {VERSION} → {SYNC_URL} (poll {POLL_INTERVAL}s, proxy={mc.PROXY})")
    async with httpx.AsyncClient(timeout=60, verify=VERIFY_TLS, trust_env=False) as client:
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
