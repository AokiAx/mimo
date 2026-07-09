#!/usr/bin/env python3
"""Write a compact fleet daily snapshot to logs/fleet.log (and stdout)."""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "logs" / "fleet.log"
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "claw") not in sys.path:
    sys.path.insert(0, str(ROOT / "claw"))


def report(*, write_file: bool = True) -> dict:
    from claw.account_pool import snapshot

    snap = snapshot(probe=True, include_archive=False)
    counts = snap.get("counts") or {}
    line = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "epoch": int(time.time()),
        "available": counts.get("available", 0),
        "serving": counts.get("serving", 0),
        "cooldown": counts.get("cooldown", 0),
        "risk": counts.get("risk", 0),
        "dead": counts.get("dead", 0),
        "disabled": counts.get("disabled", 0),
        "auto_live": len(snap.get("auto_live") or []),
        "available_emails": snap.get("available_for_create") or [],
        "serving_emails": snap.get("serving") or [],
    }
    try:
        from gateway.runtime import get_router_status

        st = get_router_status()
        line["gw_active"] = st.get("backends_active")
        line["gw_healthy"] = st.get("backends_healthy")
        line["gw_qps"] = st.get("qps")
    except Exception:
        pass

    text = (
        f"{line['ts']} fleet "
        f"avail={line['available']} serve={line['serving']} cool={line['cooldown']} "
        f"risk={line['risk']} dead={line['dead']} auto_live={line['auto_live']}"
    )
    if write_file:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("a", encoding="utf-8") as f:
            f.write(text + "\n")
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    print(text)
    return line


if __name__ == "__main__":
    report()
