#!/usr/bin/env python3
"""MiMo VPS probe agent — single-file, stdlib-only.

Usage:
    python3 agent.py --url https://panel.example/api/probe/report \\
                     --token <token> --name <display-name> [--interval 10]

Or via env: PROBE_URL, PROBE_TOKEN, PROBE_NAME, PROBE_INTERVAL.

Reads /proc/* + `df` and POSTs JSON every interval seconds. Linux only.
"""
import argparse
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request


def read_cpu():
    """Return (idle, total) jiffies from /proc/stat first cpu line."""
    with open("/proc/stat") as f:
        line = f.readline()
    parts = line.split()
    nums = [int(x) for x in parts[1:]]
    idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
    total = sum(nums)
    return idle, total


def read_mem():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, _, rest = line.partition(":")
            info[k.strip()] = int(rest.strip().split()[0])  # kB
    total_kb = info.get("MemTotal", 0)
    avail_kb = info.get("MemAvailable", info.get("MemFree", 0))
    used_kb = total_kb - avail_kb
    pct = (used_kb / total_kb * 100) if total_kb else 0
    return {
        "total_mb": round(total_kb / 1024, 1),
        "used_mb": round(used_kb / 1024, 1),
        "percent": round(pct, 1),
    }


def read_disk(path="/"):
    try:
        st = shutil.disk_usage(path)
        pct = st.used / st.total * 100 if st.total else 0
        return {
            "total_gb": round(st.total / (1024 ** 3), 2),
            "used_gb": round(st.used / (1024 ** 3), 2),
            "percent": round(pct, 1),
        }
    except OSError:
        return {"total_gb": 0, "used_gb": 0, "percent": 0}


def read_net():
    """Sum rx/tx bytes across non-loopback interfaces."""
    rx = tx = 0
    try:
        with open("/proc/net/dev") as f:
            for line in f.readlines()[2:]:
                name, _, rest = line.partition(":")
                name = name.strip()
                if name == "lo" or not rest:
                    continue
                cols = rest.split()
                rx += int(cols[0])
                tx += int(cols[8])
    except OSError:
        pass
    return rx, tx


def read_load():
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().split()
        return [float(parts[0]), float(parts[1]), float(parts[2])]
    except (OSError, ValueError):
        return [0.0, 0.0, 0.0]


def read_uptime():
    try:
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])
    except (OSError, ValueError):
        return 0.0


def read_os_info():
    info = {"hostname": socket.gethostname(), "kernel": "", "distro": ""}
    try:
        info["kernel"] = os.uname().release
    except OSError:
        pass
    try:
        with open("/etc/os-release") as f:
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    info["distro"] = line.split("=", 1)[1].strip().strip('"')
                    break
    except OSError:
        pass
    return info


def collect_sample(prev):
    """Return (sample dict, new_prev) for delta calculations."""
    now = time.time()
    idle, total = read_cpu()
    rx, tx = read_net()

    if prev:
        dt = now - prev["ts"]
        d_idle = idle - prev["idle"]
        d_total = total - prev["total"]
        cpu_pct = (1 - d_idle / d_total) * 100 if d_total > 0 else 0
        rx_speed = max(0, (rx - prev["rx"]) / dt) if dt > 0 else 0
        tx_speed = max(0, (tx - prev["tx"]) / dt) if dt > 0 else 0
    else:
        cpu_pct = 0
        rx_speed = tx_speed = 0

    sample = {
        "cpu_percent": round(cpu_pct, 1),
        "memory": read_mem(),
        "disk": read_disk("/"),
        "network": {
            "rx_bytes_total": rx,
            "tx_bytes_total": tx,
            "rx_speed_bps": round(rx_speed),
            "tx_speed_bps": round(tx_speed),
        },
        "load": read_load(),
        "uptime_seconds": int(read_uptime()),
        "os": read_os_info(),
        "agent_ts": now,
    }
    new_prev = {"ts": now, "idle": idle, "total": total, "rx": rx, "tx": tx}
    return sample, new_prev


def post(url, token, payload, timeout=8):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Probe-Token": token,
            "User-Agent": "mimo-probe/1.0",
        },
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.status, r.read().decode("utf-8", errors="replace")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("PROBE_URL", ""),
                    help="Panel URL, e.g. https://panel.example/api/probe/report")
    ap.add_argument("--token", default=os.environ.get("PROBE_TOKEN", ""),
                    help="Per-node token issued by panel")
    ap.add_argument("--name", default=os.environ.get("PROBE_NAME", socket.gethostname()),
                    help="Display name shown on panel")
    ap.add_argument("--interval", type=int,
                    default=int(os.environ.get("PROBE_INTERVAL", "10")),
                    help="Report interval in seconds")
    args = ap.parse_args()

    if not args.url or not args.token:
        print("ERROR: --url and --token are required (or set PROBE_URL / PROBE_TOKEN)",
              file=sys.stderr)
        sys.exit(2)

    prev = None
    # Prime the counters so the first POST has real deltas.
    _, prev = collect_sample(prev)
    time.sleep(min(2, args.interval))

    while True:
        started = time.monotonic()
        try:
            sample, prev = collect_sample(prev)
            payload = {"name": args.name, "sample": sample}
            status, _ = post(args.url, args.token, payload)
            if status >= 300:
                print(f"[{time.strftime('%H:%M:%S')}] HTTP {status}", file=sys.stderr)
        except urllib.error.URLError as e:
            print(f"[{time.strftime('%H:%M:%S')}] URLError: {e}", file=sys.stderr)
        except (OSError, ValueError) as e:
            print(f"[{time.strftime('%H:%M:%S')}] {type(e).__name__}: {e}", file=sys.stderr)

        elapsed = time.monotonic() - started
        time.sleep(max(0.1, args.interval - elapsed))


if __name__ == "__main__":
    main()
