#!/bin/bash
set -e
cd "$(dirname "$0")"
pip install -q -r requirements.txt 2>/dev/null
# WARP SOCKS for MiMo create/API — keeps create off the blocked host IP.
# Override with empty string to disable: MIMO_PROXY= ./run.sh
export MIMO_PROXY="${MIMO_PROXY:-socks5://127.0.0.1:40001}"
exec python3 -m uvicorn app:app --host 0.0.0.0 --port 8088

