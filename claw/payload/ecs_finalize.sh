#!/bin/bash
# Panel-side finalization: run via `ssh root@<ECS> bash -s < ecs_finalize.sh`.
#
# This script finishes the deployment that the deploy_text intentionally
# stops short of — installing aiohttp, writing the systemd unit, starting
# api-proxy, and bringing up the API reverse tunnel + keepalive.
#
# Required env: API_PORT (8800/8801/8802 — which jump-server-side port the
# API gets exposed on). Optional: JUMP_HOST (defaults to 149.88.90.137).
set -e

: "${API_PORT:?API_PORT env required (e.g. 8802)}"
: "${JUMP_HOST:=149.88.90.137}"

SCRIPTS_DIR="/root/.openclaw/workspace/scripts"
mkdir -p "$SCRIPTS_DIR"

# api-proxy.py was scp'd to /tmp before this script runs. Move to its
# canonical path and make executable.
if [ -f /tmp/api-proxy.py ]; then
    mv -f /tmp/api-proxy.py "$SCRIPTS_DIR/api-proxy.py"
    chmod +x "$SCRIPTS_DIR/api-proxy.py"
fi
[ -x "$SCRIPTS_DIR/api-proxy.py" ] || { echo "FATAL: api-proxy.py missing" >&2; exit 2; }

# ── Install aiohttp (idempotent; quiet) ──
# Ubuntu 24 / Python 3.12 enforces PEP 668 — system pip refuses to install
# without --break-system-packages. We're in a single-purpose Claw container,
# so polluting site-packages is the intended outcome here.
echo "[1/6] pip install aiohttp..."
python3 -m pip install --break-system-packages -q aiohttp 2>&1 | tail -3
python3 -c 'import aiohttp; print("  aiohttp", aiohttp.__version__)' || {
    echo "FATAL: aiohttp import failed after install" >&2
    exit 3
}

# ── Write reverse-tunnel.sh ──
echo "[2/6] write reverse-tunnel.sh (API_PORT=$API_PORT)..."
cat > "$SCRIPTS_DIR/reverse-tunnel.sh" << EOF
#!/bin/bash
# Reverse SSH tunnel: expose this ECS's api-proxy (127.0.0.1:18800) to the
# jump server at 127.0.0.1:$API_PORT. ExitOnForwardFailure=yes avoids the
# zombie state where ssh stays alive while -R forwarding is dead.
exec ssh \\
    -o ServerAliveInterval=30 \\
    -o ServerAliveCountMax=3 \\
    -o ConnectTimeout=10 \\
    -o ExitOnForwardFailure=yes \\
    -o StrictHostKeyChecking=no \\
    -R 127.0.0.1:$API_PORT:127.0.0.1:18800 \\
    root@$JUMP_HOST -N
EOF
chmod +x "$SCRIPTS_DIR/reverse-tunnel.sh"

# ── Write tunnel-keepalive.sh ──
echo "[3/6] write tunnel-keepalive.sh..."
cat > "$SCRIPTS_DIR/tunnel-keepalive.sh" << KEEPALIVE
#!/bin/bash
# Verifies the reverse tunnel is BOTH alive (process) AND working (jump-side
# port reachable). Restarts up to MAX_RETRIES times.
set -u
LOG="/tmp/tunnel-keepalive.log"
JUMP_HOST="$JUMP_HOST"
API_PORT="$API_PORT"
TUNNEL_SCRIPT="$SCRIPTS_DIR/reverse-tunnel.sh"
MAX_RETRIES=3
log() { echo "[\$(date '+%F %T')] \$*" >> "\$LOG"; }
tunnel_works() {
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no \\
        -i /root/.ssh/id_ed25519 \\
        root@"\$JUMP_HOST" \\
        "timeout 3 bash -c '</dev/tcp/127.0.0.1/\${API_PORT}' 2>/dev/null" \\
        2>/dev/null
}
restart_tunnel() {
    log "killing stale ssh -R processes"
    pkill -f "ssh.*-R 127.0.0.1:\${API_PORT}" 2>/dev/null || true
    sleep 2
    log "starting fresh tunnel"
    nohup "\$TUNNEL_SCRIPT" > /tmp/tunnel.log 2>&1 &
    sleep 4
}
for attempt in \$(seq 1 "\$MAX_RETRIES"); do
    if tunnel_works; then log "OK (attempt \$attempt)"; exit 0; fi
    log "attempt \$attempt/\$MAX_RETRIES: tunnel dead, restarting"
    restart_tunnel
done
log "FAILED after \$MAX_RETRIES attempts"
exit 1
KEEPALIVE
chmod +x "$SCRIPTS_DIR/tunnel-keepalive.sh"

# ── systemd unit ──
echo "[4/6] register systemd service..."
cat > /etc/systemd/system/api-proxy.service << 'EOF'
[Unit]
Description=MiMo API Proxy
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /root/.openclaw/workspace/scripts/api-proxy.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable api-proxy 2>&1 | tail -1
systemctl restart api-proxy
sleep 2

# ── Start API reverse tunnel (replace any stale) ──
echo "[5/6] start API reverse tunnel..."
pkill -f "ssh.*-R 127.0.0.1:$API_PORT:" 2>/dev/null || true
sleep 1
nohup "$SCRIPTS_DIR/reverse-tunnel.sh" > /tmp/tunnel.log 2>&1 &
sleep 3

# ── Crontab ──
echo "[6/6] register tunnel-keepalive cron..."
(crontab -l 2>/dev/null | grep -v tunnel-keepalive; echo "*/5 * * * * $SCRIPTS_DIR/tunnel-keepalive.sh") | crontab -

# ── Final state ──
echo ""
echo "=== final state ==="
echo "service: $(systemctl is-active api-proxy)"
echo "18800 listen: $(ss -tlnp 2>/dev/null | grep -c ':18800 ')"
echo "tunnel ssh procs: $(pgrep -fc "ssh.*-R 127.0.0.1:$API_PORT:" || echo 0)"
echo "journal tail:"
journalctl -u api-proxy -n 5 --no-pager 2>&1 | tail -5
