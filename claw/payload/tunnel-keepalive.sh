#!/bin/bash
# Tunnel keepalive — verifies the reverse tunnel is BOTH alive (ssh process
# running) AND working (jump-side port actually accepts a TCP connect).
# The old keepalive only checked `pgrep ssh`, which missed the zombie case
# where ssh was alive but `-R` forwarding had silently failed.
#
# Run from cron every 5 minutes; restarts the tunnel up to MAX_RETRIES.
set -u

LOG="/tmp/tunnel-keepalive.log"
JUMP_HOST="149.88.90.137"
API_PORT="__API_PORT__"
TUNNEL_SCRIPT="/root/.openclaw/workspace/scripts/reverse-tunnel.sh"
MAX_RETRIES=3

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

# Probe via the jump server itself: bash's /dev/tcp opens a raw TCP socket
# to 127.0.0.1:$API_PORT on the jump host. If our -R forward is alive and
# the api-proxy is listening on this end, the connect succeeds.
tunnel_works() {
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
        -i /root/.ssh/id_ed25519 \
        root@"$JUMP_HOST" \
        "timeout 3 bash -c '</dev/tcp/127.0.0.1/${API_PORT}' 2>/dev/null" \
        2>/dev/null
}

restart_tunnel() {
    log "killing stale ssh -R processes"
    pkill -f "ssh.*-R 127.0.0.1:${API_PORT}" 2>/dev/null || true
    sleep 2
    log "starting fresh tunnel"
    nohup "$TUNNEL_SCRIPT" > /tmp/tunnel.log 2>&1 &
    sleep 4
}

for attempt in $(seq 1 "$MAX_RETRIES"); do
    if tunnel_works; then
        # Quiet success — only log on state change would be nicer, but cron
        # runs every 5 min so a noisy log is acceptable.
        log "OK (attempt ${attempt})"
        exit 0
    fi
    log "attempt ${attempt}/${MAX_RETRIES}: tunnel dead, restarting"
    restart_tunnel
done

log "FAILED after ${MAX_RETRIES} attempts — tunnel still down"
exit 1
