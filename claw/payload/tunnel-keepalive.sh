#!/bin/bash
# Tunnel keepalive — verifies the reverse tunnel is BOTH alive (ssh process
# running) AND working (the api-proxy this side is forwarding to is up).
#
# Pre-2026-05 versions opened a *second* SSH connection to the jump server
# every 5 min just to probe via /dev/tcp. With many accounts that doubled
# the inbound connection count and contributed to the jump-side sshd
# MaxStartups jam ("挤爆"). Local checks are enough because reverse-tunnel.sh
# uses ServerAliveInterval=30 + ServerAliveCountMax=3 + ExitOnForwardFailure,
# so a dead -R forward causes the local ssh process to exit within ~90s,
# which the pgrep check below catches.
#
# Run from cron every minute; restarts the tunnel up to MAX_RETRIES.
set -u

LOG="/tmp/tunnel-keepalive.log"
API_PORT="__API_PORT__"
TUNNEL_SCRIPT="/root/.openclaw/workspace/scripts/reverse-tunnel.sh"
MAX_RETRIES=3

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

tunnel_works() {
    # 1. The ssh -R process must still be running.
    pgrep -f "ssh.*-R 127.0.0.1:${API_PORT}:" >/dev/null || return 1
    # 2. The local api-proxy must be listening on 18800; otherwise the
    #    forward has nothing to point at and restarting the tunnel won't
    #    fix anything (api-proxy.service will, via systemd Restart=always).
    ss -tln 2>/dev/null | grep -q ':18800 ' || return 1
    return 0
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
        log "OK (attempt ${attempt})"
        exit 0
    fi
    log "attempt ${attempt}/${MAX_RETRIES}: tunnel dead, restarting"
    restart_tunnel
done

log "FAILED after ${MAX_RETRIES} attempts — tunnel still down"
exit 1
