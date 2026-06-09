#!/bin/bash
# Lightweight watchdog for the claw-side data plane.
#
# reverse-tunnel.sh already re-dials a dropped SSH connection (ExitOnForwardFailure
# + ServerAlive make a dead -R forward exit fast, the loop re-dials) and holds a
# single-instance lock. This watchdog is a cheap cron backstop: it only restarts
# what is actually down, checks are all LOCAL (it never opens a second SSH
# connection to probe, which used to jam the target sshd).
#
# Two rules learned the hard way (see deploy/activity transcripts):
#  - NEVER use a broad `pkill -f reverse-tunnel.sh`: the maintenance agent often
#    has that path on its own command line (it cat/tail/bash'd the script), so a
#    broad match kills the agent's own shell mid-session. Kill by explicit PID.
#  - "an ssh process exists" != "the forward works". A wedged/auth-failed/zombie
#    ssh still matches pgrep and fooled the old check into never restarting. We
#    reap defunct holders and gate on the *supervisor* (reverse-tunnel.sh), whose
#    presence is stable, rather than the ssh child, which flickers between dials.
set -u

LOG="/tmp/tunnel-keepalive.log"
SCRIPTS="/root/.openclaw/workspace/scripts"
LOCAL_PROXY_PORT="__LOCAL_PROXY_PORT__"
REMOTE_API_PORT="__REMOTE_API_PORT__"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

# Precise kill of explicit PIDs only — never a broad name pattern.
kill_pids() { for p in "$@"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }

# 1. api-proxy must be listening locally; if not, the forward points at nothing.
if ! ss -tln 2>/dev/null | grep -q ":${LOCAL_PROXY_PORT} "; then
    log "api-proxy down on :${LOCAL_PROXY_PORT}, restarting"
    kill_pids $(pgrep -f "$SCRIPTS/api-proxy.py" 2>/dev/null)
    nohup python "$SCRIPTS/api-proxy.py" > /tmp/api-proxy.log 2>&1 &
    sleep 2
fi

# 2. Reap defunct (zombie) ssh -R holders for our port. A <defunct> process still
#    matches pgrep and used to make the supervisor/keepalive believe the tunnel
#    was alive while no forward existed.
for p in $(pgrep -f "ssh.* -R 127.0.0.1:${REMOTE_API_PORT}:" 2>/dev/null); do
    state=$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null)
    if [ "$state" = "Z" ]; then
        log "reaping defunct ssh pid=$p"
        kill_pids "$p"
    fi
done

# 3. Ensure exactly one tunnel supervisor is running. reverse-tunnel.sh holds a
#    flock, so launching it when one is already healthy is a harmless no-op — no
#    broad pkill needed. We gate on the supervisor (stable) not the ssh child
#    (flickers during the re-dial sleep), so we don't thrash mid-reconnect.
if ! pgrep -f "$SCRIPTS/reverse-tunnel.sh" >/dev/null 2>&1; then
    log "no reverse-tunnel supervisor for remote :${REMOTE_API_PORT}, starting"
    nohup bash "$SCRIPTS/reverse-tunnel.sh" >> /tmp/reverse-tunnel.log 2>&1 &
fi

