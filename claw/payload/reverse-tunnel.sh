#!/bin/bash
# Hardened reverse tunnel (claw -> target machine).
#
# Exposes this claw's local api-proxy (127.0.0.1:__LOCAL_PROXY_PORT__) on the
# target machine's loopback at 127.0.0.1:__REMOTE_API_PORT__, using an outbound
# SSH connection only (zero inbound ports on the claw).
#
# Design notes vs the legacy reverse-tunnel:
#  - autossh supervises the connection and re-dials on drop (fixes the WS-style
#    "easily disconnects" complaint without a separate cron keepalive).
#  - The target host/user/ports are injected as placeholders by the deploy
#    (claw/auto_deploy.py:_render_tunnel_*). DO NOT hardcode a real host here.
#  - The private key was generated ON the claw (ssh-keygen) and never leaves it.
#    The matching public key is authorized on the target with a locked-down
#    "restrict,permitlisten,command=..." prefix, so even if this claw is
#    compromised the key can ONLY open this one forward — no shell, no other
#    ports. See _deploy_ssh_key() on the panel side.
set -u

# Single-instance guard. The keepalive watchdog may (re)launch this script
# whenever it thinks the tunnel is down; without a lock those launches pile up
# into several `while true` supervisors all dialing the same remote port, so all
# but one lose the bind and churn forever. Hold an exclusive lock so any extra
# launch is a harmless no-op.
LOCKFILE="/tmp/reverse-tunnel.lock"
if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCKFILE"
    if ! flock -n 9; then
        echo "another reverse-tunnel.sh holds the lock; exiting" >&2
        exit 0
    fi
else
    if [ -f "$LOCKFILE" ] && kill -0 "$(cat "$LOCKFILE" 2>/dev/null)" 2>/dev/null; then
        echo "another reverse-tunnel.sh (pid $(cat "$LOCKFILE")) is running; exiting" >&2
        exit 0
    fi
    echo $$ > "$LOCKFILE"
fi

TARGET_HOST="__TARGET_HOST__"
TARGET_USER="__TARGET_USER__"
TARGET_SSH_PORT="__TARGET_SSH_PORT__"
REMOTE_API_PORT="__REMOTE_API_PORT__"
LOCAL_PROXY_PORT="__LOCAL_PROXY_PORT__"
KEY="/root/.openclaw/workspace/.ssh/id_tunnel"

SSH_OPTS=(
    -i "$KEY"
    -p "$TARGET_SSH_PORT"
    -o BatchMode=yes
    -o ExitOnForwardFailure=yes
    -o ServerAliveInterval=20
    -o ServerAliveCountMax=3
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
    -o UserKnownHostsFile=/root/.openclaw/workspace/.ssh/known_hosts
    -N
    -R "127.0.0.1:${REMOTE_API_PORT}:127.0.0.1:${LOCAL_PROXY_PORT}"
    "${TARGET_USER}@${TARGET_HOST}"
)

# Prefer autossh when present (self-supervising). The claw's apt repo is often
# unreachable, so fall back to a plain-ssh reconnect loop — same robustness
# (ServerAliveInterval + ExitOnForwardFailure make a dead -R forward exit fast,
# the loop re-dials). No extra package required.
if command -v autossh >/dev/null 2>&1; then
    export AUTOSSH_GATETIME=0 AUTOSSH_POLL=30 AUTOSSH_PORT=0
    exec autossh -M 0 "${SSH_OPTS[@]}"
else
    while true; do
        ssh "${SSH_OPTS[@]}"
        sleep 5
    done
fi

