#!/bin/bash
# One-time target-machine setup for scheme B (run once per target, as root).
#
# Single-user model: one `tunnel` user (nologin). Its authorized_keys holds
#   - ONE panel-admin line (forced command -> authorize-tunnel-key), so the
#     panel can append per-claw keys but can never get a shell on this box.
#   - N claw lines (added later by the panel), each locked to a single reverse
#     forward via permitlisten.
#
# Usage:
#   sudo ./setup-target.sh '<panel-admin-ed25519-pubkey>'
#
# Then hand the panel: host, ssh_port (default 22), tunnel user (default tunnel).
# The panel uses its admin PRIVATE key (matching the pubkey passed here) both to
# authorize claw keys and as the identity claws are bootstrapped against.
set -euo pipefail

PANEL_PUBKEY="${1:?usage: setup-target.sh '<panel-admin-ed25519-pubkey>'}"
TUNNEL_USER="${TUNNEL_USER:-tunnel}"
AUTHORIZER="/usr/local/bin/authorize-tunnel-key"
HERE="$(cd "$(dirname "$0")" && pwd)"

# Login shell must be a REAL shell (/bin/bash), NOT nologin: sshd executes the
# forced command via the login shell's -c, and nologin would refuse it
# ("This account is currently not available"), breaking both the authorizer and
# the claw reverse tunnel. Security is enforced by the per-key
# restrict/permitlisten + forced command, not by the login shell.
if ! id "$TUNNEL_USER" >/dev/null 2>&1; then
    useradd -r -m -s /bin/bash "$TUNNEL_USER"
else
    usermod -s /bin/bash "$TUNNEL_USER"
fi
HOME_DIR="$(getent passwd "$TUNNEL_USER" | cut -d: -f6)"
install -d -m 700 -o "$TUNNEL_USER" -g "$TUNNEL_USER" "$HOME_DIR/.ssh"

install -m 755 "$HERE/authorize-tunnel-key.sh" "$AUTHORIZER"

# Seed authorized_keys with the single panel-admin forced-command line.
# (Re-running setup is idempotent: drop any prior panel line, keep claw lines.)
AUTHK="$HOME_DIR/.ssh/authorized_keys"
PANEL_LINE="command=\"$AUTHORIZER\",restrict $PANEL_PUBKEY"
TMP="$(mktemp)"
if [ -f "$AUTHK" ]; then grep -vF "$AUTHORIZER" "$AUTHK" > "$TMP" || true; fi
printf '%s\n' "$PANEL_LINE" >> "$TMP"
install -m 600 -o "$TUNNEL_USER" -g "$TUNNEL_USER" "$TMP" "$AUTHK"
rm -f "$TMP"

echo "[setup] done."
echo "  tunnel user : $TUNNEL_USER (nologin; panel-admin + per-claw locked forwards)"
echo "  authorizer  : $AUTHORIZER"
echo "  hand panel  : host, ssh_port, tunnel user='$TUNNEL_USER'"
echo "  ensure sshd : AllowTcpForwarding remote (or yes); GatewayPorts no (loopback-only forwards)"
