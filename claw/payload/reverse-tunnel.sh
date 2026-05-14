#!/bin/bash
# Reverse SSH tunnel: expose this ECS's api-proxy (127.0.0.1:18800) to the
# jump server at 127.0.0.1:__API_PORT__.
#
# Key option: ExitOnForwardFailure=yes — if the remote port binding fails
# (e.g. someone else is already bound to the jump-side port), ssh exits
# instead of staying alive as a "zombie" with a dead forward, which is what
# made the simple "pgrep ssh" keepalive miss the failure mode.
exec ssh \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o ConnectTimeout=10 \
    -o ExitOnForwardFailure=yes \
    -o StrictHostKeyChecking=no \
    -R 127.0.0.1:__API_PORT__:127.0.0.1:18800 \
    root@149.88.90.137 -N
