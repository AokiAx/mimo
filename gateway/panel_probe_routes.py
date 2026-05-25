"""Probe node management and installer routes."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse


def register_panel_probe_routes(
    app: FastAPI,
    *,
    probe_dir: Path,
) -> None:
    """Attach probe node and installer routes."""

    @app.get("/api/gateway/vps")
    async def gateway_vps_status():
        """List monitored VPS nodes with latest agent samples."""
        from gateway.probe_registry import list_nodes, OFFLINE_AFTER_S
        nodes = list_nodes()
        online = sum(1 for n in nodes if n["online"])
        return {
            "summary": {
                "total": len(nodes),
                "up": online,
                "down": len(nodes) - online,
                "offline_after_s": OFFLINE_AFTER_S,
            },
            "nodes": nodes,
        }

    @app.post("/api/gateway/vps/refresh")
    async def gateway_vps_refresh():
        """Re-read latest snapshots (no-op now: agent push, not poll)."""
        return await gateway_vps_status()

    @app.get("/api/probe/nodes")
    async def probe_nodes_list():
        """Panel-only: list nodes including their tokens (for install command)."""
        from gateway.probe_registry import list_nodes
        return {"nodes": list_nodes(include_token=True)}

    @app.post("/api/probe/nodes/add")
    async def probe_node_add(request: Request):
        """Body: {name}. Returns {id, name, token}."""
        from gateway.probe_registry import add_node
        body = await request.json()
        try:
            return add_node(body.get("name", ""))
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    @app.post("/api/probe/nodes/{node_id}/delete")
    async def probe_node_delete(node_id: str):
        from gateway.probe_registry import delete_node
        ok = delete_node(node_id)
        return {"success": ok} if ok else JSONResponse(
            {"success": False, "error": "节点不存在"}, status_code=404)

    @app.post("/api/probe/nodes/{node_id}/regen-token")
    async def probe_node_regen_token(node_id: str):
        from gateway.probe_registry import regenerate_token
        token = regenerate_token(node_id)
        return {"token": token} if token else JSONResponse(
            {"error": "节点不存在"}, status_code=404)

    @app.post("/api/probe/report")
    async def probe_report(request: Request):
        """Agent endpoint — called every interval seconds with a sample."""
        from gateway.probe_registry import ingest_report
        token = request.headers.get("X-Probe-Token", "")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid json"}, status_code=400)
        ok = ingest_report(token, body.get("name"), body.get("sample") or {})
        if not ok:
            return JSONResponse({"error": "unknown token"}, status_code=401)
        return {"ok": True}

    @app.get("/probe/agent.py")
    async def probe_get_agent():
        """Serve agent.py for the install script to download."""
        p = probe_dir / "agent.py"
        if not p.exists():
            return PlainTextResponse("agent.py not found", status_code=404)
        return PlainTextResponse(p.read_text(encoding="utf-8"), media_type="text/x-python")

    @app.get("/probe/install.sh/{token}")
    async def probe_install_script(token: str, request: Request, name: str = ""):
        """One-shot installer for probe nodes."""
        from gateway.probe_registry import list_nodes
        nodes = list_nodes(include_token=True)
        matched = next((n for n in nodes if n.get("token") == token), None)
        if not matched:
            return PlainTextResponse(
                "echo 'ERROR: invalid or expired token'; exit 1\n",
                status_code=404, media_type="text/x-shellscript",
            )
        base = str(request.base_url).rstrip("/")
        display_name = name or matched.get("name", "")

        def _q(s):
            return "'" + str(s).replace("'", "'\\''") + "'"

        script = f"""#!/bin/bash
# MiMo VPS probe — one-shot installer
set -e

PROBE_URL={_q(base + "/api/probe/report")}
PROBE_TOKEN={_q(token)}
PROBE_NAME={_q(display_name) if display_name else '"$(hostname)"'}
PROBE_INTERVAL="${{PROBE_INTERVAL:-10}}"
INSTALL_DIR="${{INSTALL_DIR:-/opt/mimo-probe}}"
AGENT_URL={_q(base + "/probe/agent.py")}

if [ "$EUID" -ne 0 ]; then
    echo "ERROR: must run as root (try: curl ... | sudo bash)"
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 not found. Please install python3 first."
    exit 1
fi

echo ">> Installing to $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"

echo ">> Fetching agent.py from $AGENT_URL"
if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$AGENT_URL" -o "$INSTALL_DIR/agent.py"
elif command -v wget >/dev/null 2>&1; then
    wget -qO "$INSTALL_DIR/agent.py" "$AGENT_URL"
else
    echo "ERROR: need curl or wget"
    exit 1
fi
chmod 755 "$INSTALL_DIR/agent.py"

echo ">> Writing /etc/systemd/system/mimo-probe.service"
cat > /etc/systemd/system/mimo-probe.service <<UNIT
[Unit]
Description=MiMo VPS Probe Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=PROBE_URL=$PROBE_URL
Environment=PROBE_TOKEN=$PROBE_TOKEN
Environment=PROBE_NAME=$PROBE_NAME
Environment=PROBE_INTERVAL=$PROBE_INTERVAL
ExecStart=/usr/bin/python3 $INSTALL_DIR/agent.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

echo ">> Enabling and starting mimo-probe"
systemctl daemon-reload
systemctl enable --now mimo-probe

sleep 2
if systemctl is-active --quiet mimo-probe; then
    echo ""
    echo "✓ mimo-probe is running"
    echo "  Logs:   journalctl -u mimo-probe -f"
    echo "  Status: systemctl status mimo-probe"
else
    echo ""
    echo "✗ mimo-probe failed to start"
    journalctl -u mimo-probe --no-pager -n 30
    exit 1
fi
"""
        return PlainTextResponse(script, media_type="text/x-shellscript")
