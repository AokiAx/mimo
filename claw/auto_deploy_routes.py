"""FastAPI routes for auto-deploy operations."""
from __future__ import annotations

from fastapi import FastAPI, Request


def register_auto_deploy_routes(app: FastAPI) -> None:
    """Attach auto-deploy routes to ``app``."""

    @app.get("/api/auto-deploy/config")
    async def auto_deploy_config():
        from claw.auto_deploy import load_config
        return load_config()

    @app.post("/api/auto-deploy/config")
    async def auto_deploy_config_update(request: Request):
        from claw.auto_deploy import load_config, save_config
        body = await request.json()
        cfg = load_config()

        if "accounts" in body:
            for acc_name, acc_cfg in body["accounts"].items():
                if acc_name not in cfg["accounts"]:
                    cfg["accounts"][acc_name] = {}
                cfg["accounts"][acc_name].update(acc_cfg)

        save_config(cfg)
        return {"success": True, "config": cfg}

    @app.post("/api/auto-deploy/account/{account_filename}")
    async def auto_deploy_account_update(account_filename: str, request: Request):
        """Update a single account's deploy config."""
        from claw.auto_deploy import load_config, save_config
        body = await request.json()
        cfg = load_config()

        if "accounts" not in cfg:
            cfg["accounts"] = {}
        if account_filename not in cfg["accounts"]:
            cfg["accounts"][account_filename] = {
                "enabled": False,
                "schedule_time": "03:00",
                "interval_hours": 24,
                "port": 8800,
            }
        cfg["accounts"][account_filename].update(body)
        save_config(cfg)
        return {"success": True, "account": cfg["accounts"][account_filename]}

    @app.post("/api/auto-deploy/trigger/{account_filename}")
    async def auto_deploy_trigger(account_filename: str):
        """Manually trigger deployment for an account."""
        from claw.auto_deploy import trigger_deploy
        return trigger_deploy(account_filename)

    @app.post("/api/auto-deploy/cancel/{account_filename}")
    async def auto_deploy_cancel(account_filename: str):
        """Cancel an active deployment."""
        from claw.auto_deploy import cancel_deploy
        return cancel_deploy(account_filename)

    @app.get("/api/auto-deploy/status")
    async def auto_deploy_status():
        """Get deployment status for all accounts."""
        from claw.auto_deploy import get_deploy_status, get_scheduler_status
        return {
            "deploys": get_deploy_status(),
            "scheduler": get_scheduler_status(),
        }

    @app.get("/api/auto-deploy/status/{account_filename}")
    async def auto_deploy_account_status(account_filename: str):
        """Get deployment status for a specific account."""
        from claw.auto_deploy import get_deploy_status
        return get_deploy_status(account_filename)

    @app.get("/api/auto-deploy/history/{account_filename}")
    async def auto_deploy_history(account_filename: str):
        """Get run history for a specific account."""
        from claw.auto_deploy import get_run_history
        return {"history": get_run_history(account_filename)}
