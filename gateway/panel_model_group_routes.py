"""Model-group management routes for the panel."""
from __future__ import annotations

from fastapi import FastAPI, Request


def register_panel_model_group_routes(app: FastAPI) -> None:
    """Attach model-group management routes."""

    @app.get("/api/model-groups")
    async def model_groups_list():
        from gateway.model_groups_store import list_groups, ensure_default_initialized
        ensure_default_initialized()
        return {"groups": list_groups()}

    @app.post("/api/model-groups")
    async def model_groups_add(request: Request):
        """Create a new group. Body: {id, name, description}."""
        body = await request.json()
        try:
            from gateway.model_groups_store import add_group
            group = add_group(
                id=body.get("id", ""),
                name=body.get("name", ""),
                description=body.get("description", ""),
            )
            return {"success": True, "group": group}
        except ValueError as e:
            return {"success": False, "error": str(e)}

    @app.post("/api/model-groups/{group_id}/update")
    async def model_groups_update(group_id: str, request: Request):
        """Update group metadata. Body: {name?, description?}."""
        body = await request.json()
        from gateway.model_groups_store import update_group
        group = update_group(
            group_id,
            name=body.get("name", ""),
            description=body.get("description", ""),
        )
        if group is None:
            return {"success": False, "error": f"分组 {group_id!r} 不存在"}
        return {"success": True, "group": group}

    @app.post("/api/model-groups/{group_id}/delete")
    async def model_groups_delete(group_id: str):
        from gateway.model_groups_store import delete_group
        ok = delete_group(group_id)
        if not ok:
            return {"success": False, "error": f"分组 {group_id!r} 不存在"}
        return {"success": True}

    @app.post("/api/model-groups/{group_id}/mappings")
    async def model_groups_mapping_add(group_id: str, request: Request):
        """Add a mapping to a group. Body: {exposed_name, native_model, protocols?}."""
        body = await request.json()
        try:
            from gateway.model_groups_store import add_mapping
            mapping = add_mapping(
                group_id,
                exposed_name=body.get("exposed_name", ""),
                native_model=body.get("native_model", ""),
                protocols=body.get("protocols"),
            )
            if mapping is None:
                return {"success": False, "error": f"分组 {group_id!r} 不存在"}
            return {"success": True, "mapping": mapping}
        except ValueError as e:
            return {"success": False, "error": str(e)}

    @app.post("/api/model-groups/{group_id}/mappings/{mapping_id}/update")
    async def model_groups_mapping_update(group_id: str, mapping_id: str, request: Request):
        body = await request.json()
        from gateway.model_groups_store import update_mapping
        mapping = update_mapping(
            group_id,
            mapping_id,
            exposed_name=body.get("exposed_name", ""),
            native_model=body.get("native_model", ""),
            protocols=body.get("protocols"),
        )
        if mapping is None:
            return {"success": False, "error": "映射不存在"}
        return {"success": True, "mapping": mapping}

    @app.post("/api/model-groups/{group_id}/mappings/{mapping_id}/delete")
    async def model_groups_mapping_delete(group_id: str, mapping_id: str):
        from gateway.model_groups_store import delete_mapping
        ok = delete_mapping(group_id, mapping_id)
        if not ok:
            return {"success": False, "error": "映射不存在"}
        return {"success": True}

    @app.post("/api/model-groups/import-from-backends")
    async def model_groups_import_backends(request: Request):
        """One-click: scan all backends and create 1:1 mappings in a target group."""
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        from gateway.model_groups_store import import_from_backends
        result = import_from_backends(
            group_id=body.get("group_id") or "mimo",
            group_name=body.get("group_name") or "MiMo 原生",
        )
        return {"success": True, **result}
