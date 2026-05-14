"""Gateway authentication helpers.

Keeps API-key validation out of ``app.py`` so the data-plane route can stay
small while still supporting the legacy single public token for existing
installations.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi import Request

from gateway.config import APIKeyRecord, APIKeyStore
from gateway.core import AuthError
from gateway.secrets_store import secrets

API_KEYS_DB = Path(__file__).parent.parent / "data" / "api_keys.db"


@dataclass(frozen=True)
class GatewayPrincipal:
    """Authenticated caller passed to ``RequestContext.principal``."""

    key_id: str
    source: str
    record: APIKeyRecord | None = None

    @property
    def allowed_models(self) -> tuple[str, ...]:
        return self.record.allowed_models if self.record is not None else ()


_key_store: APIKeyStore | None = None


def get_key_store() -> APIKeyStore:
    global _key_store
    if _key_store is None:
        API_KEYS_DB.parent.mkdir(parents=True, exist_ok=True)
        _key_store = APIKeyStore(str(API_KEYS_DB))
    return _key_store


def close_key_store() -> None:
    global _key_store
    if _key_store is not None:
        _key_store.close()
        _key_store = None


async def authenticate_gateway_request(
    request: Request,
    *,
    auth_cookie: str,
) -> GatewayPrincipal:
    """Validate a gateway request and return its principal.

    Order intentionally preserves compatibility:
    1. Panel cookie may call gateway endpoints from the dashboard.
    2. The legacy ``MIMO_PUBLIC_API_TOKEN`` / ``data/secrets.json`` token keeps
       existing clients working.
    3. New per-key APIKeyStore credentials enable revocation and model scoping.
    """
    if request.cookies.get(auth_cookie) == secrets.public_api_token:
        return GatewayPrincipal(key_id="panel", source="panel_cookie")

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise AuthError("Missing or invalid Authorization")
    token = auth[7:].strip()
    if not token:
        raise AuthError("Missing or invalid Authorization")

    if token == secrets.public_api_token:
        return GatewayPrincipal(key_id="legacy", source="public_token")

    rec = await get_key_store().validate(token)
    return GatewayPrincipal(key_id=rec.key_id, source="api_key", record=rec)
