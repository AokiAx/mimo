"""Free API pool — manages multi-channel access to MiMo's free OpenAI-compatible API.

Each channel is either:
  - ``direct``  : a local source IP (or ``auto`` for the default)
  - ``proxy``   : an HTTP proxy URL (e.g. ``http://127.0.0.1:11000``)

Channels are registered as Backends in the gateway so the existing router can
select them for traffic. Direct channels get a lower routing score (picked
first); proxy channels serve as overflow when direct capacity is exhausted.

JWT tokens are auto-refreshed ~5 minutes before expiry (1-hour TTL).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from gateway import config_store

logger = logging.getLogger(__name__)

# ── constants ──

BOOTSTRAP_URL = "https://api.xiaomimimo.com/api/free-ai/bootstrap"
CHAT_URL = "https://api.xiaomimimo.com/api/free-ai/openai/chat"
JWT_REFRESH_BEFORE_S = 300
JWT_TTL_S = 3600

CLAIMED_MODELS = [
    "mimo-auto",
    "mimo-v2.5-pro",
    "mimo-v2.5",
    "mimo-v2-flash",
    "mimo-v2-pro",
]


class FreeApiConfig:
    """Read/write the free_api section of data/config.json."""

    SECTION = "free_api"

    @staticmethod
    def load() -> dict:
        cfg = config_store.get_section(FreeApiConfig.SECTION)
        if not isinstance(cfg, dict):
            return {"enabled": False, "direct_ips": [], "proxy_ports": []}
        return cfg

    @staticmethod
    def save(cfg: dict) -> None:
        config_store.set_section(FreeApiConfig.SECTION, cfg)

    @staticmethod
    def get_direct_ips() -> list[str]:
        return FreeApiConfig.load().get("direct_ips") or []

    @staticmethod
    def get_proxy_ports() -> list[int]:
        return FreeApiConfig.load().get("proxy_ports") or []

    @staticmethod
    def is_enabled() -> bool:
        return bool(FreeApiConfig.load().get("enabled", False))

    @staticmethod
    def get_models() -> list[str]:
        cfg = FreeApiConfig.load()
        return cfg.get("models") or list(CLAIMED_MODELS)


@dataclass
class FreeApiChannel:
    """One (IP|proxy) x JWT channel to the free API."""

    channel_id: str
    proxy_url: str | None = None
    source_ip: str | None = None
    jwt: str = ""
    jwt_expires_at: float = 0.0
    last_refreshed_at: float = 0.0
    healthy: bool = True
    last_error: str = ""
    total_requests: int = 0
    total_errors: int = 0
    latency_ms: list[float] = field(default_factory=list)

    def needs_refresh(self, now: float | None = None) -> bool:
        now = now or time.time()
        return (self.jwt == "") or (now >= self.jwt_expires_at - JWT_REFRESH_BEFORE_S)

    @property
    def label(self) -> str:
        if self.proxy_url:
            return f"proxy:{self.proxy_url}"
        return f"direct:{self.source_ip or 'auto'}"

    @property
    def is_proxy(self) -> bool:
        return self.proxy_url is not None


class FreeApiPool:
    """Manages free API channels, keeps JWTs fresh."""

    def __init__(self):
        self._channels: dict[str, FreeApiChannel] = {}
        self._lock = threading.Lock()
        self._refresh_task: asyncio.Task | None = None
        self._started = False
        self._httpx_client: httpx.AsyncClient | None = None

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._rebuild_channels()
        if self._channels:
            logger.info("free_api_pool: started with %d channel(s)", len(self._channels))
        else:
            logger.info("free_api_pool: started with no channels (empty config)")

    async def start_async(self) -> None:
        self.start()
        self._httpx_client = httpx.AsyncClient(timeout=httpx.Timeout(15))
        await self._refresh_all_jwts()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def shutdown(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        if self._httpx_client:
            await self._httpx_client.aclose()
            self._httpx_client = None

    def _rebuild_channels(self) -> None:
        cfg = FreeApiConfig.load()
        if not cfg.get("enabled", False):
            self._channels.clear()
            return
        new: dict[str, FreeApiChannel] = {}
        for ip in cfg.get("direct_ips") or []:
            cid = f"direct:{ip}"
            if cid in self._channels:
                new[cid] = self._channels[cid]
            else:
                new[cid] = FreeApiChannel(channel_id=cid, source_ip=None if ip == "auto" else ip)
        for port in cfg.get("proxy_ports") or []:
            cid = f"proxy:{port}"
            if cid in self._channels:
                new[cid] = self._channels[cid]
            else:
                new[cid] = FreeApiChannel(channel_id=cid, proxy_url=f"http://127.0.0.1:{port}")
        self._channels = new

    def reload(self) -> int:
        self._rebuild_channels()
        return len(self._channels)

    def _make_fingerprint(self, channel: FreeApiChannel) -> str:
        hostname = socket.gethostname()
        try:
            cpu = subprocess.run("grep 'model name' /proc/cpuinfo | head -1 | cut -d : -f2 | xargs",
                shell=True, capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:
            cpu = "unknown"
        try:
            user = subprocess.run("whoami", shell=True, capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:
            user = "unknown"
        raw = f"{hostname}|linux|x64|{cpu}|{user}|mimo_pool_{channel.channel_id}"
        return hashlib.sha256(raw.encode()).hexdigest()

    async def _refresh_jwt(self, channel: FreeApiChannel) -> str | None:
        if self._httpx_client is None:
            return None
        fp = self._make_fingerprint(channel)
        client = self._httpx_client
        if channel.proxy_url:
            if not hasattr(self, "_proxy_clients"):
                self._proxy_clients = {}
            if channel.proxy_url not in self._proxy_clients:
                self._proxy_clients[channel.proxy_url] = httpx.AsyncClient(timeout=httpx.Timeout(15), proxy=channel.proxy_url)
            client = self._proxy_clients[channel.proxy_url]
        try:
            resp = await client.post(BOOTSTRAP_URL, json={"client": fp})
            if resp.status_code != 200:
                channel.healthy = False
                channel.last_error = f"bootstrap HTTP {resp.status_code}"
                return None
            data = resp.json()
            jwt = data.get("jwt", "")
            if not jwt:
                channel.healthy = False
                channel.last_error = "no jwt"
                return None
            channel.jwt = jwt
            channel.jwt_expires_at = time.time() + JWT_TTL_S
            channel.healthy = True
            channel.last_error = ""
            return jwt
        except httpx.TimeoutException:
            channel.healthy = False
            channel.last_error = "timeout"
        except httpx.ConnectError as e:
            channel.healthy = False
            channel.last_error = f"connect: {e!s}"
        except Exception as e:
            channel.healthy = False
            channel.last_error = f"err: {e!s}"
        return None

    async def _refresh_all_jwts(self) -> int:
        ok = 0
        need = [c for c in self._channels.values() if c.needs_refresh()]
        if not need:
            return 0
        logger.info("free_api_pool: refreshing %d JWT(s)", len(need))
        for ch in need:
            if await self._refresh_jwt(ch):
                ok += 1
        return ok

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(JWT_REFRESH_BEFORE_S)
                await self._refresh_all_jwts()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("free_api_pool: refresh error")
                await asyncio.sleep(30)

    def get_channels(self) -> list[FreeApiChannel]:
        with self._lock:
            return list(self._channels.values())

    def get_healthy_channels(self) -> list[FreeApiChannel]:
        return [c for c in self.get_channels() if c.healthy and c.jwt and not c.needs_refresh()]

    def count(self) -> int:
        return len(self._channels)

    def get_config(self) -> dict[str, Any]:
        cfg = FreeApiConfig.load()
        channels = []
        for c in self.get_channels():
            channels.append({
                "id": c.channel_id,
                "type": "proxy" if c.is_proxy else "direct",
                "target": c.proxy_url or c.source_ip or "auto",
                "healthy": c.healthy,
                "has_jwt": bool(c.jwt),
                "jwt_expires_in": max(0, c.jwt_expires_at - time.time()),
                "last_error": c.last_error,
                "total_requests": c.total_requests,
                "total_errors": c.total_errors,
            })
        return {
            "enabled": cfg.get("enabled", False),
            "models": cfg.get("models") or CLAIMED_MODELS,
            "direct_ips": cfg.get("direct_ips") or [],
            "proxy_ports": cfg.get("proxy_ports") or [],
            "channels": channels,
        }

    async def test_channel(self, channel_id: str, timeout_s: float = 15.0) -> dict[str, Any]:
        ch = self._channels.get(channel_id)
        if not ch:
            return {"success": False, "error": "not found"}
        jwt = await self._refresh_jwt(ch)
        if not jwt:
            return {"success": False, "error": ch.last_error}
        if self._httpx_client is None:
            return {"success": False, "error": "no client"}
        if ch.proxy_url:
            if not hasattr(self, "_proxy_clients"):
                self._proxy_clients = {}
            if ch.proxy_url not in self._proxy_clients:
                self._proxy_clients[ch.proxy_url] = httpx.AsyncClient(timeout=httpx.Timeout(15), proxy=ch.proxy_url)
            test_client = self._proxy_clients[ch.proxy_url]
        else:
            test_client = self._httpx_client
        headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json", "X-Mimo-Source": "mimocode-cli-free"}
        payload = {"model": "mimo-auto", "messages": [{"role": "user", "content": "ping"}], "max_tokens": 5, "stream": False}
        start = time.time()
        try:
            resp = await test_client.post(CHAT_URL, json=payload, headers=headers, timeout=httpx.Timeout(timeout_s))
            lat = time.time() - start
            if resp.status_code == 200:
                return {"success": True, "latency_ms": round(lat * 1000, 1)}
            return {"success": False, "error": f"HTTP {resp.status_code}", "latency_ms": round(lat * 1000, 1)}
        except Exception as e:
            return {"success": False, "error": f"{type(e).__name__}: {e!s}"}


_pool: FreeApiPool | None = None


def get_pool() -> FreeApiPool:
    global _pool
    if _pool is None:
        _pool = FreeApiPool()
    return _pool


def reset_pool() -> None:
    global _pool
    _pool = None


def make_free_api_backends() -> list[dict[str, Any]]:
    """Produce backend entries for all healthy free API channels."""
    pool = get_pool()
    if not FreeApiConfig.is_enabled():
        return []
    entries: list[dict[str, Any]] = []
    models = FreeApiConfig.get_models()
    for channel in pool.get_healthy_channels():
        weight = 5 if not channel.is_proxy else 2
        entries.append({
            "id": f"freeapi-{channel.channel_id}",
            "name": f"免费API:{channel.label}",
            "base_url": "https://api.xiaomimimo.com/api/free-ai",
            "models": models,
            "api_key": channel.jwt or "",
            "weight": weight,
            "enabled": True,
            "lifecycle": "active",
            "metadata": {
                "type": "free_api",
                "channel_id": channel.channel_id,
                "is_proxy": channel.is_proxy,
            },
        })
    return entries