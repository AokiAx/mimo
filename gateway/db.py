"""Single consolidated SQLite database for the gateway.

Metrics, API keys, and the reasoning cache used to live in three separate
SQLite files (data/metrics.db, data/api_keys.db, data/reasoning_cache.db). They
are independent tables, so we keep them in ONE file (data/mimo.db) to cut down
on the pile of runtime files. Each owning module still creates/manages its own
tables — this module only provides the shared path and a one-time migration
that copies any pre-existing legacy DBs into mimo.db (schema + rows preserved),
so upgrading an existing install never loses data (notably the real API keys).

Path is overridable via ``MIMO_DB``. ``MIMO_REASONING_CACHE_DB`` still wins for
the reasoning cache specifically (reasoning_cache.py honors it).
"""
from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

_DEFAULT = Path(__file__).parent.parent / "data" / "mimo.db"
DB_PATH = Path(os.environ.get("MIMO_DB") or _DEFAULT)

# Legacy per-feature DB filenames (relative to DB_PATH's dir) to fold in once.
_LEGACY_FILES = ("metrics.db", "api_keys.db", "reasoning_cache.db")


def _copy_legacy(conn: sqlite3.Connection, legacy: Path) -> None:
    """Copy every user table (schema + rows + indexes) from a legacy DB file
    into the open mimo.db connection. Skips sqlite internals."""
    conn.execute(f"ATTACH '{legacy.as_posix()}' AS leg")
    try:
        objs = conn.execute(
            "SELECT type, name, sql FROM leg.sqlite_master "
            "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' "
            "AND sql IS NOT NULL"
        ).fetchall()
        # tables first, then indexes
        for kind in ("table", "index"):
            for otype, name, sql in objs:
                if otype != kind:
                    continue
                conn.execute(sql)  # recreate object in main with identical schema
                if otype == "table":
                    conn.execute(f'INSERT INTO main."{name}" SELECT * FROM leg."{name}"')
        conn.commit()
    finally:
        conn.execute("DETACH leg")


def migrate_legacy_once() -> None:
    """If mimo.db does not exist yet but legacy per-feature DBs do, fold them in
    once. No-op for fresh installs (modules will just create mimo.db) and for
    already-consolidated installs (mimo.db present)."""
    if DB_PATH.exists():
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    legacy = [DB_PATH.parent / n for n in _LEGACY_FILES if (DB_PATH.parent / n).exists()]
    if not legacy:
        return  # fresh install — let owning modules create their tables
    conn = sqlite3.connect(str(DB_PATH))
    try:
        for lf in legacy:
            try:
                _copy_legacy(conn, lf)
                lf.rename(lf.with_suffix(lf.suffix + ".bak"))
                log.info("consolidated legacy DB %s into %s (backed up as .bak)", lf.name, DB_PATH.name)
            except Exception:  # noqa: BLE001
                log.exception("failed to consolidate legacy DB %s", lf)
    finally:
        conn.close()


# Run once at import, before any owning module connects to DB_PATH.
migrate_legacy_once()
