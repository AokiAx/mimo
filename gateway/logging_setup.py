"""Application logging setup and safe log-file readers.

The web panel and gateway mostly run inside one FastAPI process.  This module
centralizes file logging so runtime errors, startup failures, and operational
messages are written to rotated files that can also be viewed from the panel.
"""
from __future__ import annotations

import logging
import os
import re
from collections import deque
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Iterable

_LOG_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_DEFAULT_RETENTION_DAYS = 14


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _build_handler(path: Path, *, level: int, retention_days: int) -> TimedRotatingFileHandler:
    handler = TimedRotatingFileHandler(
        path,
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=False,
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handler.name = f"mimo-file-{path.name}"
    return handler


def _same_log_file(handler: logging.Handler, path: Path) -> bool:
    raw = getattr(handler, "baseFilename", None)
    if not raw:
        return False
    try:
        return Path(raw).resolve() == path.resolve()
    except OSError:
        return False


def setup_logging(base_dir: str | Path) -> Path:
    """Configure root logging with daily rotation and retention.

    Environment variables:
    - ``MIMO_LOG_DIR``: override log directory (default: ``<repo>/logs``)
    - ``MIMO_LOG_RETENTION_DAYS``: number of rotated daily files to keep
    - ``MIMO_LOG_LEVEL``: root log level (default: ``INFO``)
    """
    base = Path(base_dir)
    log_dir = Path(os.environ.get("MIMO_LOG_DIR", str(base / "logs"))).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)

    retention_days = _int_env("MIMO_LOG_RETENTION_DAYS", _DEFAULT_RETENTION_DAYS)
    level_name = os.environ.get("MIMO_LOG_LEVEL", "INFO").upper()
    root_level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(root_level)

    for name, level in (("app.log", logging.INFO), ("error.log", logging.ERROR)):
        handler_name = f"mimo-file-{name}"
        target = log_dir / name
        existing = [
            h for h in root.handlers
            if getattr(h, "name", "") == handler_name
        ]
        reusable = False
        for handler in existing:
            if _same_log_file(handler, target):
                handler.setLevel(level)
                if isinstance(handler, TimedRotatingFileHandler):
                    handler.backupCount = retention_days
                reusable = True
                continue
            root.removeHandler(handler)
            handler.close()
        if not reusable:
            root.addHandler(_build_handler(target, level=level, retention_days=retention_days))

    # Ensure uvicorn loggers propagate into the file handlers while preserving
    # their console output handlers.
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        logging.getLogger(logger_name).setLevel(root_level)

    cleanup_old_logs(log_dir, retention_days=retention_days)
    logging.getLogger(__name__).info(
        "Logging initialized: dir=%s retention_days=%s level=%s",
        log_dir,
        retention_days,
        logging.getLevelName(root_level),
    )
    return log_dir


def cleanup_old_logs(log_dir: str | Path, *, retention_days: int = _DEFAULT_RETENTION_DAYS) -> int:
    """Delete rotated log files beyond the configured retention window.

    ``TimedRotatingFileHandler`` removes files it rotates itself.  This helper
    also handles stale files left behind by previous runs or renamed handlers.
    """
    root = Path(log_dir)
    if not root.exists():
        return 0

    keep_per_prefix = max(1, int(retention_days))
    removed = 0
    prefixes = ("app.log.", "error.log.")
    for prefix in prefixes:
        files = sorted(
            (p for p in root.glob(prefix + "*") if p.is_file()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in files[keep_per_prefix:]:
            old.unlink(missing_ok=True)
            removed += 1
    return removed


def _iter_log_files(log_dir: Path) -> Iterable[Path]:
    if not log_dir.exists():
        return
    for path in log_dir.iterdir():
        if path.is_file() and path.name.startswith(("app.log", "error.log")):
            yield path


def list_log_files(log_dir: str | Path) -> list[dict]:
    """Return metadata for logs exposed in the web UI."""
    root = Path(log_dir)
    files = []
    for path in sorted(_iter_log_files(root), key=lambda p: p.stat().st_mtime, reverse=True):
        st = path.stat()
        files.append({
            "name": path.name,
            "size": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            "active": path.name in {"app.log", "error.log"},
        })
    return files


def resolve_log_file(log_dir: str | Path, name: str) -> Path:
    """Resolve a user-supplied log name without allowing path traversal."""
    if not name or not _LOG_NAME_RE.match(name):
        raise ValueError("Invalid log filename")
    root = Path(log_dir).resolve()
    path = (root / name).resolve()
    if root not in path.parents and path != root:
        raise ValueError("Invalid log filename")
    if not path.is_file() or not path.name.startswith(("app.log", "error.log")):
        raise FileNotFoundError(name)
    return path


def read_log_tail(log_dir: str | Path, name: str, *, lines: int = 300) -> str:
    """Read the last N lines of a log file."""
    path = resolve_log_file(log_dir, name)
    limit = max(1, min(int(lines), 5000))
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        return "".join(deque(fh, maxlen=limit))
