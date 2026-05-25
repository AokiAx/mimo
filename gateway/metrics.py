"""
SQLite-backed metrics for gateway requests.

Two consumers write here:

* The legacy ``gateway.proxy.proxy_request`` calls ``record_request``.
* The new pipeline (``gateway.handler.GatewayHandler``) accepts a
  ``MetricsRecorder`` protocol; ``SQLiteMetricsRecorder`` below is the
  implementation we plug in.

The schema is one row per finished request. It carries enough columns to
build the panel's 24h dashboard, per-backend stats, and the public
total-tokens page without re-reading raw payloads.
"""
from __future__ import annotations

import queue
import sqlite3
import threading
import time
from typing import Any

from project_paths import METRICS_DB_PATH

DB_PATH = METRICS_DB_PATH


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


_local = threading.local()


def _get_thread_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = _get_conn()
        _init_db(_local.conn)
    return _local.conn


_BASE_COLUMNS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "ts": "REAL NOT NULL",
    "method": "TEXT NOT NULL DEFAULT ''",
    "path": "TEXT NOT NULL DEFAULT ''",
    "backend_id": "TEXT DEFAULT ''",
    "status_code": "INTEGER DEFAULT 0",
    "latency_ms": "REAL DEFAULT 0",
    "source_format": "TEXT DEFAULT ''",
    "is_stream": "INTEGER DEFAULT 0",
    "error": "TEXT DEFAULT ''",
    # Added in the metrics-aggregation refactor:
    "prompt_tokens": "INTEGER DEFAULT 0",
    "completion_tokens": "INTEGER DEFAULT 0",
    "model": "TEXT DEFAULT ''",
    "request_id": "TEXT DEFAULT ''",
}


def _init_db(conn: sqlite3.Connection) -> None:
    cols_sql = ",\n            ".join(f"{k} {v}" for k, v in _BASE_COLUMNS.items())
    conn.execute(f"CREATE TABLE IF NOT EXISTS requests (\n            {cols_sql}\n        )")
    # Migrate older databases: add any columns introduced after creation.
    existing = {row[1] for row in conn.execute("PRAGMA table_info(requests)").fetchall()}
    for name, decl in _BASE_COLUMNS.items():
        if name not in existing:
            ddl = decl.replace("PRIMARY KEY AUTOINCREMENT", "")
            conn.execute(f"ALTER TABLE requests ADD COLUMN {name} {ddl}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON requests(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_backend_ts ON requests(backend_id, ts)")
    conn.commit()


DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_init_db(_get_conn())


# ───────── writers ─────────


def _insert_records(records: list[dict[str, Any]]) -> None:
    if not records:
        return
    conn = _get_thread_conn()
    conn.executemany(
        "INSERT INTO requests (ts, method, path, backend_id, status_code, "
        "latency_ms, source_format, is_stream, error, prompt_tokens, "
        "completion_tokens, model, request_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r["ts"], r["method"], r["path"], r["backend_id"], r["status_code"],
                r["latency_ms"], r["source_format"], int(r["is_stream"]), r["error"],
                int(r["prompt_tokens"]), int(r["completion_tokens"]),
                r["model"], r["request_id"],
            )
            for r in records
        ],
    )
    conn.commit()


def _request_record(
    method: str,
    path: str,
    backend_id: str = "",
    status_code: int = 0,
    latency_ms: float = 0,
    source_format: str = "",
    is_stream: bool = False,
    error: str = "",
    *,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    model: str = "",
    request_id: str = "",
) -> dict[str, Any]:
    return {
        "ts": time.time(),
        "method": method,
        "path": path,
        "backend_id": backend_id,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "source_format": source_format,
        "is_stream": is_stream,
        "error": error,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "model": model,
        "request_id": request_id,
    }


def record_request(
    method: str,
    path: str,
    backend_id: str = "",
    status_code: int = 0,
    latency_ms: float = 0,
    source_format: str = "",
    is_stream: bool = False,
    error: str = "",
    *,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    model: str = "",
    request_id: str = "",
) -> None:
    """Record a request to SQLite synchronously. Failures are swallowed."""
    try:
        _insert_records([
            _request_record(
                method, path, backend_id, status_code, latency_ms, source_format,
                is_stream, error, prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens, model=model,
                request_id=request_id,
            )
        ])
    except Exception:
        pass


class SQLiteMetricsRecorder:
    """Implements the ``MetricsRecorder`` protocol used by ``GatewayHandler``.

    The handler hands us a ``RequestContext`` plus per-call counters; we
    flatten that to one row in ``requests``. We never raise — metrics
    failure must not break the request path.
    """

    def record(
        self,
        *,
        ctx: Any,
        backend_id: str,
        status_code: int,
        latency_ms: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: str = "",
    ) -> None:
        record_request(
            method=getattr(ctx, "src_method", "POST") or "POST",
            path=getattr(ctx, "src_path", "") or "",
            backend_id=backend_id,
            status_code=status_code,
            latency_ms=latency_ms,
            source_format=getattr(ctx, "src_protocol", "") or "",
            is_stream=bool(getattr(ctx, "is_stream", False)),
            error=error,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=getattr(ctx, "model", "") or "",
            request_id=getattr(ctx, "request_id", "") or "",
        )


class QueuedSQLiteMetricsRecorder:
    """Non-blocking metrics recorder for the request path.

    ``record`` extracts primitive values from the RequestContext and enqueues a
    row for a daemon worker. The worker batches inserts so SQLite commits no
    longer sit directly on the gateway hot path. If the queue is full, metrics
    are dropped rather than delaying user requests.
    """

    def __init__(
        self,
        *,
        max_queue: int = 10_000,
        batch_size: int = 100,
        flush_interval_s: float = 0.25,
    ):
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=max_queue)
        self._batch_size = max(1, int(batch_size))
        self._flush_interval_s = max(0.01, float(flush_interval_s))
        self._dropped = 0
        self._closed = False
        self._thread = threading.Thread(
            target=self._worker, name="mimo-metrics-writer", daemon=True,
        )
        self._thread.start()

    @property
    def dropped(self) -> int:
        return self._dropped

    def record(
        self,
        *,
        ctx: Any,
        backend_id: str,
        status_code: int,
        latency_ms: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: str = "",
    ) -> None:
        if self._closed:
            return
        row = _request_record(
            method=getattr(ctx, "src_method", "POST") or "POST",
            path=getattr(ctx, "src_path", "") or "",
            backend_id=backend_id,
            status_code=status_code,
            latency_ms=latency_ms,
            source_format=getattr(ctx, "src_protocol", "") or "",
            is_stream=bool(getattr(ctx, "is_stream", False)),
            error=error,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=getattr(ctx, "model", "") or "",
            request_id=getattr(ctx, "request_id", "") or "",
        )
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            self._dropped += 1

    def flush(self, timeout_s: float = 2.0) -> None:
        """Block until queued rows are written or timeout elapses."""
        deadline = time.monotonic() + timeout_s
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.01)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            # Make room for the sentinel by dropping one pending row.
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                pass
            self._queue.put_nowait(None)
        self._thread.join(timeout=2.0)

    def _worker(self) -> None:
        batch: list[dict[str, Any]] = []
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                if batch:
                    self._write_batch(batch)
                    batch = []
                break
            batch.append(item)
            self._queue.task_done()

            # Drain more rows without waiting so bursts become one SQLite commit.
            while len(batch) < self._batch_size:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    self._queue.task_done()
                    self._write_batch(batch)
                    return
                batch.append(item)
                self._queue.task_done()

            if len(batch) >= self._batch_size:
                self._write_batch(batch)
                batch = []
            else:
                # Bound the flush delay for low-QPS deployments.
                time.sleep(self._flush_interval_s)
                if batch:
                    self._write_batch(batch)
                    batch = []

    @staticmethod
    def _write_batch(batch: list[dict[str, Any]]) -> None:
        try:
            _insert_records(batch)
        except Exception:
            pass


# ───────── readers ─────────


def _percentile(conn: sqlite3.Connection, since: float, pct: float) -> float:
    """Crude SQL-only percentile (good enough for a dashboard)."""
    total = conn.execute(
        "SELECT COUNT(*) FROM requests WHERE ts > ? AND latency_ms > 0",
        (since,),
    ).fetchone()[0]
    if not total:
        return 0.0
    offset = max(int(total * pct) - 1, 0)
    row = conn.execute(
        "SELECT latency_ms FROM requests WHERE ts > ? AND latency_ms > 0 "
        "ORDER BY latency_ms LIMIT 1 OFFSET ?",
        (since, offset),
    ).fetchone()
    return float(row[0]) if row else 0.0


def get_metrics_summary() -> dict:
    """Top-line stats + recent requests for the panel's metrics page."""
    try:
        conn = _get_thread_conn()
        now = time.time()
        since = now - 86400

        row = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN status_code BETWEEN 200 AND 399 THEN 1 ELSE 0 END), "
            "AVG(latency_ms), SUM(prompt_tokens), SUM(completion_tokens) "
            "FROM requests WHERE ts > ?",
            (since,),
        ).fetchone()
        total_24h = row[0] or 0
        success_24h = row[1] or 0
        avg_latency = round(row[2] or 0, 1)
        prompt_tokens_24h = row[3] or 0
        completion_tokens_24h = row[4] or 0
        success_rate = round(success_24h / max(total_24h, 1) * 100, 1)
        errors_24h = total_24h - success_24h
        p95_latency = round(_percentile(conn, since, 0.95), 1)
        p99_latency = round(_percentile(conn, since, 0.99), 1)

        recent = []
        for r in conn.execute(
            "SELECT ts, method, path, backend_id, status_code, latency_ms, "
            "source_format, is_stream, error, prompt_tokens, completion_tokens, model "
            "FROM requests ORDER BY ts DESC LIMIT 50"
        ).fetchall():
            recent.append({
                "ts": r[0],
                "time": time.strftime("%H:%M:%S", time.localtime(r[0])),
                "method": r[1],
                "path": r[2],
                "backend": r[3] or "",
                "status": r[4],
                "latency_ms": round(r[5] or 0, 1),
                "format": r[6] or "",
                "stream": bool(r[7]),
                "error": r[8] or "",
                "prompt_tokens": r[9] or 0,
                "completion_tokens": r[10] or 0,
                "model": r[11] or "",
            })

        return {
            "total_24h": total_24h,
            "success_rate": success_rate,
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "p99_latency_ms": p99_latency,
            "errors_24h": errors_24h,
            "prompt_tokens_24h": prompt_tokens_24h,
            "completion_tokens_24h": completion_tokens_24h,
            "total_tokens_24h": prompt_tokens_24h + completion_tokens_24h,
            "recent": recent,
        }
    except Exception as e:
        return {
            "total_24h": 0,
            "success_rate": 0,
            "avg_latency_ms": 0,
            "p95_latency_ms": 0,
            "p99_latency_ms": 0,
            "errors_24h": 0,
            "prompt_tokens_24h": 0,
            "completion_tokens_24h": 0,
            "total_tokens_24h": 0,
            "recent": [],
            "error": str(e),
        }


def get_hourly_buckets(hours: int = 24) -> list[dict]:
    """24 (or N) one-hour buckets ending now, oldest first.

    Each bucket has count / errors / avg latency / token totals so the panel
    can paint a histogram without doing math in JS.
    """
    try:
        conn = _get_thread_conn()
        now = time.time()
        cutoff = now - hours * 3600
        rows = conn.execute(
            "SELECT CAST((? - ts) / 3600 AS INTEGER) AS bucket, "
            "COUNT(*), "
            "SUM(CASE WHEN status_code BETWEEN 200 AND 399 THEN 0 ELSE 1 END), "
            "AVG(latency_ms), "
            "SUM(prompt_tokens), SUM(completion_tokens) "
            "FROM requests WHERE ts > ? GROUP BY bucket",
            (now, cutoff),
        ).fetchall()
        by_bucket = {int(r[0]): r for r in rows}
        out: list[dict] = []
        for h in range(hours - 1, -1, -1):
            r = by_bucket.get(h)
            bucket_end = now - h * 3600
            out.append({
                "hour": time.strftime("%H:00", time.localtime(bucket_end)),
                "ts": bucket_end,
                "count": r[1] if r else 0,
                "errors": r[2] if r else 0,
                "avg_latency_ms": round(r[3] or 0, 1) if r else 0,
                "prompt_tokens": r[4] or 0 if r else 0,
                "completion_tokens": r[5] or 0 if r else 0,
            })
        return out
    except Exception:
        return []


def get_backend_stats(hours: int = 24) -> list[dict]:
    """Per-backend rollup: counts, success rate, avg latency, tokens."""
    try:
        conn = _get_thread_conn()
        since = time.time() - hours * 3600
        rows = conn.execute(
            "SELECT backend_id, COUNT(*), "
            "SUM(CASE WHEN status_code BETWEEN 200 AND 399 THEN 1 ELSE 0 END), "
            "AVG(latency_ms), SUM(prompt_tokens), SUM(completion_tokens) "
            "FROM requests WHERE ts > ? AND backend_id != '' "
            "GROUP BY backend_id ORDER BY 2 DESC",
            (since,),
        ).fetchall()
        out = []
        for r in rows:
            total = r[1] or 0
            success = r[2] or 0
            out.append({
                "backend_id": r[0],
                "total": total,
                "success": success,
                "errors": total - success,
                "success_rate": round(success / max(total, 1) * 100, 1),
                "avg_latency_ms": round(r[3] or 0, 1),
                "prompt_tokens": r[4] or 0,
                "completion_tokens": r[5] or 0,
                "total_tokens": (r[4] or 0) + (r[5] or 0),
            })
        return out
    except Exception:
        return []


def get_status_distribution(hours: int = 24) -> dict[str, int]:
    """Histogram of HTTP status codes in the window."""
    try:
        conn = _get_thread_conn()
        since = time.time() - hours * 3600
        rows = conn.execute(
            "SELECT status_code, COUNT(*) FROM requests WHERE ts > ? "
            "GROUP BY status_code ORDER BY status_code",
            (since,),
        ).fetchall()
        return {str(int(r[0])): int(r[1]) for r in rows}
    except Exception:
        return {}


def get_public_totals() -> dict:
    """All-time totals safe to expose on a public stats page."""
    try:
        conn = _get_thread_conn()
        row = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN status_code BETWEEN 200 AND 399 THEN 1 ELSE 0 END), "
            "SUM(prompt_tokens), SUM(completion_tokens), MIN(ts) "
            "FROM requests"
        ).fetchone()
        total = row[0] or 0
        success = row[1] or 0
        prompt = row[2] or 0
        completion = row[3] or 0
        first_ts = row[4] or time.time()
        return {
            "total_requests": total,
            "successful_requests": success,
            "success_rate": round(success / max(total, 1) * 100, 2),
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
            "since_ts": first_ts,
            "since": time.strftime("%Y-%m-%d", time.localtime(first_ts)),
        }
    except Exception:
        return {
            "total_requests": 0, "successful_requests": 0, "success_rate": 0,
            "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
            "since_ts": 0, "since": "",
        }
