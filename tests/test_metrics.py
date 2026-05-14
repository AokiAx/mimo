"""Unit tests for gateway.metrics aggregation layer."""
from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
from dataclasses import dataclass

import pytest


@pytest.fixture
def metrics_module(tmp_path, monkeypatch):
    """Load gateway.metrics against a throwaway SQLite DB."""
    db = tmp_path / "metrics.db"
    monkeypatch.setattr("os.environ", os.environ)
    sys.modules.pop("gateway.metrics", None)
    import gateway.metrics as m
    importlib.reload(m)
    m._local.conn = None
    m.DB_PATH = db
    db.parent.mkdir(parents=True, exist_ok=True)
    m._init_db(m._get_conn())
    yield m
    if hasattr(m._local, "conn") and m._local.conn is not None:
        try:
            m._local.conn.close()
        except Exception:
            pass
        m._local.conn = None


@dataclass
class _Ctx:
    request_id: str = "rid"
    src_method: str = "POST"
    src_path: str = "/v1/chat/completions"
    src_protocol: str = "openai_chat"
    is_stream: bool = False
    model: str = "MiMo-VL-7B-RL-2508"


def _seed_basic(m):
    for i in range(5):
        m.record_request(
            "POST", "/v1/chat/completions", backend_id="b1",
            status_code=200, latency_ms=100 + i * 10,
            source_format="openai", is_stream=False,
            prompt_tokens=10, completion_tokens=20, model="m",
        )
    m.record_request(
        "POST", "/v1/messages", backend_id="b2",
        status_code=500, latency_ms=300, error="upstream",
        prompt_tokens=5, completion_tokens=0,
    )


def test_record_request_persists_token_columns(metrics_module):
    m = metrics_module
    m.record_request(
        "POST", "/v1/chat/completions", backend_id="b1",
        status_code=200, latency_ms=42.5,
        prompt_tokens=11, completion_tokens=22,
        model="model-x", request_id="rid-1",
    )
    conn = m._get_thread_conn()
    row = conn.execute(
        "SELECT prompt_tokens, completion_tokens, model, request_id "
        "FROM requests WHERE backend_id=?", ("b1",),
    ).fetchone()
    assert row == (11, 22, "model-x", "rid-1")


def test_summary_counts_24h_and_tokens(metrics_module):
    _seed_basic(metrics_module)
    s = metrics_module.get_metrics_summary()
    assert s["total_24h"] == 6
    assert s["errors_24h"] == 1
    assert s["success_rate"] == pytest.approx(83.3, abs=0.1)
    # 5 * (10+20) + 5 = 155
    assert s["total_tokens_24h"] == 155
    assert s["prompt_tokens_24h"] == 55
    assert s["completion_tokens_24h"] == 100
    assert len(s["recent"]) == 6


def test_summary_treats_3xx_as_success(metrics_module):
    m = metrics_module
    m.record_request("POST", "/v1/chat/completions", status_code=204)
    m.record_request("POST", "/v1/chat/completions", status_code=302)
    m.record_request("POST", "/v1/chat/completions", status_code=400)
    m.record_request("POST", "/v1/chat/completions", status_code=500)
    s = m.get_metrics_summary()
    assert s["total_24h"] == 4
    assert s["errors_24h"] == 2
    assert s["success_rate"] == 50.0


def test_hourly_buckets_returns_full_window(metrics_module):
    m = metrics_module
    m.record_request("POST", "/x", backend_id="b1", status_code=200,
                     latency_ms=50, prompt_tokens=1, completion_tokens=2)
    buckets = m.get_hourly_buckets(hours=24)
    assert len(buckets) == 24
    # Most-recent bucket carries the row we just wrote
    assert buckets[-1]["count"] == 1
    assert buckets[-1]["prompt_tokens"] == 1
    assert buckets[-1]["completion_tokens"] == 2
    # Older buckets are empty placeholders, not missing
    assert buckets[0]["count"] == 0


def test_backend_stats_groups_per_backend(metrics_module):
    _seed_basic(metrics_module)
    stats = {b["backend_id"]: b for b in metrics_module.get_backend_stats()}
    assert stats["b1"]["total"] == 5
    assert stats["b1"]["success_rate"] == 100.0
    assert stats["b1"]["total_tokens"] == 150
    assert stats["b2"]["total"] == 1
    assert stats["b2"]["success_rate"] == 0.0
    assert stats["b2"]["errors"] == 1


def test_backend_stats_excludes_empty_backend_id(metrics_module):
    m = metrics_module
    m.record_request("POST", "/x", backend_id="", status_code=200, latency_ms=10)
    m.record_request("POST", "/x", backend_id="b1", status_code=200, latency_ms=10)
    stats = metrics_module.get_backend_stats()
    assert {b["backend_id"] for b in stats} == {"b1"}


def test_status_distribution(metrics_module):
    _seed_basic(metrics_module)
    dist = metrics_module.get_status_distribution()
    assert dist == {"200": 5, "500": 1}


def test_public_totals_aggregates_all_time(metrics_module):
    _seed_basic(metrics_module)
    p = metrics_module.get_public_totals()
    assert p["total_requests"] == 6
    assert p["successful_requests"] == 5
    assert p["total_tokens"] == 155
    assert p["since"]  # non-empty date string


def test_sqlite_recorder_writes_via_protocol(metrics_module):
    m = metrics_module
    rec = m.SQLiteMetricsRecorder()
    ctx = _Ctx()
    rec.record(
        ctx=ctx, backend_id="b1", status_code=200,
        latency_ms=33.3, prompt_tokens=7, completion_tokens=14,
    )
    s = m.get_metrics_summary()
    assert s["total_24h"] == 1
    assert s["recent"][0]["backend"] == "b1"
    assert s["recent"][0]["model"] == ctx.model
    assert s["recent"][0]["format"] == "openai_chat"


def test_init_migrates_legacy_schema(tmp_path):
    """Old DB with only the original columns still upgrades cleanly."""
    db = tmp_path / "legacy.db"
    import sqlite3
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            backend_id TEXT,
            status_code INTEGER,
            latency_ms REAL,
            source_format TEXT,
            is_stream INTEGER DEFAULT 0,
            error TEXT
        )
    """)
    conn.execute(
        "INSERT INTO requests (ts, method, path, backend_id, status_code, "
        "latency_ms, source_format, is_stream, error) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (time.time(), "POST", "/v1/chat/completions", "b1", 200, 50.0,
         "openai", 0, ""),
    )
    conn.commit()
    conn.close()

    sys.modules.pop("gateway.metrics", None)
    import gateway.metrics as m
    importlib.reload(m)
    m._local.conn = None
    m.DB_PATH = db
    fresh = m._get_conn()
    m._init_db(fresh)
    cols = {r[1] for r in fresh.execute("PRAGMA table_info(requests)").fetchall()}
    assert {"prompt_tokens", "completion_tokens", "model", "request_id"} <= cols
    fresh.close()


def test_queued_sqlite_recorder_writes_in_background(metrics_module):
    m = metrics_module
    rec = m.QueuedSQLiteMetricsRecorder(batch_size=10, flush_interval_s=0.01)
    ctx = _Ctx()
    rec.record(
        ctx=ctx, backend_id="queued", status_code=200,
        latency_ms=12.3, prompt_tokens=3, completion_tokens=4,
    )
    rec.close()
    s = m.get_metrics_summary()
    assert s["total_24h"] == 1
    assert s["recent"][0]["backend"] == "queued"
    assert s["recent"][0]["prompt_tokens"] == 3
    assert s["recent"][0]["completion_tokens"] == 4
