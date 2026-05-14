from __future__ import annotations

import logging

import pytest

from gateway.logging_setup import (
    cleanup_old_logs,
    list_log_files,
    read_log_tail,
    resolve_log_file,
    setup_logging,
)


def test_setup_logging_writes_rotated_files_to_configured_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("MIMO_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("MIMO_LOG_RETENTION_DAYS", "3")

    log_dir = setup_logging(tmp_path)
    logging.getLogger("tests.logging").error("sample error for test")
    for handler in logging.getLogger().handlers:
        handler.flush()

    assert log_dir == tmp_path
    assert (tmp_path / "app.log").exists()
    assert (tmp_path / "error.log").exists()
    assert "sample error for test" in read_log_tail(tmp_path, "error.log", lines=20)


def test_list_and_tail_logs_are_restricted_to_log_directory(tmp_path):
    (tmp_path / "app.log").write_text("one\ntwo\nthree\n", encoding="utf-8")
    (tmp_path / "error.log").write_text("bad\n", encoding="utf-8")
    (tmp_path / "other.txt").write_text("hidden\n", encoding="utf-8")

    names = {item["name"] for item in list_log_files(tmp_path)}
    assert names == {"app.log", "error.log"}
    assert read_log_tail(tmp_path, "app.log", lines=2) == "two\nthree\n"

    with pytest.raises(ValueError):
        resolve_log_file(tmp_path, "../app.log")
    with pytest.raises(FileNotFoundError):
        resolve_log_file(tmp_path, "other.txt")


def test_cleanup_old_logs_keeps_recent_rotations(tmp_path):
    for i in range(4):
        path = tmp_path / f"app.log.2026-05-1{i}"
        path.write_text(str(i), encoding="utf-8")
        # Ensure deterministic ordering by modification time.
        path.touch()

    removed = cleanup_old_logs(tmp_path, retention_days=2)

    assert removed == 2
    assert len(list(tmp_path.glob("app.log.*"))) == 2
