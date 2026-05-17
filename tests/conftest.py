"""Test-wide fixtures.

Currently focused on isolating the persistent reasoning cache: every test
gets its own SQLite file under ``tmp_path``, so cases don't leak state
between each other and ``data/reasoning_cache.db`` in the working tree is
never touched during a test run.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolated_reasoning_cache(tmp_path, monkeypatch):
    """Point the reasoning cache at a per-test SQLite file and tear it down.

    Autouse so every test gets isolation without having to opt in. The cost
    is one ``reset_for_tests`` call per test, which is microseconds when the
    writer thread hasn't started (the common case).
    """
    monkeypatch.setenv(
        "MIMO_REASONING_CACHE_DB", str(tmp_path / "reasoning_cache.db")
    )
    # Reset before each test in case a prior test in the same process left
    # state behind (the env var pointing at the old tmp_path would already
    # be gone, but the in-memory dict and writer thread might persist).
    from gateway.reasoning_cache import reset_for_tests
    reset_for_tests()
    yield
    reset_for_tests()
