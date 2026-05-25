"""Canonical filesystem paths for the MiMo manager.

This module names the existing repo layout without changing where runtime
files live.
"""
from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

ACCOUNTS_DIR = BASE_DIR / "accounts"
CLAW_DIR = BASE_DIR / "claw"
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
PROBE_DIR = BASE_DIR / "probe"
TEMPLATES_DIR = BASE_DIR / "templates"
TMP_DIR = BASE_DIR / "tmp"

CURRENT_ACCOUNT_FILE = ACCOUNTS_DIR / "_current.json"

API_KEYS_DB_PATH = DATA_DIR / "api_keys.db"
AUTO_DEPLOY_CONFIG_PATH = DATA_DIR / "auto_deploy.json"
BACKENDS_PATH = DATA_DIR / "backends.json"
DEPLOY_HISTORY_DIR = DATA_DIR / "deploy_history"
DEPLOY_LOG_DIR = DATA_DIR / "deploy_logs"
METRICS_DB_PATH = DATA_DIR / "metrics.db"
MODEL_GROUPS_PATH = DATA_DIR / "model_groups.json"
PROBE_NODES_PATH = DATA_DIR / "probe_nodes.json"
REASONING_CACHE_DB_PATH = DATA_DIR / "reasoning_cache.db"
SECRETS_PATH = DATA_DIR / "secrets.json"

AUTH_CONFIG_PATH = TMP_DIR / "mimo_auth_config.json"
CLAW_PAYLOAD_DIR = CLAW_DIR / "payload"
