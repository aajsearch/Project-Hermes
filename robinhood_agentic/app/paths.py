"""Shared paths for the Command Center app."""

from __future__ import annotations

from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = APP_DIR.parent  # robinhood_agentic/
REPO_ROOT = PACKAGE_ROOT.parent
DATA_DIR = PACKAGE_ROOT / "data"
DB_PATH = DATA_DIR / "command_center.db"
CONFIG_DIR = PACKAGE_ROOT / "config"
MONITOR_DIR = PACKAGE_ROOT / "monitor"
TECH_YAML = CONFIG_DIR / "tech_scalper.yaml"
OPTIONS_YAML = CONFIG_DIR / "options_directional.yaml"
SESSION_STATE_JSON = MONITOR_DIR / "session_state.json"
UI_DIST = APP_DIR / "ui" / "dist"
