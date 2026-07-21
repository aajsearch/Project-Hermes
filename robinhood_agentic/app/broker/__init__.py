"""Broker adapter — wraps existing monitor MCP executor."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from ..paths import MONITOR_DIR, REPO_ROOT


def _ensure_monitor_path() -> None:
    p = str(MONITOR_DIR)
    if p not in sys.path:
        sys.path.insert(0, p)


def mcp_available() -> tuple[bool, str]:
    _ensure_monitor_path()
    try:
        from mcp.auth import auth_status_message, load_access_token, mcp_cli_authenticated

        if load_access_token():
            return True, "token"
        if mcp_cli_authenticated():
            return True, "cursor_agent_mcp"
        return False, auth_status_message()
    except Exception as e:
        return False, str(e)


def get_trade_actions():
    _ensure_monitor_path()
    from mcp.executor import TradeActions

    return TradeActions()


def sync_state_file(state: dict[str, Any], state_path: Path) -> None:
    """Write DB-shaped state to session_state.json for TradeActions compatibility."""
    import json

    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def probe_mcp() -> tuple[bool, str]:
    """Fast health check — never construct TradeActions (can block on bridge)."""
    return mcp_available()


REPO = REPO_ROOT
