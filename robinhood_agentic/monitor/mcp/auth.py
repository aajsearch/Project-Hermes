"""Resolve MCP bearer token for headless monitor execution."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

MONITOR_DIR = Path(__file__).resolve().parent.parent
TOKEN_FILE = MONITOR_DIR / ".mcp_access_token"
ENV_KEYS = (
    "ROBINHOOD_MCP_ACCESS_TOKEN",
    "MCP_ACCESS_TOKEN",
    "ROBINHOOD_MCP_BEARER_TOKEN",
)


def load_access_token() -> str | None:
    for key in ENV_KEYS:
        val = os.environ.get(key, "").strip()
        if val:
            return val
    if TOKEN_FILE.is_file():
        token = TOKEN_FILE.read_text().strip()
        if token:
            return token
    return None


def mcp_cli_authenticated(server: str = "robinhood-trading") -> bool:
    """True if `cursor agent mcp list` shows the server connected."""
    try:
        result = subprocess.run(
            ["cursor", "agent", "mcp", "list"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        out = (result.stdout or "") + (result.stderr or "")
        for line in out.splitlines():
            if server in line and "connected" in line.lower():
                return True
            if server in line and "requires_authentication" not in line.lower():
                if "error" not in line.lower() and "login" not in line.lower():
                    return True
    except (OSError, subprocess.TimeoutExpired):
        pass
    return False


def auth_status_message(server: str = "robinhood-trading") -> str:
    if load_access_token():
        return "token: env or .mcp_access_token (direct HTTP)"
    if mcp_cli_authenticated(server):
        return f"cursor agent mcp: {server} ready (agent bridge)"
    return (
        f"no token — run: cursor agent mcp login {server}\n"
        f"  then export token to {TOKEN_FILE} (see monitor/MCP_AUTH.md)"
    )
