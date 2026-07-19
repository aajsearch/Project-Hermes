"""Load MCP server configuration from .cursor/mcp.json."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class McpServerConfig:
    name: str
    url: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_mcp_config(
    server_name: str = "robinhood-trading",
    mcp_json: Path | None = None,
) -> McpServerConfig:
    path = mcp_json or (_repo_root() / ".cursor" / "mcp.json")
    if not path.is_file():
        fallback = _repo_root() / "robinhood_agentic" / "config" / "mcp.cursor.json"
        path = fallback if fallback.is_file() else path
    data = json.loads(path.read_text())
    servers = data.get("mcpServers", {})
    if server_name not in servers:
        raise KeyError(f"MCP server {server_name!r} not in {path}")
    entry = servers[server_name]
    url = entry.get("url")
    if not url:
        raise ValueError(f"MCP server {server_name!r} has no url (stdio servers unsupported)")
    return McpServerConfig(name=server_name, url=url.rstrip("/"))
