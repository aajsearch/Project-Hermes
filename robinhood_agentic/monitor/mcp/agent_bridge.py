"""Call Robinhood MCP tools via `cursor agent` when CLI MCP is authenticated."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from .auth import mcp_cli_authenticated


class CursorAgentMcpBridge:
    """Deterministic MCP tool calls through cursor agent CLI (no LLM planning)."""

    def __init__(self, repo_root: Path | None = None, server: str = "robinhood-trading") -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[3]
        self.server = server

    def available(self) -> bool:
        return mcp_cli_authenticated(self.server)

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        args_json = json.dumps(arguments, separators=(",", ":"))
        prompt = (
            f"ROBINHOOD MCP DIRECT TOOL CALL. Server: {self.server}. "
            f"Call MCP tool {name!r} with arguments exactly: {args_json}. "
            f"Reply with ONLY the raw JSON tool result object. No markdown, no prose."
        )
        cmd = [
            "cursor", "agent", "-p", "--trust", "--approve-mcps", "--yolo",
            "--output-format", "text",
            prompt,
        ]
        result = subprocess.run(
            cmd,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()
        if result.returncode != 0 and not out:
            raise RuntimeError(err or f"cursor agent exit {result.returncode}")
        parsed = self._extract_json(out)
        if parsed is None:
            raise RuntimeError(f"no JSON in agent output: {out[:500]}")
        return parsed

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any] | None:
        """Parse first complete JSON object; tolerate trailing/truncated noise."""
        text = text.strip()
        if not text:
            return None
        decoder = json.JSONDecoder()
        # Prefer start-of-string / first '{' onward via raw_decode.
        for start in [0] + [m.start() for m in re.finditer(r"\{", text)]:
            try:
                obj, _ = decoder.raw_decode(text[start:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                continue
        return None
