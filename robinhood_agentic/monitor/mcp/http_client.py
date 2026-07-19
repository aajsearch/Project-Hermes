"""Minimal MCP Streamable HTTP client (stdlib only)."""

from __future__ import annotations

import json
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class McpHttpError(RuntimeError):
    pass


class McpHttpClient:
    def __init__(self, base_url: str, access_token: str, timeout: float = 45.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.timeout = timeout
        self.session_id: str | None = None
        self._req_id = 0

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {self.access_token}",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        return headers

    def _parse_body(self, raw: str) -> dict[str, Any]:
        raw = raw.strip()
        if not raw:
            raise McpHttpError("empty MCP response")
        if raw.startswith("{"):
            return json.loads(raw)
        # SSE: event: message\ndata: {...}
        for line in raw.splitlines():
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload:
                    return json.loads(payload)
        raise McpHttpError(f"unparseable MCP response: {raw[:300]}")

    def _request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(
            {"jsonrpc": "2.0", "id": self._next_id(), "method": method, "params": params or {}}
        ).encode()
        req = Request(self.base_url, data=body, headers=self._headers(), method="POST")
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                session = resp.headers.get("Mcp-Session-Id") or resp.headers.get("mcp-session-id")
                if session:
                    self.session_id = session
                raw = resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
            raise McpHttpError(f"MCP HTTP {e.code}: {detail}") from e
        except URLError as e:
            raise McpHttpError(f"MCP network error: {e}") from e

        msg = self._parse_body(raw)
        if "error" in msg:
            err = msg["error"]
            raise McpHttpError(f"MCP {method} error: {err}")
        return msg.get("result", msg)

    def initialize(self) -> dict[str, Any]:
        return self._request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "hades-monitor", "version": "1.0"},
            },
        )

    def notify_initialized(self) -> None:
        body = json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        ).encode()
        req = Request(self.base_url, data=body, headers=self._headers(), method="POST")
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                resp.read()
        except (HTTPError, URLError):
            pass

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if not self.session_id:
            self.initialize()
            self.notify_initialized()
        result = self._request("tools/call", {"name": name, "arguments": arguments})
        if isinstance(result, dict) and result.get("isError"):
            texts = []
            for block in result.get("content") or []:
                if isinstance(block, dict) and block.get("type") == "text":
                    texts.append(block.get("text", ""))
            raise McpHttpError("; ".join(texts) or f"tool {name} failed")
        return result
