#!/usr/bin/env python3
"""CLI for direct MCP tool calls (monitor automation / debugging)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MONITOR_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(MONITOR_DIR))

from mcp.auth import auth_status_message, load_access_token  # noqa: E402
from mcp.executor import McpExecutor, TradeActions  # noqa: E402
from mcp.http_client import McpHttpError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Direct Robinhood MCP executor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Show MCP auth status")

    call_p = sub.add_parser("call", help="Call any MCP tool")
    call_p.add_argument("tool", help="MCP tool name")
    call_p.add_argument("arguments", help="JSON arguments object")

    exit_p = sub.add_parser("exit", help="Execute synthetic SL/TP exit from alert string")
    exit_p.add_argument("alert", help="e.g. SL_HIT:GOOGL:...")
    exit_p.add_argument(
        "--state",
        type=Path,
        default=MONITOR_DIR / "session_state.json",
    )

    args = parser.parse_args()

    if args.cmd == "status":
        print(auth_status_message())
        print(f"token_loaded={bool(load_access_token())}")
        return 0

    try:
        if args.cmd == "call":
            arguments = json.loads(args.arguments)
            result = McpExecutor().call(args.tool, arguments)
            print(json.dumps(result, indent=2))
            return 0

        if args.cmd == "exit":
            state = json.loads(args.state.read_text())
            ok = TradeActions().execute_synthetic_exit(args.alert, state, args.state)
            print("OK" if ok else "FAILED")
            return 0 if ok else 1
    except (McpHttpError, json.JSONDecodeError, KeyError, OSError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
