#!/usr/bin/env python3
"""
Run Kalshi WebSocket briefly and print [kalshi_ws] logs plus env/cryptography status.
Usage: python tools/run_kalshi_ws_diagnostic.py
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env")

# Logging to stdout so we see all [kalshi_ws] lines
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

def main() -> None:
    api_key = os.environ.get("KALSHI_API_KEY")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY")
    print("--- Env ---")
    print("KALSHI_API_KEY: set" if (api_key and api_key.strip()) else "KALSHI_API_KEY: missing")
    if key_path and key_path.strip():
        if key_path.strip().startswith("-----BEGIN"):
            print("KALSHI_PRIVATE_KEY: set (inline PEM)")
        else:
            exists = Path(key_path).exists() if key_path else False
            print("KALSHI_PRIVATE_KEY: set (path=%s, exists=%s)" % (key_path[:60] + ("..." if len(key_path) > 60 else ""), exists))
    else:
        print("KALSHI_PRIVATE_KEY: missing")
    print("--- cryptography ---")
    try:
        import cryptography
        print("cryptography: installed version %s" % getattr(cryptography, "__version__", "?"))
    except ImportError:
        print("cryptography: not installed")
    print("--- Starting Kalshi WS for 12s ---")
    from bot.kalshi_ws_manager import start_kalshi_ws, stop_kalshi_ws, subscribe_to_tickers
    start_kalshi_ws()
    subscribe_to_tickers(["KXBTC15M-26MAR141400"])
    time.sleep(12)
    stop_kalshi_ws()
    print("--- Done ---")

if __name__ == "__main__":
    main()
