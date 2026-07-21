"""CLI entry: python -m robinhood_agentic.app"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure package imports work when run as module
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Robinhood Agentic Command Center")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("Install deps: pip install -r robinhood_agentic/requirements-app.txt", file=sys.stderr)
        return 1

    from robinhood_agentic.app.paths import DB_PATH
    from robinhood_agentic.app.api.app import create_app

    db = args.db or DB_PATH
    app = create_app(db)

    print(f"Command Center → http://{args.host}:{args.port}")
    print("IMPORTANT: Stop session_monitor.py before using the engine (single account writer).")
    # Note: uvicorn.run blocks until shutdown
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
