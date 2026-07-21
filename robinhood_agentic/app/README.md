# Command Center App

Python-native control plane for the Robinhood Agentic tech scalper + options playbooks.

**Do not run** `monitor/session_monitor.py` at the same time as this app — both would write positions for the same account.

## Quick start

```bash
cd /path/to/Hades-prediction-market
python3 -m venv robinhood_agentic/.venv
source robinhood_agentic/.venv/bin/activate
pip install -r robinhood_agentic/requirements-app.txt

# Optional: MCP token for live broker actions
# cp robinhood_agentic/monitor/.mcp_access_token.example \
#    robinhood_agentic/monitor/.mcp_access_token

python -m robinhood_agentic.app
# → http://127.0.0.1:8787
```

UI is served from `app/ui/public/index.html` (all four tabs). Optional React build:

```bash
cd robinhood_agentic/app/ui && npm install && npm run build
# dist/ is preferred over public/ when present
```

## Architecture

| Piece | Role |
|--------|------|
| `app/engine/` | Trading loop: quotes, TP/SL, rescan, auto-entry/exit, command queue |
| `app/api/` | FastAPI REST + SSE |
| `app/db/` | SQLite (`data/command_center.db`) |
| `app/broker/` | Thin wrapper around `monitor/mcp` |
| `app/ui/` | Command Center / Telemetry / Strategy / Analytics |

YAML under `config/` is **seed only**; runtime truth is `runtime_config` in SQLite (hot-reloaded each loop).

## GLOBAL HALT variants

| Variant | Behavior |
|---------|----------|
| `flatten` (default) | Cancel pending + stop scanner + market-flatten all |
| `soft` | Cancel pending + stop scanner, keep positions |
| `entries_only` | Block new entries; exits still monitored |

## Agent modes

- **autonomous** — auto-entry/exit via MCP when enabled
- **copilot** — proposes into `pending_approvals`; Approve/Reject in UI
- **halted** — after GLOBAL HALT

## Cloud (Phase 5)

```bash
export COMMAND_CENTER_PIN=your-pin   # enables PIN header auth on mutating routes
# Header: X-Command-Center-Pin: your-pin

docker compose -f robinhood_agentic/app/docker-compose.yml up
```

See `app/Dockerfile` and compose file for process supervision hints.
