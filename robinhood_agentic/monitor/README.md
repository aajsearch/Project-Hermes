# Session monitor

Runs **outside Cursor chat** so AAPL (and other open legs) are polled on a real timer.

## What it does

| Interval | Action |
|----------|--------|
| **Every 15s** (from `tech_scalper.yaml` `poll_seconds`) | Equity position check: last vs synthetic TP / synthetic SL |
| **Every ~30s** (when `option_positions` non-empty) | Option mark via MCP vs premium TP (+15%) / SL (−10%); time-flat 3:45 PM ET |
| **Every 15 min** | Full 33-name watchlist rescan; optional **`--auto-entry`** places top pick if slot + BP |
| **Startup + every rescan + every 60s while a buy is pending** | **Broker reconcile** — adopt untracked equities **and** long options, finalize/drop pending buys |

Quotes use Robinhood’s **public read-only API** (fast, no MCP). **Trades** use **direct MCP HTTP** when `.mcp_access_token` is set — see [`MCP_AUTH.md`](MCP_AUTH.md).

## Setup

```bash
cd /path/to/Hades-prediction-market
python3 robinhood_agentic/monitor/session_monitor.py --notify
```

No extra pip packages required (stdlib only).

### Options

```bash
# One tick (test)
python3 robinhood_agentic/monitor/session_monitor.py --once

# Custom state file after editing positions
python3 robinhood_agentic/monitor/session_monitor.py --state robinhood_agentic/monitor/session_state.json

# Auto-execute synthetic TP/SL via cursor agent (REAL SELLS — use with care)
python3 robinhood_agentic/monitor/session_monitor.py --notify --auto-exit

# Auto-enter top rescan pick when a concurrent slot opens (REAL BUYS — needs cursor agent login)
python3 robinhood_agentic/monitor/session_monitor.py --notify --auto-entry --auto-exit
```

## State file

Edit `session_state.json` when you open/close legs:

```json
{
  "account_number": "560944019",
  "cap_overrides": ["AAPL", "GOOGL"],
  "positions": [
    {
      "symbol": "MSFT",
      "qty": 1,
      "entry": 420.0,
      "tp": 422.52,
      "sl": 418.11,
      "sl_order_id": "uuid-from-place_equity_order",
      "synthetic_sl": false,
      "synthetic_tp": true,
      "fractional": false
    },
    {
      "symbol": "GOOGL",
      "qty": 0.28,
      "entry": 356.08,
      "tp": 358.22,
      "sl": 354.65,
      "synthetic_sl": true,
      "synthetic_tp": true,
      "fractional": true
    }
  ]
}
```

- **Whole-share:** `sl_order_id` + `synthetic_sl: false` (broker SL); `synthetic_tp: true` (monitor TP)
- **Fractional:** `synthetic_tp` + `synthetic_sl` both true — no resting broker exits
- **`pending: true`** — buy order placed but not yet filled; monitor skips TP/SL checks and reconcile finalizes (fill) or drops (cancel/reject; stale >30m limit buys are cancelled)
- **`adopted: true`** — position found at broker but missing from state; reconcile added it with default TP/SL from `tech_scalper.yaml`
- **`option_positions`** — single-leg long options tracked by premium (entry/tp/sl per-share). TP/SL from `options_directional.yaml` (+15% / −10%). Quotes via MCP (public option marketdata is blocked). Auto-exit = market sell-to-close
- Remove position from array when flat (reconcile also removes legs no longer at broker)
- `cap_overrides` — symbols allowed above $100 notional in rescan

## Reconcile (never orphan a position)

Auto-entry used to drop a limit buy that didn't fill within 30s — the order could still fill later at the broker, leaving a live position **nobody was watching** (this happened with SMCI/INTC on 2026-07-17). Now:

1. Auto-entry writes the position to `session_state.json` **immediately** with `pending: true`.
2. Reconcile runs at startup, at every rescan, and every 60s while any position is pending:
   - Pending buy **filled** → real entry/qty recorded, TP/SL set, broker `stop_market` SL placed for whole shares.
   - Pending buy **cancelled/rejected** → dropped from state.
   - Pending buy **stale >30 min** → cancelled at broker, dropped.
   - Broker position **not in state** → adopted with default TP/SL (links an existing open `stop_market` sell if found).
   - State position **not at broker** → removed (already exited).

Requires MCP auth (`--auto-exit`/`--auto-entry` path) — reconcile is skipped otherwise.

## macOS: run in background

```bash
nohup python3 robinhood_agentic/monitor/session_monitor.py \
  --notify --tick-notify --auto-exit --auto-entry \
  >> robinhood_agentic/monitor/monitor.log 2>&1 &
```

- `--tick-notify` — desktop alert **every 15s** with AAPL/GOOGL prices (no typing)
- `--notify` — alerts only on SL/TP warnings

Stop: `pkill -f session_monitor.py`

## Live panel (no chat typing)

Cursor **cannot push into chat** on a timer. Use one of these instead:

| Method | How |
|--------|-----|
| **Dashboard** | `open robinhood_agentic/monitor/dashboard.html` — auto-refreshes every 15s |
| **Markdown** | Pin `LIVE_STATUS.md` in editor (updates every poll) |
| **Notifications** | `--tick-notify` — macOS banner every 15s |
| **Terminal** | `tail -f robinhood_agentic/monitor/monitor.log` |
| **New Agent chat** | `sessionStart` hook injects latest snapshot automatically |

**In Cursor:** Command Palette → **Simple Browser: Show** → open  
`file:///.../robinhood_agentic/monitor/dashboard.html` beside chat.

Reply **log** in chat anytime for a pasted snapshot (manual).

## Direct MCP executor (recommended for auto-exit)

```bash
# One-time auth — see MCP_AUTH.md
cursor agent mcp login robinhood-trading
# Save JWT to monitor/.mcp_access_token

python3 robinhood_agentic/monitor/mcp_cli.py status
python3 robinhood_agentic/monitor/mcp_cli.py call get_accounts '{}'
```

Generic tool call:

```bash
python3 robinhood_agentic/monitor/mcp_cli.py call <tool_name> '<json-args>'
```

## Limits

- **Whole-share SL** rests on Robinhood 24/7 — monitor does not auto-exit broker SL
- **Synthetic TP/SL** uses **direct MCP** when `.mcp_access_token` is set; otherwise cursor agent fallback
- **`--auto-entry`** same auth requirement — see [`MCP_AUTH.md`](MCP_AUTH.md)
- Headless `cursor agent` fallback often **cannot reach Robinhood MCP** — use token file for reliable exits
- Rescan is **alert-only** unless `--auto-entry` + token configured
