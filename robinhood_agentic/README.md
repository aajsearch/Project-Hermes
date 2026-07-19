# Robinhood Agentic Trading (MCP)

Connect Cursor to Robinhood's **Agentic Trading** account via the official Trading MCP.

- MCP endpoint: `https://agent.robinhood.com/mcp/trading`
- Official guide: [Robinhood Agentic Trading overview](https://robinhood.com/us/en/support/articles/agentic-trading-overview/#ConnectyourAIagent)
- Cursor MCP docs: [cursor.com/docs/mcp](https://cursor.com/docs/mcp)

This folder is **separate** from the Kalshi/Alpaca/Coinbase bots in this repo. It does not run a local Python bot — trading happens through Cursor + Robinhood MCP after you authenticate.

## Folder layout

```
robinhood_agentic/
  README.md                 # this file
  AGENT_INSTRUCTIONS.md     # rules for the AI when using Robinhood MCP
  config/
    mcp.cursor.json         # same MCP snippet (backup / copy-paste)
    tech_scalper.yaml       # tech scalper (33-name watchlist, max 2 legs)
    leveraged_etf.yaml      # 2x/3x ETF (pilot: TQQQ/SQQQ)
    options_directional.yaml
  docs/
    CURSOR_SETUP.md         # step-by-step Cursor + Robinhood onboarding
    SAFETY.md               # risks and guardrails
    TECH_SCALPER_STRATEGY.md
    LEVERAGED_ETF_STRATEGY.md
    OPTIONS_MCP_STRATEGY.md
  prompts/
    examples.md             # safe example prompts (read-only vs trade)
    tech_scalper.md
    leveraged_etf.md
    options_mcp.md
  monitor/
    session_monitor.py      # 15s position poll + 15m watchlist rescan (run in terminal)
    README.md
    session_state.example.json
```

Project-level MCP config lives at:

```
.cursor/mcp.json            # already wired to robinhood-trading
```

## Quick start

1. Read `docs/CURSOR_SETUP.md` and complete Robinhood OAuth (desktop browser).
2. Open an **Agentic** Robinhood account when prompted during auth.
3. In Cursor: **Settings → Cursor Settings → Tools & MCPs** → connect **robinhood-trading**.
4. Start a new Agent chat and try a read-only prompt from `prompts/examples.md`.
5. Playbooks ( **one per day** on $500 Agentic):
   - Tech scalper: `TECH_SCALPER_STRATEGY.md` + `tech_scalper.md` (start here)
   - Lev ETF pilot: `LEVERAGED_ETF_STRATEGY.md` + `leveraged_etf.md`
   - Options (after enroll): `OPTIONS_MCP_STRATEGY.md` + `options_mcp.md`
6. Only after read-only scans work, try small test orders in the Agentic account.
7. **Live session monitor** (optional): `python3 robinhood_agentic/monitor/session_monitor.py --notify` — polls AAPL TP/SL every 15s; see `monitor/README.md`.

## Important

- Trades can only be placed in your **Robinhood Agentic** account (not your primary account).
- You are responsible for every order the agent places. See `docs/SAFETY.md`.
