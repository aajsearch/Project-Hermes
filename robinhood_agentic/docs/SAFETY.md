# Safety — Robinhood Agentic + Cursor

## You are in control

- You are **legally and financially responsible** for every trade the agent places.
- The agent can read balances, positions, history, watchlists, and scans across Robinhood accounts.
- **Orders are placed only in the Robinhood Agentic account** — still real money.

## Recommended guardrails

1. **Read-only first** — portfolio, buying power, quotes — before any order.
2. **Confirm every trade** — ask the agent to show order payload and wait for “yes” unless you explicitly want unattended automation.
3. **Small size** — first live test: 1 share or minimal notional.
4. **No secrets in chat** — never paste API keys or passwords; OAuth handles auth.
5. **Separate from other bots** — this MCP path is unrelated to `bot.v2_main`, Alpaca, or Coinbase code in this repo.

## What the MCP can access (Robinhood)

When connected, the agent typically has **read** access to:

- Account numbers and balances
- Positions
- Transaction / order history
- Watchlists and scans

See [Robinhood disclosures](https://robinhood.com/us/en/support/articles/agentic-trading-overview/#Disclosures).

## If something goes wrong

1. Stop the agent / close the chat.
2. Disconnect **robinhood-trading** in Cursor → Tools & MCPs.
3. Cancel open orders in the Robinhood app (Agentic account).
4. Contact Robinhood Support if the issue is on their side.
