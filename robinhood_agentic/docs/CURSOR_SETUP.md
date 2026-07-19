# Cursor setup — Robinhood Trading MCP

## Prerequisites

- Robinhood account in good standing (primary individual investing account).
- **Desktop** browser for Agentic account onboarding (mobile auth may require copying the URL to desktop).
- Cursor with MCP support (Settings → Tools & MCPs).

## 1. MCP config (already in this repo)

This project includes:

```json
{
  "mcpServers": {
    "robinhood-trading": {
      "url": "https://agent.robinhood.com/mcp/trading"
    }
  }
}
```

Location: `.cursor/mcp.json` at the repo root.

If you use a **global** config instead, copy `robinhood_agentic/config/mcp.cursor.json` to:

- macOS: `~/.cursor/mcp.json`

## 2. Connect in Cursor UI

1. Open this project in Cursor.
2. **Cursor Settings** (gear) → **Tools & MCPs** (or search “MCP”).
3. Find **robinhood-trading**.
4. If it shows **Needs authentication**, click **Connect**.
5. Complete login in the browser (OAuth). Robinhood may prompt you to open an **Agentic** account — follow those steps.
6. Return to Cursor; the server should show as connected and tools should appear.

Per [Robinhood’s Cursor instructions](https://robinhood.com/us/en/support/articles/agentic-trading-overview/#ConnectyourAIagent):

1. MCP link: `https://agent.robinhood.com/mcp/trading`
2. Cursor Settings → Tools & MCPs → Connect

## 3. Verify (read-only)

In a **new Agent chat** (`Cmd+L`), ask:

> Using Robinhood MCP, what is my buying power and open positions in my Agentic account?

Do not place trades until read-only calls work.

## 4. First trade (small test)

Example (adjust symbol/size):

> Using Robinhood MCP, place a **limit buy** for **1 share** of **SPY** at **$1 below** the current bid in my **Agentic** account only. Show me the order details before submitting and wait for my confirmation.

Always require explicit confirmation before live orders unless you fully accept unattended trading risk.

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| Server not listed | Reload Cursor; confirm `.cursor/mcp.json` is valid JSON. |
| Needs authentication | Click Connect again; use desktop browser. |
| Auth fails | Disconnect MCP, reconnect; ensure Robinhood Agentic onboarding completed. |
| Tools missing | New Agent chat; check Tools & MCPs shows green/connected. |
| Wrong account | Remember: **trades only go to the Agentic account**, not primary. |

Robinhood support: [Agentic Trading overview](https://robinhood.com/us/en/support/articles/agentic-trading-overview/) → Troubleshooting.
