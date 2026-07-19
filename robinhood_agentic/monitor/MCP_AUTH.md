# MCP auth for headless monitor

The session monitor can **place trades directly** via Robinhood MCP HTTP — no `cursor agent` LLM in the loop.

## One-time setup

### 1. Cursor agent login (API)

```bash
cursor agent login
cursor agent status   # should show your email
```

### 2. MCP server login (Robinhood OAuth)

This is **separate** from step 1:

```bash
cursor agent mcp login robinhood-trading
```

Complete the browser OAuth flow for your **Agentic** account.

Verify:

```bash
cursor agent mcp list
# robinhood-trading: connected   (not requires_authentication)
```

### 3. Token file (optional — faster than agent bridge)

If `cursor agent mcp login robinhood-trading` shows **ready**, the monitor uses the **agent MCP bridge** automatically — no JWT file required.

For lowest latency, optionally save a JWT to `.mcp_access_token` (direct HTTP).

| Method | How |
|--------|-----|
| **File** | Save token to `robinhood_agentic/monitor/.mcp_access_token` (one line, no quotes) |
| **Env** | `export ROBINHOOD_MCP_ACCESS_TOKEN='eyJ...'` before starting monitor |

**Getting the token:** After MCP login in Cursor IDE, open **Developer Tools → Network**, filter `agent.robinhood.com`, find an MCP request, copy the `Authorization: Bearer ...` value (JWT only, no `Bearer ` prefix in the file).

Or copy from a successful MCP tool request in Cursor's MCP logs.

```bash
cp robinhood_agentic/monitor/.mcp_access_token.example \
   robinhood_agentic/monitor/.mcp_access_token
# paste JWT into .mcp_access_token
chmod 600 robinhood_agentic/monitor/.mcp_access_token
```

### 4. Test

```bash
python3 robinhood_agentic/monitor/mcp_cli.py status
python3 robinhood_agentic/monitor/mcp_cli.py call get_accounts '{}'
```

## Monitor usage

Auto-exit / auto-entry use **direct MCP** when a token is available:

```bash
nohup python3 robinhood_agentic/monitor/session_monitor.py \
  --notify --tick-notify --auto-exit --auto-entry \
  >> robinhood_agentic/monitor/monitor.log 2>&1 &
```

Falls back to `cursor agent` only if no token is configured.

## CLI reference

```bash
# Any MCP tool (generic)
python3 robinhood_agentic/monitor/mcp_cli.py call place_equity_order \
  '{"account_number":"560944019","symbol":"GOOGL","side":"sell","type":"market","quantity":"0.28","market_hours":"regular_hours"}'

# Synthetic exit from alert
python3 robinhood_agentic/monitor/mcp_cli.py exit 'SL_HIT:GOOGL:last=354.00:sl=354.65:fractional=true'
```

## Security

- `.mcp_access_token` is gitignored — never commit
- Revoke by disconnecting MCP in Cursor → Settings → Tools & MCPs
- Token expires; re-export if monitor logs `JWT verification failed`
