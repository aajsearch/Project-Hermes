# Tech scalper strategy (Robinhood Agentic)

Intraday micro-swing for liquid tech names on ~$500 Agentic cash. Execution via Cursor Agent + Robinhood MCP.

Config: [`config/tech_scalper.yaml`](../config/tech_scalper.yaml)  
Prompts: [`prompts/tech_scalper.md`](../prompts/tech_scalper.md)

## Live rules

| Setting | Value |
|---------|-------|
| Playbook | Tech scalper + options same day OK; avoid leveraged ETF same day |
| Watchlist | **Full 33 symbols** (quotes batch 20 + 13) |
| `allow_fractional_live` | **false** default — whole-share broker SL + synthetic TP |
| `max_concurrent` | **2** |
| Entry cap | `mid ≤ min($100, BP − $50)` — skip if too expensive |
| Ranking tie-break | Prefer `core_symbols`; rank `deprioritize_symbols` lower (still eligible) |
| Exit model | Whole: broker **SL** + monitor **TP**; fractional: monitor **TP+SL** |
| Sibling orders | Cancel resting SL when synthetic TP exits (no OCO on Robinhood) |
| Pending deposits | Subtract from usable BP before sizing |

## Idea

1. Scan all 33 watchlist symbols.
2. Filter spread, session window, buying power, whole-share cap.
3. Enter whole-share limit when affordable; skip expensive names (fractional off).
4. Exit +0.60% / −0.45%.
5. Re-enter when flat, GFV-safe cash, signals pass.

## Entry decision tree

```
available = buying_power - reserve_usd
cap = min(target_notional_usd, available)

IF mid <= cap AND 1 whole share fits:
  → LIMIT BUY qty=1 at mid × (1 − 5 bps)

ELSE IF allow_fractional_live:
  → MARKET dollar_amount = cap

ELSE:
  → SKIP
```

## Exit

Robinhood cannot rest **both** a limit TP and `stop_market` SL on the same share. Prefer **broker SL** (24/7 downside) over broker TP.

### Whole-share

| Exit | Action |
|------|--------|
| Stop loss | `stop_market` at entry × 0.9955 on broker immediately after fill |
| Take profit | Monitor `synthetic_tp: true`; market sell when last ≥ entry × 1.006 |
| On synthetic TP | `cancel_equity_order` on resting SL, then market sell |
| On broker SL fill | Position flat; remove from `session_state.json` |
| Time stop | 3:55 PM ET flatten |

### Fractional (`allow_fractional_live: true`)

| Exit | Action |
|------|--------|
| Take profit / stop loss | Both synthetic — monitor + `--auto-exit` or manual sell in chat |
| Broker resting orders | Not supported for fractional qty |

## Session windows (ET)

- No entry before **9:45 AM** or after **3:30 PM**
- Flat all by **3:55 PM**

## Cash / GFV

Fund same-day-exit buys from settled cash. Do not chain sell→buy→sell on unsettled proceeds. One chat session owns the ledger.

## Scaling gates (after 50+ closed trades)

Prove ≥55% win rate before:
- Raising `max_concurrent` (2 → 3–4)
- Enabling `allow_fractional_live`

Watchlist stays at 33 names throughout — 50 trades gates **size/aggressiveness**, not symbol count.

## MCP tools

`get_accounts`, `get_portfolio`, `get_equity_quotes`, `review_equity_order`, `place_equity_order`, `get_equity_orders`, `get_equity_positions`, `cancel_equity_order`, `get_pnl_trade_history`
