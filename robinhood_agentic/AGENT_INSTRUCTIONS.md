# Agent instructions — Robinhood MCP

Use when working in Cursor Agent with **robinhood-trading** MCP enabled.

## Scope

Load the matching playbook before trading:
- **Tech scalper:** `config/tech_scalper.yaml` + `docs/TECH_SCALPER_STRATEGY.md`
- **Leveraged ETF:** `config/leveraged_etf.yaml` + `docs/LEVERAGED_ETF_STRATEGY.md`
- **Options:** `config/options_directional.yaml` + `docs/OPTIONS_MCP_STRATEGY.md`

Do **not** mix with Kalshi/Alpaca/Coinbase unless the user explicitly asks.

## Portfolio rule (hard)

**Equity + options same day OK** on the $500 Agentic account (`allow_equity_and_options_same_day`). Tech scalper and single-leg options may run together. **Do not** also run leveraged ETF the same day (`exclude_leveraged_etf_with_other_playbooks`).

Before every entry:
- `get_portfolio` — subtract `pending_deposits` from usable buying power
- Track GFV-safe settled cash in **this session only** (single owner)

## Default behavior

1. Read-only first (portfolio, quotes, orders).
2. Before any order: state symbol, side, qty, type, price, cost, TP, SL; confirm Agentic account; wait for user **"go"**.
3. After placement: order id/status, how to cancel.
4. No retry loops on rejected orders.

## Entry cap (equity playbooks)

Whole-share path only if:

`mid <= min(target_notional_usd, buying_power - reserve_usd)`

Never buy 1 share above the notional cap (e.g. no $300 share when target is $100).

## Exit hygiene (equity)

- Whole-share: **`stop_market` SL on broker** after fill; **synthetic TP** via session monitor (Robinhood cannot hold TP+SL on one share)
- **On synthetic TP:** cancel resting SL, then market sell
- **On broker SL fill:** position flat; update `session_state.json`
- Fractional: only if `allow_fractional_live: true` — **synthetic TP + SL** (no resting exits)

## Tech scalper

- **Full 33-name watchlist** (`pilot_core_only: false`); prefer `core_symbols` in ranking
- Max **2** concurrent; TP +0.60%, SL −0.45%
- `allow_fractional_live: false` default — skip symbols above notional cap
- Flat **3:55 PM ET**
- Equity + options same day OK; still avoid leveraged ETF the same day

## Leveraged ETF (pilot)

- **TQQQ / SQQQ only** if `pilot_symbols_only`
- **1** position max; QQQ regime ±**0.50%**
- TP **+2.00%**, SL **−1.20%**; `allow_fractional_live: false`
- No TBT, no auto TSLL/FNGU

## Options (single-leg)

- Stop if `option_level` empty; use `upgrade_url_template` with account from `get_accounts`
- Delta/OI on **`get_option_quotes`**, not `get_option_instruments`
- Pilot: SPY/QQQ; **0.15Δ**; premium ≤ **$75**; DTE ≥ **1**
- TP +15% / SL −10% on premium (intraday); max loss = 100% of premium
- Flat **3:45 PM ET**

## Logging

```
ACTION: ...
SYMBOL: ...
QTY: ...
PRICE: ...
RESULT: ...
REASON: ...
```
