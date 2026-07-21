# Tech scalper prompts — Robinhood Agentic

Config: [`config/tech_scalper.yaml`](../config/tech_scalper.yaml)  
Strategy: [`docs/TECH_SCALPER_STRATEGY.md`](../docs/TECH_SCALPER_STRATEGY.md)

---

## 1) Pre-market scan (read-only)

```
Follow tech_scalper.yaml and TECH_SCALPER_STRATEGY.md. Robinhood MCP only.

1. get_accounts → Agentic; get_portfolio (flag pending_deposits — subtract from usable BP).
2. Equity+options same day OK; skip only if leveraged ETF already ran today.
3. get_equity_quotes: full watchlist 33 symbols (batch 20+13).
4. Per symbol: mid, spread%, day%; cap = min(100, BP-reserve).
   entry_path = whole_share_limit if mid <= cap else (fractional_market if allow_fractional_live else SKIP).
5. Filter: spread ≤0.15%, |day%| 0.30–0.80%, active.
6. Rank: spread → prefer core_symbols → deprioritize last → |day%|. Pick up to 2 (max_concurrent).

Table: | Rank | Symbol | Mid | Spread% | Day% | Path | Fits cap? |

No orders.
```

---

## 2) Live session (requires "go")

```
Follow tech_scalper.yaml on Agentic (same account_number every call).

PHASE A — Read-only setup
- skip if leveraged ETF already live today; portfolio minus pending_deposits
- Session: 9:45 AM–3:30 PM ET entries only
- Scan (#1); select up to 2 symbols; track settled_cash_pool

PHASE B — Entry (wait for "go")
cap = min(target_notional_usd, buying_power - reserve)
IF mid <= cap: review_equity_order limit buy qty=1, mid*(1-5bps)
ELIF allow_fractional_live: review market dollar_amount=cap
ELSE: skip symbol
Show TP +0.60%, SL -0.45%. place after "go".

PHASE C — Post-fill
- get_equity_orders + get_equity_positions
- Whole-share: place stop_market SL on broker; synthetic_tp via monitor (Robinhood cannot hold TP+SL on same share)
- Fractional (if allow_fractional_live): no resting exits — synthetic_tp + synthetic_sl in session_state
- On broker SL fill: cancel any open sell sibling if present
- On synthetic TP exit: cancel resting SL first, then market sell

PHASE D — Manage (15s poll while chat active)
- Whole-share: broker SL must stay live; monitor handles synthetic TP
- Fractional only if allow_fractional_live; monitor handles both TP and SL
- Session monitor `--auto-entry`: after 15m rescan, enter top pick if slot open + BP fits (needs `cursor agent login`)
- 3:55 PM ET flatten

PHASE E — Re-entry when flat, GFV-safe cash, SL count < 5

No orders without "go" / "exit" / "flatten".
```

---

## 3) Quick test (one symbol)

```
tech_scalper.yaml: scan full 33-name watchlist; pick tightest spread that fits cap (mid <= min(100, BP-50)).
review_equity_order → wait "yes" → place. Post-fill + TP/SL. One trade only.
```

---

## 4) Mid-day rescan (read-only)

```
Rescan tech watchlist; symbols passing filters not held; GFV-safe cash for next leg. No orders.
```

---

## 5) End of day report

```
EOD: start/end BP, fills, PnL, round-trips, SL count, GFV warnings. get_pnl_trade_history if trades occurred.
```

---

## 6) Emergency flatten

```
Cancel all equity orders; market sell all positions. Confirm each.
```
