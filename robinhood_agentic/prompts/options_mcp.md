# Options MCP prompts — Robinhood Agentic (single-leg)

Config: [`config/options_directional.yaml`](../config/options_directional.yaml)  
Strategy: [`docs/OPTIONS_MCP_STRATEGY.md`](../docs/OPTIONS_MCP_STRATEGY.md)

---

## 1) Prerequisites check (read-only)

```
Robinhood MCP:
1. get_accounts → Agentic option_level, agentic_allowed.
2. If option_level empty: STOP. Show upgrade URL: upgrade_url_template with account_number from get_accounts.
3. Equity+options same day OK; skip if leveraged ETF already ran today.
4. get_portfolio; reserve $100.

Report: READY | NEED_OPTIONS_ENROLLMENT | INSUFFICIENT_CASH | LEV_ETF_CONFLICT
```

---

## 2) Chain scan (read-only)

```
Follow options_directional.yaml. No orders.

1. get_equity_quotes QQQ → regime (bull call ≥+0.50% / bear put ≤−0.50%).
2. Pilot underlyings: SPY, QQQ only.
3. get_option_chains(underlying)
4. get_option_instruments(expiration 1–5 DTE, call or put) — NO delta filter here
5. get_option_quotes on candidate option_ids
6. Filter ON QUOTES: delta 0.05–0.25 (target 0.15), open_interest ≥500, spread ≤10%, premium ≤$75

Table: | Underlying | Type | Exp | Strike | Delta | OI | Mid | Premium |

Recommend 1 candidate. Defined risk = 100% of premium.
```

---

## 3) Live trade (requires "go" + option_level 2+)

```
Follow options_directional.yaml.

PHASE A — Verify level + BP; equity playbook may already be live.
PHASE B — Chain scan (#2); ONE contract, premium ≤$75.
PHASE C — review_option_order buy open 1 limit; show TP +15%, SL -10%, max loss=premium. "go" → place.
PHASE D — Post-fill get_option_orders + get_option_positions.
PHASE E — Poll 15s: TP/SL on premium; 3:45 PM ET flatten. Max 2 trades/day.

No multi-leg. No 0DTE (dte_min=1).
```

---

## 4) CSP (explicit request only)

```
User must request CSP. Cash collateral + $150 buffer. sell to open ~20Δ put 7–21 DTE. review → "go".
```

---

## 5) EOD report

```
get_option_positions, get_option_orders, get_pnl_trade_history. Premium PnL per trade.
```

---

## 6) Emergency flatten

```
cancel_option_order all; sell-to-close every open option (review then place).
```
