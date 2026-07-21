# Leveraged ETF strategy (2x / 3x) — Robinhood Agentic

Directional intraday playbook for **leveraged ETFs** on ~$500 Agentic cash. MCP execution via Cursor Agent.

Config: [`config/leveraged_etf.yaml`](../config/leveraged_etf.yaml)  
Prompts: [`prompts/leveraged_etf.md`](../prompts/leveraged_etf.md)

## Pilot rules (post-review)

- **Do not stack with other playbooks** — if tech scalper or options ran today, skip leveraged ETF (equity+options may share a day with each other).
- **One position only** (`max_concurrent: 1`).
- **Pilot symbols:** TQQQ and SQQQ only until proven.
- **Whole-share path only** (`allow_fractional_live: false`).
- **QQQ regime:** ±**0.50%** day change (not 0.20%).
- **TP / SL:** **+2.00% / −1.20%** (wider for 3x noise).
- **TBT removed** from auto bear bucket (rates ≠ equity).
- **TSLL / FNGU:** discretionary only, not auto-select.

## Entry decision tree

```
available = buying_power - reserve_usd
cap = min(target_notional_usd, available)

IF allow_fractional_live == false:
  require mid <= cap for any entry (whole-share limit, qty=1)

IF mid <= cap AND 1 whole share fits:
  → LIMIT BUY at mid × (1 − 8 bps)
ELSE IF allow_fractional_live:
  → MARKET dollar_amount = min(cap, target_notional_usd)
ELSE:
  → SKIP symbol (too expensive for pilot)
```

## Exit (whole-share)

- Limit sell at TP (+2.00%)
- `stop_market` at SL (−1.20%)
- **On any fill:** cancel sibling resting sell order immediately
- Hard flat **3:55 PM ET**

## Regime

| QQQ day % | Trade |
|-----------|-------|
| ≥ +0.50% | Bull: TQQQ (pilot) |
| ≤ −0.50% | Bear: SQQQ (pilot) |
| Between | No entry |

## Risks

Vol decay, gap risk, SL is best-effort if chat stops. See [`SAFETY.md`](SAFETY.md).

## Rollout

After **tech scalper** pilot (50+ trades). One lev ETF day per week initially.
