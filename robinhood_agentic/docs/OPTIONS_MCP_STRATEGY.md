# Options strategy (single-leg) — Robinhood Agentic MCP

Single-leg directional options on ~$500 Agentic cash. **Read-only until options Level 2+ on Agentic.**

Config: [`config/options_directional.yaml`](../config/options_directional.yaml)  
Prompts: [`prompts/options_mcp.md`](../prompts/options_mcp.md)

## Prerequisites

1. `get_accounts` → `agentic_allowed=true` and `option_level_2` or `option_level_3`
2. If empty → upgrade URL from config template with **your** `account_number` from `get_accounts`
3. **One playbook per day** — do not combine with equity playbooks

## MCP workflow (correct)

```
get_option_chains(underlying)
→ get_option_instruments(chain, expiration, type)   # NO delta filter here
→ get_option_quotes(option_ids)                     # delta, OI, spread HERE
→ filter: delta ~0.15, OI ≥ 500, spread ≤ 10%, premium ≤ $75
→ review_option_order → place_option_order
```

## Pilot sizing ($75 budget)

| Setting | Value | Note |
|---------|-------|------|
| Delta target | **0.15** | 0.35Δ costs $200+ on QQQ — not compatible with $75 |
| DTE | **1–5** | No 0DTE in agentic flow |
| Max premium | $75 | Defined risk = 100% of premium |
| TP / SL | +40% / −30% on premium | Best-effort; can gap through |
| Underlyings | **SPY, QQQ** only (pilot) | |

## Regime

| QQQ day % | Type |
|-----------|------|
| ≥ +0.50% | Call |
| ≤ −0.50% | Put |
| Between | No entry |

## Not supported on MCP

Multi-leg spreads → use [`bot/alpaca_put_spread`](../../bot/alpaca_put_spread).

## Safety

Treat as **defined-risk lottery**, not scalping. Flat by **3:45 PM ET**.
