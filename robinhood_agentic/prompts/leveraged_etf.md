# Leveraged ETF prompts — Robinhood Agentic

Config: [`config/leveraged_etf.yaml`](../config/leveraged_etf.yaml)  
Strategy: [`docs/LEVERAGED_ETF_STRATEGY.md`](../docs/LEVERAGED_ETF_STRATEGY.md)

---

## 1) Regime scan (read-only)

```
Follow leveraged_etf.yaml. Robinhood MCP only.

1. Do not start if tech scalper or options already ran today.
2. get_portfolio (minus pending_deposits).
3. get_equity_quotes: QQQ + tradable_symbols (TQQQ,SQQQ,UPRO,SPXU,SOXL,SOXS,TECL,TECS,NVDL).
4. QQQ regime: bull ≥+0.50% / bear ≤−0.50% / else chop.
5. Pilot: only recommend TQQQ (bull) or SQQQ (bear) from pilot_symbols_only.
6. Filter: spread ≤0.20%, ETF |day%| 0.40–2.50%.
7. cap = min(75, BP-50); entry_path = whole_share_limit if mid<=cap else SKIP (allow_fractional_live false).

Table: | Regime | Symbol | Mid | Spread% | Path | Est $ |

No orders.
```

---

## 2) Live session (requires "go")

```
Follow leveraged_etf.yaml on Agentic.

PHASE A — Regime scan; select ONE symbol (max_active_symbols=1). No opposite pair held.
PHASE B — Entry: cap=min(75,BP-50); whole-share limit if mid<=cap; else skip. TP +2.0%, SL -1.2%. Wait "go".
PHASE C — Post-fill TP/SL; cancel sibling on fill.
PHASE D — Poll 15s; 3:55 PM ET flatten.
PHASE E — Re-entry if flat, regime valid, SL<3.

Never bull+bear pair. Never TSLL/FNGU/TBT in pilot auto mode.
```

---

## 3) Quick test ($50 TQQQ or SQQQ)

```
Regime scan; one pilot symbol; whole-share if mid<=75 else skip. review → "yes" → place. One trade.
```

---

## 4) Mid-day rescan (read-only)

```
QQQ regime + TQQQ/SQQQ quotes; still valid? GFV-safe cash? No orders.
```

---

## 5) End of day report

```
EOD: fills, PnL vs TP/SL, regime at entry/exit. get_pnl_trade_history. No new orders.
```

---

## 6) Emergency flatten

```
Cancel equity orders; market sell all positions.
```
