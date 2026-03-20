# Strategy Review & Tuning Guide

Assessment of entry/exit robustness, blind spots, what to watch when paper trading, missing metrics, and a path from paper to live.

---

## Is the strategy robust enough?

**Short answer:** It’s a solid base (trend + RSI, risk rules, MFE/MAE), but there are **blind spots** that can hurt profitability. Fixing them and tuning with data will improve the odds.

**What’s good today:**
- **Entry:** 2-bar trend confirmation (EMA20 > EMA50), min trend gap %, RSI in band (50–70) and rising → reduces some chop.
- **Exit (risk):** Hard stop loss, take profit, breakeven trail (trigger then trail to entry), time stop → limits drawdown and locks some profit.
- **Observability:** Exit reasons, MFE/MAE, and entry snapshots in the ledger → you can see why trades exited and how much they gave back.

**Where it’s weak:**
- **Single timeframe** — 5m only; no filter vs daily trend (leveraged ETFs can gap against you).
- **“RSI rising” = one bar** — noisy; can trigger on random bounces.
- **No volume filter** — can enter on low-volume, fake moves.
- **SELL RSI = 48 hardcoded** — not tunable per symbol/profile.
- **Trail only to entry** — no moving trail (e.g. trail below recent high); you give back a lot after a big MFE.
- **No volatility filter** — ATR exists but isn’t used for entry (e.g. skip or reduce size when ATR is very high).
- **Options** — Same trend as underlying; no IV/delta-based exit (e.g. IV crush, delta threshold).

---

## Blind spots and fixes

| Blind spot | Risk | Fix (code/config) |
|------------|------|--------------------|
| SELL RSI 48 hardcoded | Exits too early or too late depending on symbol | Add **exit_rsi_max** to profile; use it in strategy SELL (e.g. SELL when trend_down and rsi < exit_rsi_max). *Implemented below.* |
| No volume confirmation | False breakouts in thin bars | Add **volume** to entry snapshot; optionally require volume > N-period avg (profile: **entry_volume_ratio_min**). Start with logging only. |
| No ATR at entry | Can’t tune stop vs volatility | Add **ATR** (and **atr_pct**) to equity entry snapshot. Use for analysis; optionally **atr_stop_pct** in profile for wider SL in high vol. *Snapshot implemented below.* |
| Trail only to entry | Big giveback after large MFE | Consider **trailing_stop_pct** (e.g. exit when price falls X% from cycle high). Requires tracking “running high” per position (you already have MFE tracker). Future enhancement. |
| Single timeframe | Buying against daily downtrend | Add **daily_trend_filter**: e.g. fetch daily bars, require daily EMA20 > daily EMA50 to allow BUY. Future enhancement. |
| RSI “rising” = 1 bar | Noisy entries | Require RSI up over 2–3 bars (profile: **entry_rsi_rising_bars: 2**). Future enhancement. |
| No “no trade last N min” | Choppy close | Add **no_trade_last_minutes** (like no_trade_first_minutes) to skip entries near market close. Future enhancement. |

---

## What to watch when paper trading (for fine-tuning)

Use these to decide what to change in profiles and strategy.

### 1. Exit reason distribution (EOD report / UI)

- **Most exits are stop_loss** → SL may be too tight or entries too late; consider wider SL (or ATR-based) or stricter entry (e.g. stronger trend gap).
- **Most exits are time_stop** → Positions often held to max hold; consider shorter max_hold or earlier strategy SELL (e.g. exit_rsi_max).
- **Most exits are take_profit** → TP might be too easy; you can try raising TP or adding a trailing exit to capture more.
- **Many strategy SELL** → Trend/RSI exit is doing work; check if those exits are profitable on average (MFE/MAE vs realized).

*Source: `logs/daily_report_YYYY-MM-DD.md` “Exit reason distribution”; UI “Exit reason distribution” chart.*

### 2. MFE vs realized PnL (UI / trades.csv)

- If **MFE is often much larger than realized PnL** → You’re giving back profit; consider tighter trail (e.g. trail_trigger_pct smaller) or a proper trailing stop (future).
- If **MFE ≈ realized** → Exits are capturing most of the move; TP/trail may be well tuned for that symbol.

*Source: `logs/trades.csv` — mfe_pct, mae_pct on SELL rows; UI “MFE vs PnL %” scatter.*

### 3. MAE (max adverse excursion)

- **MAE often near or worse than -stop_loss_pct** → Stops are being hit at the limit; SL placement is in play.
- **MAE much worse than -stop_loss_pct** → Slippage or delay; or stop is ATR-based and wide. Use for sizing and SL tuning.

*Source: `logs/trades.csv` mae_pct; UI “MAE vs PnL %” scatter.*

### 4. Entry snapshot (trades.csv entry_snapshot_json)

- Compare **winning vs losing trades**: RSI, trend_gap_pct, and (once added) ATR/volume at entry.
- If losers often have **RSI near 70** or **weak trend_gap_pct** → Tighten entry_rsi_max or min_trend_gap_pct.
- If losers often have **high ATR (high vol)** → Consider atr_stop_pct or skipping entries when ATR > threshold.

*Source: `core/engine.py` — `_equity_entry_snapshot()`; ledger entry_snapshot_json on BUY.*

### 5. Win rate, avg win, avg loss, profit factor (EOD / UI)

- **Win rate & profit factor** → Target e.g. profit factor > 1.2 and win rate you’re comfortable with; tune SL/TP and entry filters to get there.
- **Avg loss much larger than avg win** → Either widen TP or tighten SL so risk/reward improves.

*Source: EOD report “Summary”; UI “KPIs”.*

### 6. Symbol-level breakdown

- Which symbols are **consistently profitable** vs **losing**? Consider removing or relaxing/strengthening rules per symbol (e.g. different profiles per symbol already allow this).

*Source: Filter by symbol in UI or group by symbol in trades.csv.*

### 7. Time of day

- **Hour of entry** (derived from `time` in trades.csv): Do certain hours have better win rate? Consider no_trade_first_minutes / no_trade_last_minutes.

*Source: Parse `time` in trades.csv; add to analysis or UI later.*

### 8. Hold duration

- **Winners closed fast vs losers held to time stop** → Suggests time stop is cutting winners or holding losers; tune max_hold_minutes and strategy SELL.

*Source: Not in ledger today; can add **hold_minutes** to SELL row (entry_time to exit time). Future enhancement.*

---

## Missing technical data / metrics (and what we added)

| Metric / data | Used today? | Added / suggested |
|---------------|-------------|--------------------|
| **ATR at entry** | No (only optional atr_stop_pct in profile) | **Yes** — add to equity entry snapshot (atr, atr_pct of price) for analysis and future ATR-based SL. |
| **Volume at entry** | No | **Yes** — add to snapshot if bars have volume (Alpaca bars do). Enables volume filter later. |
| **Hold duration (minutes)** | No | **Suggested** — add to SELL row in ledger for “time in trade” analysis. |
| **Exit reason** | Yes | Already in ledger; keep using for distribution. |
| **MFE/MAE** | Yes | Already in ledger; use for scatter and giveback analysis. |
| **Daily trend** | No | Suggested later: daily EMA trend as entry filter. |
| **Session (time of day)** | Derivable from `time` | No code change; analyze from existing `time` field. |

---

## Path from paper to live (profit goal)

1. **Paper run (2–4 weeks minimum)**  
   - Run current (or updated) strategy on paper only.  
   - Collect enough trades so exit reason distribution and MFE/MAE stats are meaningful (e.g. 30+ closed trades).

2. **Tune using “what to watch”**  
   - Adjust profiles (stop_loss_pct, take_profit_pct, trail_trigger_pct, max_hold_minutes, entry_rsi_min/max, min_trend_gap_pct, **exit_rsi_max**).  
   - Optionally drop or separate profiles for symbols that lose consistently.  
   - Target: profit factor > 1.2, drawdown and win rate you can live with.

3. **Add one or two robustness fixes**  
   - e.g. exit_rsi_max in profile (done), ATR/volume in snapshot (done), then consider volume filter or daily trend filter when you have data.

4. **Live with small size first**  
   - Switch to live only when you’re comfortable with paper results.  
   - Use a **small notional** (e.g. trade_notional_usd 200–300) and **one or two symbols** first.  
   - Keep **KILL_SWITCH** and **portfolio loss limit**; consider tighter **MAX_BUYS_PER_HOUR** / **MAX_BUYS_PER_CYCLE** initially.

5. **Scale slowly**  
   - Increase size or symbol set only after a period of stable, profitable live results.

---

## Source of truth

- Strategy: `strategies/lev_etf_trend.py`, `strategies/options_overlay.py`
- Risk: `core/risk.py` — `should_exit_position()`
- Snapshot: `core/engine.py` — `_equity_entry_snapshot()`, `_option_entry_snapshot()`
- Ledger: `core/ledger.py` — `TRADE_LEDGER_FIELDS`
- Profiles: `config/profiles.yaml`

---

## What can go wrong

- **Overfitting** — Tuning too much on too few paper trades can overfit; use at least a few weeks of data and out-of-sample check if possible.
- **Regime change** — What works in one vol/market regime may break in another; keep monitoring exit reasons and MFE/MAE after going live.
- **Leveraged ETFs** — 3x products can gap and reverse quickly; 5m may be noisy; consider testing 15m or daily filter.

---

## Paper trading checklist (daily / weekly)

**Daily (quick):**
- [ ] EOD report: `python scripts/generate_eod_report.py` — check total PnL, win rate, exit reason counts.
- [ ] Any new symbol that lost repeatedly? Note for profile tweak or removal.

**Weekly (tuning):**
- [ ] Exit reason distribution: mostly stop_loss → consider wider SL or stricter entry; mostly time_stop → tune max_hold or strategy SELL.
- [ ] MFE vs realized PnL (UI or CSV): large giveback → consider smaller trail_trigger_pct or trailing stop later.
- [ ] Entry snapshots of losers: RSI near 70? Weak trend_gap_pct? High atr_pct? → Tighten entry_rsi_max, min_trend_gap_pct, or add ATR filter.
- [ ] Profit factor and avg win vs avg loss: aim profit factor > 1.2; adjust TP/SL if risk/reward is off.

**Implemented for tuning:**
- **exit_rsi_max** in profile (default 48): strategy SELL when trend down and RSI < this. Lower = exit earlier (less giveback, may cut winners); higher = hold longer.
- **ATR and volume in entry snapshot**: each BUY row in `logs/trades.csv` has entry_snapshot_json with atr, atr_pct, volume for that bar (when bars have high/low/volume). Use to compare winning vs losing entries.

---

## How to verify

- After **exit_rsi_max**: run a cycle, trigger a strategy SELL, confirm log/reason uses profile value.  
- After **ATR/volume in snapshot**: run a BUY, inspect `logs/trades.csv` entry_snapshot_json for atr, atr_pct, volume.  
- Paper run: run 2+ weeks, generate EOD reports, review exit reason distribution and MFE vs PnL in UI.
