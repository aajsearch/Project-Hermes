# Strategy

Strategies implemented, indicators, entry/exit conditions, and tunable knobs. See also [Configuration](configuration.md) for profile and watchlist structure.

---

## Strategies implemented

### 1. Leveraged ETF trend (equities) — `lev_etf_trend_v1`

**File:** `strategies/lev_etf_trend.py` — `generate_signal(df, profile)` (exported as `lev_etf_signal` in engine).

**Idea:** Trend + momentum: BUY when short-term trend is up and RSI is in range and rising; SELL when trend weakens (EMA cross down) or RSI drops.

**Indicators:**  
- **EMA:** Fast and slow EMAs (e.g. 20 and 50) on close.  
  *Source: `indicators/ta.py` — `ema(series, length)`.*  
- **RSI:** RSI on close (e.g. length 14).  
  *Source: `indicators/ta.py` — `rsi(close, length)`.*

**Entry (BUY):**  
- Trend up: `ema_fast > ema_slow` for current and previous bar, and `(ema_fast - ema_slow) / ema_slow >= min_trend_gap_pct`.  
- RSI in band: `entry_rsi_min <= rsi_now <= entry_rsi_max`.  
- RSI rising: `rsi_now > rsi_prev`.  
*Source: `strategies/lev_etf_trend.py` — trend_up, rsi_ok, rsi_rising.*

**Exit (SELL):**  
- Trend down: `ema_fast < ema_slow` for current and previous bar and `rsi_now < 48`.  
*Source: `strategies/lev_etf_trend.py` — trend_down.*

**Per-trade risk (equity):** Handled in `core/risk.py` — `should_exit_position()`:  
- Stop loss: exit if `pnl_pct <= -stop_loss_pct`.  
- Take profit: exit if `pnl_pct >= take_profit_pct`.  
- Breakeven trail: if `pnl_pct >= trail_trigger_pct` and price falls back to entry, exit.  
- Time stop: exit if hold time >= `max_hold_minutes`.  
Strategy SELL is only acted on after `min_hold_minutes` (stops/trail/time are immediate).  
*Source: `core/risk.py`; `core/engine.py` — min_hold_minutes check.*

**Tunable knobs (profiles.yaml — `lev_etf_trend_v1`):**  
- `timeframe`, `lookback_bars`, `ema_fast`, `ema_slow`, `rsi_len`, `entry_rsi_min`, `entry_rsi_max`  
- `min_trend_gap_pct`, `trade_notional_usd`, `no_trade_first_minutes`  
- `stop_loss_pct`, `take_profit_pct`, `trail_trigger_pct`, `trail_to_entry`, `max_hold_minutes`, `min_hold_minutes`, `cooldown_minutes`  
- Optional: `atr_stop_pct` (wider stop), `atr_stop_k` (uncomment in profile if used).  

*Source: `config/profiles.yaml`; `strategies/lev_etf_trend.py`; `core/risk.py`.*

---

### 2. Options overlay — `options_call_overlay_v1`

**File:** `strategies/options_overlay.py` — `generate_options_signal(df, profile)`.

**Idea:** Reuse underlying equity trend: BUY → consider buying a call (selection in options selector); SELL → exit calls / avoid new entry.

**Implementation:** Same underlying bars and profile are passed to `underlying_signal(df, profile)` (i.e. `lev_etf_trend` logic). If underlying says BUY, options signal is BUY; if SELL, options signal is SELL; else HOLD.  
*Source: `strategies/options_overlay.py`.*

**Option selection:** When signal is BUY, `options/selector.py` — `pick_best_call()` selects a call by:  
- DTE range (`dte_min`, `dte_max`), delta range (`delta_min`, `delta_max`), `iv_max`, `spread_pct_max`.  
- Scoring prefers delta near target (e.g. 0.48).  
*Source: `options/selector.py`; `core/engine.py` — calls `pick_best_call()` with profile params.*

**Option exits:**  
- Stop loss / take profit: `OPTION_STOP_LOSS_PCT`, `OPTION_TAKE_PROFIT_PCT` (from settings).  
- Max hold: `OPTION_MAX_HOLD_MINUTES`.  
- Underlying SELL → exit reason "underlying_sell_signal".  
*Source: `core/engine.py` — options loop; `config/settings.py`.*

**Tunable knobs (profiles.yaml — `options_call_overlay_v1`):**  
- Same trend params as above (timeframe, lookback, ema, rsi).  
- Option selection: `dte_min`, `dte_max`, `delta_min`, `delta_max`, `spread_pct_max`, `iv_max`.  
- Execution notional: `option_trade_notional_usd` in settings; profile may override via `option_trade_notional_usd`.  
*Source: `config/profiles.yaml`; `core/engine.py` — `OPTION_TRADE_NOTIONAL_USD`, profile.get("option_trade_notional_usd", ...).*

---

## Indicators (ta.py)

- **ema(series, length):** Exponential moving average, span=length, adjust=False.  
- **rsi(close, length):** RSI (100 - 100/(1+RS)); gain/loss from close.diff(); first inf/nan clipped and filled with 50.  
- **atr(high, low, close, length):** Average True Range (rolling mean of true range). Present in code but optional for strategy (e.g. atr_stop_pct in profile if uncommented).  
*Source: `indicators/ta.py`.*

---

## Known limitations / next improvements

- **Cadence:** Loop uses fixed 5-minute sleep (`sleep_until_next_5min()`); `watchlist.yaml` has `cadence_minutes` but it is not read.  
  *ASSUMPTION: cadence is effectively 5 minutes. Verify `main.py` / `core/scheduler.py` if configurable cadence is needed.*
- **Options:** One position per underlying (one contract type); multiple contracts per underlying at broker are reconciled by taking largest qty and one symbol.
- **Equity entry price on reconcile:** When state is repopulated from broker, equity entry_price may be set from current mid, not actual fill price.
- **Strategy SELL vs risk exit:** Strategy SELL is delayed by `min_hold_minutes`; stop/take profit/trail/time exit are immediate.
- Possible future: separate “shadow mode” or dry-run that logs would-BUY/would-SELL without placing orders.

---

## Source of truth

- Equity strategy: `strategies/lev_etf_trend.py` — `generate_signal()`
- Options overlay: `strategies/options_overlay.py` — `generate_options_signal()`
- Option selection: `options/selector.py` — `pick_best_call()`
- Exit rules (equity): `core/risk.py` — `should_exit_position()`, `skip_near_open()`
- Exit rules (options): `core/engine.py` — options loop (stop/take profit/max_hold/underlying SELL)
- Indicators: `indicators/ta.py` — `ema()`, `rsi()`, `atr()`
- Profile knobs: `config/profiles.yaml`

---

## What can go wrong

- **No data / not enough bars:** Strategy returns NONE or “not enough bars”; no trade.  
  *Source: `strategies/lev_etf_trend.py` — empty df or len(df) < max(ema_slow+2, rsi_len+2).*
- **Options chain missing or strict filters:** `pick_best_call()` returns None; no option BUY.  
  *Source: `options/selector.py`; `core/engine.py` — `if best:`.*
- **Profile typo or missing key:** KeyError or wrong default; check profiles.yaml and engine/risk use of profile.get().

---

## How to verify

- Run one cycle and check `logs/signals.csv` for BUY/SELL/HOLD and reason strings.
- Change profile (e.g. `entry_rsi_min` / `entry_rsi_max`) and confirm signal behavior changes.
- Run options with a known underlying and confirm reco/contract in logs and state when BUY fires.
