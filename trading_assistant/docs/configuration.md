# Configuration

Settings, watchlist, and profiles. All paths and keys below are from the codebase.

---

## config/settings.py

| Variable | Meaning | Safe default |
|----------|--------|----------------|
| **MARKET_OPEN_HHMM** | Market open (hour, minute) Pacific | (6, 30) |
| **MARKET_CLOSE_HHMM** | Market close Pacific | (13, 0) |
| **LOG_DIR** | Log directory | "logs" |
| **STATE_DIR** | State directory | "state" |
| **STATE_PATH** | State file | "state/state.json" |
| **SIGNALS_CSV** | Signals log | "logs/signals.csv" |
| **EVENTS_LOG** | Events log | "logs/events.log" |
| **TRADES_LEDGER_CSV** | Trade ledger | "logs/trades.csv" |
| **FORCE_RUN_WHEN_MARKET_CLOSED** | Run cycle when market closed | False |
| **RUN_ONCE** | Exit after one cycle | False |
| **KILL_SWITCH** | If False, no orders (signals only) | True |
| **ALLOW_EXECUTION_WHEN_CLOSED** | Allow equity orders when closed | False |
| **EXECUTE_TRADES** | Execute equity trades | True |
| **DEFAULT_ORDER_TYPE** | Order type | "market" |
| **MAX_POSITION_PER_SYMBOL** | Max positions per equity symbol | 1 |
| **MAX_BUYS_PER_CYCLE** | Max new buys per cycle | 10 |
| **MAX_BUYS_PER_HOUR** | Max new buys in rolling hour | 20 |
| **EXECUTE_OPTION_TRADES** | Execute option trades | True |
| **ALLOW_OPTION_EXECUTION_WHEN_CLOSED** | Allow option orders when closed | False |
| **OPTION_TRADE_NOTIONAL_USD** | Option notional per trade | 500 |
| **OPTION_STOP_LOSS_PCT** | Option stop loss (decimal) | 0.20 |
| **OPTION_TAKE_PROFIT_PCT** | Option take profit (decimal) | 0.30 |
| **OPTION_MAX_HOLD_MINUTES** | Option max hold | 1440 |
| **OPTION_COOLDOWN_MINUTES** | Option cooldown after exit | 60 |
| **PORTFOLIO_PROFIT_LOCK_PCT** | Exit all if profit >= this of invested | e.g. 0.05 |
| **PORTFOLIO_LOSS_LIMIT_PCT** | Exit all if loss >= this of invested | e.g. 0.15 |
| **PORTFOLIO_COOLDOWN_MINUTES** | No new entries after portfolio exit | 60 |
| **STRATEGY_VERSION** | Tag for ledger/EOD | "1.1" |

**Recommended Monday (production) defaults:**  
- `FORCE_RUN_WHEN_MARKET_CLOSED = False`, `RUN_ONCE = False`  
- `KILL_SWITCH = True`, `ALLOW_EXECUTION_WHEN_CLOSED = False`, `ALLOW_OPTION_EXECUTION_WHEN_CLOSED = False`  
- `EXECUTE_TRADES = True`, `EXECUTE_OPTION_TRADES = True` (or disable one if you want equities-only or options-only)

**Testing defaults:**  
- Single cycle: `RUN_ONCE = True`  
- After-hours test: `FORCE_RUN_WHEN_MARKET_CLOSED = True`, `ALLOW_EXECUTION_WHEN_CLOSED = True` (and optionally options), then revert before live.

*Source: `config/settings.py`; used in `core/engine.py`, `main.py`, `core/ledger.py`, etc.*

---

## config/watchlist.yaml

**Structure:**
- **mode:** Optional (e.g. "paper"); not used in main loop.  
  *ASSUMPTION: present for documentation only; verify usage if needed.*
- **cadence_minutes:** Optional; loop does **not** read this—sleep is fixed 5 min in `core/scheduler.py`.  
  *Source: `main.py` uses `sleep_until_next_5min()`; watchlist is loaded but cadence_minutes not passed to scheduler.*
- **equities:** List of `{ symbol: "...", profile: "..." }`. Each symbol is traded with the named profile from `profiles.yaml`.
- **options:** List of `{ underlying: "...", profile: "..." }`. Each underlying gets one option position (call selected by profile filters).

**Example:**
```yaml
mode: paper
cadence_minutes: 5

equities:
  - symbol: FNGU
    profile: lev_etf_trend_v1
  - symbol: TQQQ
    profile: lev_etf_trend_v1

options:
  - underlying: SPY
    profile: options_call_overlay_v1
```

*Source: `main.py` — `load_yaml("config/watchlist.yaml")`; `core/engine.py` — `watchlist_cfg.get("equities", [])`, `watchlist_cfg.get("options", [])`.*

---

## config/profiles.yaml

**Structure:**  
- **profiles:** Map of profile name → dict of parameters.

**lev_etf_trend_v1 (equity):**
- **timeframe:** Bar timeframe (e.g. "5Min").  
- **lookback_bars:** Number of bars to fetch.  
- **ema_fast,** **ema_slow:** EMA lengths.  
- **rsi_len,** **entry_rsi_min,** **entry_rsi_max:** RSI and entry band.  
- **min_trend_gap_pct:** Min (ema_fast - ema_slow)/ema_slow to consider trend up.  
- **trade_notional_usd:** Notional per equity trade.  
- **no_trade_first_minutes:** Skip entries in first N minutes after 6:30 PT.  
- **stop_loss_pct,** **take_profit_pct:** Hard exit (decimal).  
- **trail_trigger_pct,** **trail_to_entry:** Breakeven trail (trigger %, trail to entry).  
- **cooldown_minutes:** Cooldown after exit.  
- **max_hold_minutes:** Time stop.  
- **min_hold_minutes:** Min hold before strategy SELL is allowed (stops/trail/time still immediate).  
- Optional: **atr_stop_pct**, **atr_stop_k** (uncomment if used).

**options_call_overlay_v1 (options):**
- Same trend params (timeframe, lookback_bars, ema_fast, ema_slow, rsi_len, entry_rsi_min, entry_rsi_max).  
- **dte_min,** **dte_max:** DTE range for contract selection.  
- **delta_min,** **delta_max:** Delta range.  
- **spread_pct_max:** Max bid-ask spread (as fraction of mid).  
- **iv_max:** Max implied volatility.  

Option notional is from settings `OPTION_TRADE_NOTIONAL_USD` unless profile has `option_trade_notional_usd`.

*Source: `config/profiles.yaml`; `core/engine.py`; `strategies/lev_etf_trend.py`; `core/risk.py`; `options/selector.py`.*

---

## Environment variables

- **ALPACA_API_KEY,** **ALPACA_SECRET_KEY:** Required for Alpaca API. Loaded from `.env` (python-dotenv) in `broker/alpaca/client.py`.  
Do not commit `.env`; use `.env.example` as a template.

---

## Source of truth

- All knobs: `config/settings.py`, `config/watchlist.yaml`, `config/profiles.yaml`
- Usage: `main.py`, `core/engine.py`, `core/risk.py`, `strategies/lev_etf_trend.py`, `options/selector.py`

---

## What can go wrong

- **Wrong profile name in watchlist:** KeyError when engine looks up `profiles[profile_name]`. Ensure names match `profiles.yaml`.
- **Missing key in profile:** Strategy or risk may use `.get()` with defaults; some keys (e.g. ema_fast) are required by strategy. Check strategy code for required keys.
- **YAML syntax error:** Load fails at startup; fix indentation and colons.

---

## How to verify

- `python -c "from config.settings import KILL_SWITCH, STATE_PATH; print(KILL_SWITCH, STATE_PATH)"`
- `python -c "import yaml; print(yaml.safe_load(open('config/watchlist.yaml')))"`
- `python -c "import yaml; print(yaml.safe_load(open('config/profiles.yaml'))['profiles']['lev_etf_trend_v1'])"`
