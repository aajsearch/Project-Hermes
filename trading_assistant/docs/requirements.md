# Requirements

Functional and non-functional requirements for the Trading Assistant, derived from the codebase.

---

## Functional requirements

### Signals

- **FR-S1:** Compute a trend + momentum signal (BUY / SELL / HOLD) per equity symbol using bars and a profile.  
  *Source: `strategies/lev_etf_trend.py` — `generate_signal()`; `core/engine.py` — calls `lev_etf_signal(df, profile)` per equity.*
- **FR-S2:** Compute an options overlay signal per underlying from the same underlying trend (BUY → consider call, SELL → exit/avoid).  
  *Source: `strategies/options_overlay.py` — `generate_options_signal()`; `core/engine.py` — calls it per option underlying.*
- **FR-S3:** Append each cycle’s signals (time, asset, symbol, profile, signal, reason) to `logs/signals.csv`.  
  *Source: `core/engine.py` — `_append_signal_row()`.*

### Execution

- **FR-E1:** Place market orders for equities (BUY/SELL) when signal and risk checks allow; wait for fill and update state + ledger.  
  *Source: `core/engine.py` (equity loop), `broker/alpaca/orders.py` — `submit_market_order()`, `wait_for_order()`.*
- **FR-E2:** Place market orders for options (one call per underlying) when options signal is BUY and selection passes filters; exit on SELL or stop/take profit/max hold.  
  *Source: `core/engine.py` (options loop), `options/selector.py` — `pick_best_call()`.*
- **FR-E3:** Respect kill switch: when `KILL_SWITCH` is False, do not place any order (signals still run).  
  *Source: `config/settings.py` — `KILL_SWITCH`; `core/engine.py` — `can_place_orders`.*
- **FR-E4:** Gate execution by market hours (Pacific) unless `ALLOW_EXECUTION_WHEN_CLOSED` / `ALLOW_OPTION_EXECUTION_WHEN_CLOSED` are True.  
  *Source: `core/scheduler.py` — `is_market_open_now()`; `core/engine.py` — `can_execute`, `can_execute_opt`.*

### State

- **FR-ST1:** Persist positions (equity by symbol, options by `OPT:<underlying>`), pending orders, cooldowns, option_recos, portfolio_cooldown_until, mfe_mae_tracker, buys_last_hour in `state/state.json`.  
  *Source: `core/state_store.py` — `_default_state()`, `load_state()`, `save_state()`.*
- **FR-ST2:** Avoid duplicate buys: check state + broker for existing position; enforce MAX_BUYS_PER_CYCLE and MAX_BUYS_PER_HOUR.  
  *Source: `core/engine.py` — broker_qty, `buys_this_cycle`, `count_buys_in_last_hour()`, `append_buy_time()`.*

### Reconciliation

- **FR-R1:** On startup and at the start of each cycle, reconcile equity positions from broker into state (broker is source of truth).  
  *Source: `main.py` — startup reconcile; `core/engine.py` — `reconcile_equity_positions_from_broker()` at top of `run_cycle()`.*
- **FR-R2:** Reconcile option positions from broker into state; if multiple contracts per underlying, pick largest qty and log warning.  
  *Source: `core/engine.py` — `reconcile_option_positions_from_broker()`.*

### Reports and ledger

- **FR-L1:** Append every fill to `logs/trades.csv` with time, asset_type, symbol, side, qty, price, profile, strategy_version, signal_reason, exit_reason, mfe_pct, mae_pct, entry_snapshot_json.  
  *Source: `core/ledger.py` — `append_trade()`, `TRADE_LEDGER_FIELDS`.*
- **FR-L2:** Provide an EOD report script that reads `logs/trades.csv`, filters by date, computes PnL and exit reason counts, and writes `logs/daily_report_YYYY-MM-DD.md`.  
  *Source: `scripts/generate_eod_report.py`.*

---

## Non-functional requirements

### Safety

- **NFR-S1:** Paper trading by default; no live trading unless code and credentials are explicitly changed.  
  *Source: `main.py` — `make_trading_client(paper=True)`; `broker/alpaca/client.py`.*
- **NFR-S2:** A single kill switch disables all order placement while allowing signals and logging.  
  *Source: `config/settings.py` — `KILL_SWITCH`; `core/engine.py` — `can_place_orders`.*

### Resiliency

- **NFR-R1:** State file written atomically (tmp + replace) to avoid corruption on crash.  
  *Source: `core/state_store.py` — `save_state()` uses `.tmp` and `os.replace()`.*
- **NFR-R2:** State load self-heals missing keys (positions, pending_orders, cooldowns, option_recos, portfolio_cooldown_until, mfe_mae_tracker, buys_last_hour).  
  *Source: `core/state_store.py` — `load_state()` — `raw.setdefault(...)`.*

### Observability

- **NFR-O1:** Log startup, account info, reconcile result, per-symbol signals, SKIP reasons, order status, and errors.  
  *Source: `core/logger.py` — `log()`; `core/engine.py` — log calls throughout.*
- **NFR-O2:** Persist signals and fills to CSV for offline analysis and EOD report.  
  *Source: `logs/signals.csv`, `logs/trades.csv`.*

---

## Explicit constraints (from code/comments)

- **Alpaca options:** Order quantity must be integer (contracts).  
  *Source: `broker/alpaca/orders.py` — `_looks_like_option_contract()` then `qty = int(qty)`.*
- **Alpaca options:** Time-in-force for options is DAY.  
  *Source: `broker/alpaca/orders.py` — `time_in_force = TimeInForce.DAY` for option contracts.*
- **Order IDs:** Stored as strings (UUID converted at submission) so state is JSON-serializable.  
  *Source: `broker/alpaca/orders.py` — `return str(o.id)`; `core/state_store.py` — `JSONEncoder` for UUID.*

---

## Source of truth

- Execution and gating: `core/engine.py` — `run_cycle()`, `can_place_orders`, `can_execute`, `can_execute_opt`
- State schema: `core/state_store.py` — `_default_state()`, `load_state()`
- Ledger schema: `core/ledger.py` — `TRADE_LEDGER_FIELDS`, `append_trade()`
- Settings: `config/settings.py`

---

## What can go wrong

- **Requirements drift:** New features (e.g. another strategy or asset) may require new state keys or ledger columns; reconciliation and EOD script must be updated.
- **Missing env:** Bot fails at startup if `ALPACA_API_KEY` or `ALPACA_SECRET_KEY` are missing.  
  *Source: `broker/alpaca/client.py` — `raise ValueError("Missing ...")`.*

---

## How to verify

- Run bot and confirm signals in `logs/signals.csv` and fills in `logs/trades.csv`.
- Set `KILL_SWITCH = False`, run one cycle, confirm no orders and log "SKIP: kill_switch_off".
- Delete `state/state.json`, run with broker holding positions, confirm state repopulates and no duplicate buy for same symbol.
- Run `python scripts/generate_eod_report.py 2026-02-09` and confirm `logs/daily_report_2026-02-09.md` is created.
