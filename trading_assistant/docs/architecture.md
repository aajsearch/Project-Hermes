# Architecture

High-level components, data flow, and position lifecycle.

---

## Component diagram (text)

```
┌─────────────────────────────────────────────────────────────────────────┐
│ main.py                                                                  │
│   - Load env, create clients (stock data, trading, option data)          │
│   - Load watchlist + profiles                                             │
│   - Startup: reconcile state from broker → save_state                     │
│   - Loop: is_market_open_now() → run_cycle() or sleep_until_next_5min()  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ core/engine.py — run_cycle()                                              │
│   - Reconcile equities + options from broker                             │
│   - Portfolio risk: if PnL % hit → _exit_all_positions()                │
│   - can_place_orders = KILL_SWITCH and not portfolio cooldown             │
│   - EQUITIES: for each symbol → bars → lev_etf_signal → execute or skip   │
│   - OPTIONS: for each underlying → bars → options_signal → pick_best_call │
│             → execute BUY/SELL or skip                                    │
│   - All fills → append_trade(); state updates → save_state()              │
└─────────────────────────────────────────────────────────────────────────┘
        │                │                │                │
        ▼                ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ strategies/  │  │ core/        │  │ broker/      │  │ core/         │
│ lev_etf_     │  │ state_store  │  │ alpaca/      │  │ ledger        │
│ trend.py     │  │ load/save    │  │ orders,      │  │ append_trade  │
│ options_     │  │ positions,   │  │ positions,   │  │               │
│ overlay.py   │  │ cooldowns,   │  │ market_data  │  │               │
│              │  │ MFE/MAE,     │  │ client       │  │               │
│              │  │ buys_last_hour│  │              │  │               │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
        │                │                │                │
        ▼                ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ indicators/  │  │ state/       │  │ Alpaca API   │  │ logs/        │
│ ta.py        │  │ state.json   │  │ (paper)      │  │ trades.csv   │
│ ema, rsi, atr│  │              │  │              │  │ signals.csv  │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

**Config:** `config/settings.py`, `config/watchlist.yaml`, `config/profiles.yaml` are read by `main.py` and `core/engine.py`.  
**Scheduler:** `core/scheduler.py` — `is_market_open_now()` (Pacific), `sleep_until_next_5min()` (fixed 5-minute cadence).  
**ASSUMPTION:** `config/watchlist.yaml` has `cadence_minutes: 5` but the loop uses `sleep_until_next_5min()` which is hardcoded; cadence is not read from watchlist. Verify in `main.py` / `core/scheduler.py` if you need configurable cadence.

---

## Data flow

1. **Market data → signals**  
   For each equity: `get_bars_df(stock_data_client, sym, lookback, timeframe)` → `lev_etf_signal(df, profile)` → Signal(BUY/SELL/HOLD).  
   For each option underlying: same bars → `generate_options_signal(df, profile)` (underlying trend) → BUY/SELL/HOLD.

2. **Signals → orders**  
   If `can_place_orders` and `can_execute` / `can_execute_opt`: no pending order, not in cooldown, not already holding (broker check), under buy caps → `submit_market_order()` → `wait_for_order()`.

3. **Orders → state + ledger**  
   On fill: update state (set_position, init_mfe_mae_tracker, append_buy_time for BUY; clear position, set cooldown, get_and_clear_mfe_mae for SELL) → `save_state()`; `append_trade()` to `logs/trades.csv`.  
   Signals are always appended to `logs/signals.csv` each cycle.

4. **Broker → state (reconciliation)**  
   Startup and every cycle: `reconcile_equity_positions_from_broker()`, `reconcile_option_positions_from_broker()` — broker qty and (for options) contract list are source of truth; state positions are created/updated/cleared to match.

---

## State machine: position lifecycle

```
[No position]
      │ signal=BUY, checks pass
      ▼
[Pending BUY]  ── order filled ──► [Open position]
      │                                   │
      │ order canceled/rejected            │ each cycle: update_mfe_mae_tracker
      ▼                                   │ exit if: stop/take profit/trail/max_hold/strategy SELL
[No position]                             │           or portfolio lock → _exit_all_positions
                                          ▼
                              [Pending SELL] ── filled ──► [No position] + cooldown
                                          │
                                          └── timeout (e.g. market closed) ──► [Open position] remains, pending kept
```

- **Equities:** Key = symbol. Cooldown per symbol after exit (from profile `cooldown_minutes` or portfolio exit).
- **Options:** Key = `OPT:<underlying>`. One position per underlying; cooldown per underlying after exit.
- **Portfolio lock:** When portfolio PnL % crosses profit lock or loss limit, `_exit_all_positions()` runs and then `set_portfolio_cooldown()`; no new entries until cooldown expires.

---

## Source of truth

- Main loop: `main.py` — `main()`, `while True`, `run_cycle()`, `sleep_until_next_5min()`
- Cycle logic: `core/engine.py` — `run_cycle()`, reconciliation, portfolio check, equity/option loops
- State: `core/state_store.py` — `load_state()`, `save_state()`, position/cooldown/option_reco/MFE/MAE/buys_last_hour
- Market hours: `core/scheduler.py` — `is_market_open_now()`, `sleep_until_next_5min()`

---

## What can go wrong

- **State and broker diverge:** Restart or missed reconciliation can leave state stale; reconciliation at start of each cycle is intended to correct this. If broker has positions for symbols not in watchlist, they are not reconciled into state (equity reconcile only considers watchlist symbols).
- **Options: multiple contracts per underlying:** Reconciler picks largest qty and logs a warning; state holds one position per underlying. Verify `reconcile_option_positions_from_broker()` in `core/engine.py` if you use multiple contracts per underlying.

---

## How to verify

- Run `python main.py` and confirm log: "Startup reconcile complete.", then either cycle output or "Market closed. Waiting...".
- Inspect `state/state.json` after a few cycles; should have `positions`, `cooldowns`, and optionally `pending_orders` consistent with broker.
- After an exit, confirm cooldown in state: `cat state/state.json | grep -A2 cooldowns`.
