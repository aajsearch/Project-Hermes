# Design

Design decisions and tradeoffs: reconciliation, kill switch, portfolio lock, MFE/MAE, and order handling.

---

## Reconciliation design

**Why:** After a crash or restart, state may be missing positions that exist at the broker. Without reconciliation, the bot might open duplicate positions (e.g. BUY again for a symbol already held). Reconciling from broker to state makes broker the source of truth and repopulates state so the next cycle does not double-buy.

**When:**  
- On startup in `main.py`: after loading state, `reconcile_equity_positions_from_broker()` and `reconcile_option_positions_from_broker()` are called, then `save_state()`.  
- At the start of every `run_cycle()` in `core/engine.py`: same two reconciliation functions run, then state is saved. So every cycle begins with state aligned to broker for watchlist symbols/underlyings.

**How:**  
- **Equities:** For each symbol in watchlist, get broker qty via `get_open_position_qty(trading_client, sym)`. If broker qty <= 0, clear state position. If state is missing or qty differs, set state position from broker (entry price from latest mid if needed).  
  *Source: `core/engine.py` — `reconcile_equity_positions_from_broker()`.*  
- **Options:** Fetch all broker positions, filter to option contracts, group by underlying (parse from contract symbol). For each watchlist underlying: if broker has no position, clear state; if broker has position(s), pick one (e.g. largest qty if multiple contracts), set state position.  
  *Source: `core/engine.py` — `reconcile_option_positions_from_broker()`.*

**Tradeoff:** Entry price after reconcile may be set from current mid (equities) or broker avg_entry_price (options), not the original fill price; PnL and MFE/MAE for that position may be off until next real fill. Acceptable for avoiding duplicate buys.

---

## Kill switch design

**Purpose:** Single flag to disable all order placement while keeping strategy and logging running. Useful for testing, incidents, or “signals only” mode.

**Implementation:** In `core/engine.py`, `can_place_orders = (KILL_SWITCH is True) and (not is_portfolio_in_cooldown(state))`. Every order path (equity BUY/SELL, option BUY/SELL, portfolio exit) checks `can_place_orders`; when False, no `submit_market_order()` is called. When `KILL_SWITCH` is False, the log message "SKIP: kill_switch_off" is emitted once per cycle.

**Location:** `config/settings.py` — `KILL_SWITCH = True`. Intended default is True (orders allowed when other conditions pass); set to False to disable orders.

---

## Portfolio lock design

**Purpose:** Cap risk and lock in profit at the portfolio level: if unrealized PnL as a percentage of invested notional crosses a threshold, exit all positions and block new entries for a cooldown period.

**Implementation:**  
- At the start of each cycle (after reconciliation), `_compute_portfolio_invested_and_pnl()` computes invested notional and unrealized PnL (equities: broker qty × entry vs current mid; options: broker qty × 100 × entry vs mid).  
- If invested > 0 and `pnl_pct = unrealized / invested` is >= `PORTFOLIO_PROFIT_LOCK_PCT` or <= `-PORTFOLIO_LOSS_LIMIT_PCT`, the bot calls `_exit_all_positions(..., exit_reason_str=reason)` and then sets portfolio cooldown via `set_portfolio_cooldown(state, _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES))`.  
- `can_place_orders` is False while `is_portfolio_in_cooldown(state)` is True, so no new BUYs until the cooldown expires.

**Parameters:** `config/settings.py` — `PORTFOLIO_PROFIT_LOCK_PCT`, `PORTFOLIO_LOSS_LIMIT_PCT`, `PORTFOLIO_COOLDOWN_MINUTES`.

**Exit when market is closed:** `_exit_all_positions()` takes `market_open` and uses a short wait timeout (e.g. 15s) when market is closed so the process does not block; orders that remain "accepted" are left in state and can be retried next cycle. *Source: `core/engine.py` — `_exit_all_positions(..., market_open=market_open)`, `wait_timeout_s = 15 if not market_open else 60`.*

---

## MFE/MAE tracking design

**Purpose:** Record Maximum Favorable Excursion and Maximum Adverse Excursion (as % of entry) per position for later analysis (e.g. in EOD report or UI).

**Implementation:**  
- On open (after filled BUY): `init_mfe_mae_tracker(state, key, entry_price)` stores `{ high: entry_price, low: entry_price }` under `state["mfe_mae_tracker"][key]`.  
- Each cycle while position is open: `update_mfe_mae_tracker(state, key, current_price)` updates high/low from current price.  
- On close (filled SELL): `get_and_clear_mfe_mae(state, key, entry_price)` returns `(mfe_pct, mae_pct)` as decimals and removes the tracker entry; these are passed to `append_trade(..., mfe_pct=..., mae_pct=...)` for the SELL row in the ledger.

**Keys:** Equity key = symbol; option key = `OPT:<underlying>`.  
*Source: `core/state_store.py` — `init_mfe_mae_tracker`, `update_mfe_mae_tracker`, `get_and_clear_mfe_mae`; `core/engine.py` — called on BUY fill, each cycle for open position, and on SELL fill.*

---

## Order ID storage (UUID serialization)

**Problem:** Alpaca returns order IDs as UUIDs; JSON does not natively serialize UUID. Writing them to `state/state.json` would cause serialization errors.

**Solution:**  
- In `broker/alpaca/orders.py`, `submit_market_order()` returns `str(o.id)`.  
- Pending order dict in state stores `"order_id": str(order_id)`.  
- `core/state_store.py` uses a custom `JSONEncoder` that converts UUID to string if any UUID is ever passed.  
So all order IDs in state are strings; no raw UUID is written.

---

## Source of truth

- Reconciliation: `core/engine.py` — `reconcile_equity_positions_from_broker()`, `reconcile_option_positions_from_broker()`
- Kill switch: `config/settings.py` — `KILL_SWITCH`; `core/engine.py` — `can_place_orders`
- Portfolio lock: `core/engine.py` — `_compute_portfolio_invested_and_pnl()`, portfolio check, `_exit_all_positions()`, `set_portfolio_cooldown()`; `core/state_store.py` — `set_portfolio_cooldown()`, `is_portfolio_in_cooldown()`
- MFE/MAE: `core/state_store.py` — `init_mfe_mae_tracker`, `update_mfe_mae_tracker`, `get_and_clear_mfe_mae`; `core/engine.py` — calls and ledger `mfe_pct`/`mae_pct`
- Order IDs: `broker/alpaca/orders.py` — `return str(o.id)`; state pending order dict uses string

---

## What can go wrong

- **Reconcile overwrites entry_price:** Equity reconcile may set entry_price from current mid when state was missing; historical PnL for that position is approximate.
- **Portfolio exit during closed market:** Some SELL orders may stay "accepted"; bot leaves them pending and retries next cycle. Positions may still show in state until filled.
- **MFE/MAE missing on old trades:** Positions opened before MFE/MAE was implemented will not have tracker data; ledger rows may have empty mfe_pct/mae_pct.

---

## How to verify

- **Kill switch:** Set `KILL_SWITCH = False`, run one cycle; log should show "SKIP: kill_switch_off" and no order submissions.
- **Portfolio cooldown:** After a portfolio lock event, `state/state.json` should have `portfolio_cooldown_until` set; next cycle should log "SKIP: portfolio_lock (cooldown active)".
- **MFE/MAE:** Run a full BUY → hold a few cycles → SELL; check `logs/trades.csv` for the SELL row with non-empty mfe_pct and mae_pct.
- **Order ID:** Inspect `state/state.json` after placing an order; `pending_orders` should have string `order_id` values (no raw UUID).
