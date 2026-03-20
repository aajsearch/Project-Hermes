# Options PUT Strategy — How to Test

Short checklist to verify PUT strategy, execution, exits, and reporting.

---

## 1. Signals only (no orders): BUY_PUT recommendations in logs

- Set **KILL_SWITCH = False** in `config/settings.py`.
- Run `python main.py` (or one cycle with RUN_ONCE=True and FORCE_RUN_WHEN_MARKET_CLOSED=True if market is closed).
- In logs, for an option underlying that is **bearish** (trend down + RSI ≤ bear_rsi_max, e.g. 45), you should see a signal that implies BUY PUT, e.g.:
  - `[options] -> BUY | Underlying bearish -> consider put`
- For **bullish** underlying (BUY from trend strategy): `BUY | Underlying BUY -> consider call`.
- For **SELL** from underlying: `SELL | Underlying SELL -> exit options / avoid new entry`.

---

## 2. No duplicate buys after restart

- Run the bot until it opens an option position (CALL or PUT), then stop (Ctrl+C).
- Restart `python main.py`. On startup, **reconcile** runs and repopulates state from broker.
- Confirm in logs: "Startup reconcile complete." and that it does **not** place a second BUY for the same underlying.
- Check `state/state.json`: one entry under `positions` for `OPT:<underlying>` with correct `contract_type` (CALL or PUT).

---

## 3. Exit triggers: direction flip, stop-loss, take-profit

- **Direction flip:** With a **CALL** held, when the underlying signal becomes **bearish** (BUY PUT), logs should show exit reason `underlying_bearish_exit_call` and an "EXIT CALL" line. With a **PUT** held and signal **bullish** (BUY CALL), expect `underlying_bullish_exit_put` and "EXIT PUT".
- **Stop-loss / take-profit:** Option exits use `OPTION_STOP_LOSS_PCT` and `OPTION_TAKE_PROFIT_PCT` from `config/settings.py`. If the option’s mid PnL hits those thresholds, the bot should exit and log the corresponding reason.
- **Strategy SELL:** When the underlying trend strategy returns SELL, the bot should exit the current option (CALL or PUT) with reason `underlying_sell_signal`.

---

## 4. Ledger and EOD report

- After option trades (CALL and/or PUT), check **logs/trades.csv**: each row should have `contract_type` (CALL or PUT) for option rows.
- Run **EOD report:** `python scripts/generate_eod_report.py [YYYY-MM-DD]`. Open **logs/daily_report_YYYY-MM-DD.md** and confirm:
  - Section **"Options: CALL vs PUT"** with exit counts, PnL, and win rate per type.
  - **Exit reason distribution** and **Full trade list** include `contract_type` where applicable.

---

## 5. Streamlit UI

- Run `python3 -m streamlit run ui/app.py`.
- Use sidebar **Contract type (options)** filter: All / CALL / PUT.
- Confirm **Options: CALL vs PUT** summary (exits, PnL, win rate per type).
- Confirm **Completed trades** and **Best 5 / Worst 5** show `contract_type` when present.

---

## 6. Profile knobs (config/profiles.yaml)

For **options_call_overlay_v1**:

- **bear_rsi_max:** 45 — when trend down and RSI ≤ this, signal is BUY with direction=PUT.
- **put_delta_min / put_delta_max:** 0.40 and 0.55 — put selection uses delta in [-put_delta_max, -put_delta_min] (e.g. -0.55 to -0.40).
- Call selection still uses **delta_min / delta_max** (positive).

---

## Source of truth

- Strategy: `strategies/options_overlay.py` — `generate_options_signal()`, `_is_bearish_put_zone()`
- Selector: `options/selector.py` — `pick_best_call()`, `pick_best_put()`
- Engine: `core/engine.py` — options loop (exit direction-flip, BUY CALL vs BUY PUT), `_exit_all_positions()`
- Reconcile: `core/engine.py` — `reconcile_option_positions_from_broker()` (contract_type from symbol)
- Ledger: `core/ledger.py` — `contract_type` in TRADE_LEDGER_FIELDS and `append_trade()`
- Models: `core/models.py` — `Signal.direction`, `Position.contract_type`
