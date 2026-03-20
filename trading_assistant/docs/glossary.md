# Glossary

Definitions of terms used in the codebase and docs. References are to files/functions where the concept is implemented or used.

---

## profile

A named set of strategy and risk parameters in `config/profiles.yaml` (under `profiles`). Each watchlist entry (equity symbol or option underlying) references a profile by name (e.g. `lev_etf_trend_v1`, `options_call_overlay_v1`). The engine looks up `profiles[profile_name]` and passes the dict to the strategy and risk logic.  
*Source: `config/profiles.yaml`; `core/engine.py` — `profile = profiles[profile_name]`.*

---

## watchlist

The list of symbols (equities) and underlyings (options) the bot trades, plus the profile name for each. Defined in `config/watchlist.yaml` under `equities` (list of `{ symbol, profile }`) and `options` (list of `{ underlying, profile }`). Reconciliation and the main loop iterate only over these symbols/underlyings.  
*Source: `config/watchlist.yaml`; `main.py` — `load_yaml("config/watchlist.yaml")`; `core/engine.py` — `watchlist_cfg.get("equities", [])`, `watchlist_cfg.get("options", [])`.*

---

## signal

The output of a strategy for one symbol or underlying in a single cycle: an action (BUY, SELL, HOLD, NONE) and a reason string. Produced by e.g. `lev_etf_signal(df, profile)` or `generate_options_signal(df, profile)`. Signals are appended to `logs/signals.csv` each cycle.  
*Source: `core/models.py` — `Signal` dataclass; `strategies/lev_etf_trend.py`; `core/engine.py` — `_append_signal_row()`.*

---

## position

A held asset (equity or option) tracked in state. Stored under `state["positions"]` with key = symbol (equity) or `OPT:<underlying>` (options). Each entry is a dict compatible with the `Position` dataclass: asset_type, key, symbol, qty, entry_price, entry_time; for options, underlying and contract. Broker is source of truth for qty; state is repopulated by reconciliation.  
*Source: `core/models.py` — `Position`; `core/state_store.py` — `get_position()`, `set_position()`; `core/engine.py` — reconciliation.*

---

## reco (option recommendation)

When the options strategy signals BUY, the selector picks a recommended call contract; that result is stored in state as `option_recos[underlying]` (e.g. contract, dte, delta, iv, bid, ask, spread_pct, time, profile, signal). Used for logging and display; execution uses the same contract from `pick_best_call()`.  
*Source: `core/state_store.py` — `set_option_reco()`, `get_option_reco()`; `core/engine.py` — options BUY path.*

---

## cooldown

A period after an exit during which the bot will not open a new position in that symbol (or underlying). Stored in `state["cooldowns"]` as `{ key: { "until": iso_datetime, "reason": str } }`. Per-symbol/underlying cooldowns are set on exit (from profile or portfolio exit); portfolio cooldown is a single `portfolio_cooldown_until` and blocks all new entries.  
*Source: `core/state_store.py` — `set_cooldown()`, `get_cooldown()`, `_is_in_cooldown_local()`; `core/engine.py` — set after SELL, check before BUY.*

---

## MFE / MAE

**Maximum Favorable Excursion / Maximum Adverse Excursion.** For an open position, the highest and lowest price (or mid) since entry. Stored as percentages of entry: MFE% = (high - entry)/entry, MAE% = (low - entry)/entry. Recorded in state in `mfe_mae_tracker[key]` (high/low), updated each cycle; on close, computed and written to the ledger (mfe_pct, mae_pct) then cleared. Used for post-trade analysis (e.g. EOD report, UI).  
*Source: `core/state_store.py` — `init_mfe_mae_tracker()`, `update_mfe_mae_tracker()`, `get_and_clear_mfe_mae()`; `core/engine.py` — call sites and `append_trade(..., mfe_pct=, mae_pct=)`; `core/ledger.py` — TRADE_LEDGER_FIELDS.*

---

## profit lock (portfolio)

A portfolio-level rule: if unrealized PnL as a fraction of invested notional is >= `PORTFOLIO_PROFIT_LOCK_PCT`, the bot exits all positions and sets portfolio cooldown. Used to lock in gains.  
*Source: `config/settings.py` — `PORTFOLIO_PROFIT_LOCK_PCT`; `core/engine.py` — `_compute_portfolio_invested_and_pnl()`, portfolio check, `_exit_all_positions()`.*

---

## loss limit (portfolio)

A portfolio-level rule: if unrealized PnL as a fraction of invested notional is <= `-PORTFOLIO_LOSS_LIMIT_PCT`, the bot exits all positions and sets portfolio cooldown. Used to cap drawdown.  
*Source: `config/settings.py` — `PORTFOLIO_LOSS_LIMIT_PCT`; `core/engine.py` — same as profit lock.*

---

## kill switch

A single config flag (`KILL_SWITCH` in `config/settings.py`). When False, the bot does not place any order; strategies still run and signals are logged ("SKIP: kill_switch_off").  
*Source: `config/settings.py`; `core/engine.py` — `can_place_orders`.*

---

## reconciliation

The process of aligning `state/state.json` with the broker: for each watchlist symbol/underlying, broker positions are read and state positions are created, updated, or cleared so that state matches broker (broker is source of truth). Runs on startup and at the start of every cycle.  
*Source: `main.py` — startup; `core/engine.py` — `reconcile_equity_positions_from_broker()`, `reconcile_option_positions_from_broker()`.*

---

## ledger

The append-only log of executed fills: `logs/trades.csv`. Each row is one fill (time, asset_type, symbol, side, qty, price, strategy_version, profile, signal_reason, exit_reason, mfe_pct, mae_pct, entry_snapshot_json).  
*Source: `core/ledger.py` — `append_trade()`, `TRADE_LEDGER_FIELDS`; `config/settings.py` — `TRADES_LEDGER_CSV`.*

---

## EOD report

End-of-day report generated by `scripts/generate_eod_report.py`: reads `logs/trades.csv`, filters by date, pairs BUY/SELL for PnL, and writes `logs/daily_report_YYYY-MM-DD.md` with summary, exit reason distribution, best/worst trades, and full trade list.  
*Source: `scripts/generate_eod_report.py`.*

---

## Source of truth

- Terms above are defined by usage in: `config/settings.py`, `config/watchlist.yaml`, `config/profiles.yaml`, `core/models.py`, `core/state_store.py`, `core/engine.py`, `core/ledger.py`, `strategies/lev_etf_trend.py`, `strategies/options_overlay.py`, `scripts/generate_eod_report.py`.

---

## What can go wrong

- **Ambiguous term:** If a term is used differently in code vs docs, the code is the source of truth; update docs or code for consistency.
- **New features:** New state keys or ledger columns should be reflected in the glossary and in [Architecture](architecture.md) / [Design](design.md) as needed.

---

## How to verify

- Search codebase for the term (e.g. "cooldown", "reconcile", "MFE") to find definitions and usage.
- Cross-check with the referenced file and function names in this glossary.
