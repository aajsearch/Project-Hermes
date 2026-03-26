## Purpose
This doc is the parity contract for migrating **legacy (V1) Kalshi hourly** into the **V2 unified pipeline**.
It maps legacy config + behavior to V2 config + strategy responsibilities, and defines the acceptance criteria we
validate during rollout.

## Legacy sources of truth
- **Config**:
  - `config/common.yaml` (global + hourly caps + hourly order defaults + `strategy.pick_all_in_range`)
  - `config/hourly.yaml` (hourly schedule window, hourly exit criteria, hourly risk guards, spot window, thresholds, hourly_last_90s_limit_99)
- **Loader behavior**: `bot/config_loader.py` deep-merges and **flattens** `hourly.yaml.schedule.entry_window` into `cfg["schedule"]`,
  and promotes `schedule.exit_criteria` into `cfg["exit_criteria"]["hourly"]`.
- **Legacy hourly engines**:
  - Regular hourly loop: `bot/main.py` (`run_bot_for_asset`)
  - Hourly last-90s thread: `bot/hourly_last_90s_strategy.py`

## V2 targets
- **Config**: `config/v2_common.yaml` + `config/v2_hourly.yaml` loaded by `bot/v2_config_loader.py` (strict schema).
- **Engine**: `bot/v2_main.py` hourly thread calls `bot/pipeline/run_unified.py:run_pipeline_cycle(interval="hourly")`.
- **Persistence**: `data/v2_state.db` via `bot/pipeline/registry.py:OrderRegistry` (+ any hourly telemetry tables).
- **Rollout**: per-asset feature flag (default off).

## Parity matrix (config)

### Interval enablement and asset universe
- **Legacy**: `common.yaml: intervals.hourly.enabled`, `intervals.hourly.assets`
- **V2**: `v2_common.yaml: intervals.hourly.enabled`, `intervals.hourly.assets`
- **Acceptance**:
  - Hourly pipeline thread can be enabled/disabled globally via `intervals.hourly.enabled`.
  - Actual trading eligibility is further gated per-asset via feature flag (V2-only requirement).

### Regular hourly: entry window gating
- **Legacy**: `hourly.yaml: schedule.entry_window.late_window_minutes`, `late_interval_seconds`
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.entry_window.*` (new block)
- **Acceptance**:
  - V2 regular-hourly entry intents **only** appear when `minutes_to_close <= late_window_minutes`.
  - V2 evaluation cadence respects `late_interval_seconds` (or an equivalent single `run_interval_seconds` + internal gating).

### Regular hourly: exit criteria cadence + thresholds
- **Legacy**: `hourly.yaml: schedule.exit_criteria.*` (stop loss / panic stop loss / persistence polls / evaluation interval)
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.exit_criteria.*` (new block)
- **Acceptance**:
  - Exit evaluation happens no more frequently than legacy `evaluation_interval_seconds` per open order/position.
  - Stop-loss / panic stop-loss thresholds match legacy meaning (same percent-of-entry or equivalent definition used in V1).

### Regular hourly: thresholds band (YES/NO)
- **Legacy**: `hourly.yaml: thresholds.yes_min/yes_max/no_min/no_max`
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.thresholds.*`
- **Acceptance**:
  - V2 eligible tickers set matches legacy when fed the same market snapshot and spot.
  - YES and NO selection uses the same cents band semantics.

### Regular hourly: spot window filter
- **Legacy**: `hourly.yaml: spot_window`, `spot_window_by_asset`
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.spot_window.*`
- **Acceptance**:
  - For a given spot price, eligible tickers are filtered to the same neighborhood as `fetch_eligible_tickers` in V1.

### Regular hourly: pick-all-in-range vs farthest
- **Legacy**: `common.yaml: strategy.pick_all_in_range` (hourly only)
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.pick_all_in_range` (strategy-local)
- **Acceptance**:
  - When enabled, V2 produces intents for all tickers in band (subject to guards/caps).
  - When disabled, V2 produces only the single “farthest” signal (legacy default semantics for daily/weekly).

### Regular hourly: hourly risk guards
- **Legacy**: `hourly.yaml: schedule.hourly_risk_guards.*`
  - `no_new_entry_cutoff_seconds`, `persistence_polls`, `stop_once_disable`, `recent_cross`, `distance_buffer`, `anchor_one_per_side`,
    `stoploss_roll_reentry`, `hard_flip_exit`
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_signals_farthest.risk_guards.*`
- **Acceptance**:
  - Each enabled guard suppresses/permits entries and exits in the same circumstances as V1.
  - V2 logs guard decisions (skip reasons) to hourly telemetry so parity debugging is possible.

### Hourly last-90s limit-99 strategy (separate in V1)
- **Legacy**: `hourly.yaml: hourly_last_90s_limit_99.*`
- **V2**: `v2_hourly.yaml: hourly.strategies.hourly_last_90s_limit_99.*`
- **Acceptance**:
  - V2 places intents only inside the configured last-window (`window_seconds`) and at `run_interval_seconds`.
  - Limits and filters match: `limit_price_cents`, `min_bid_cents`, side rules, per-market caps, min distance at placement.
  - Stop-loss gating matches: `stop_loss_pct` and `stop_loss_distance_factor` relative to `min_distance_at_placement`.

### Caps (per ticker / per hour) and scope
- **Legacy**: `common.yaml: caps.hourly.*`, `caps.cap_scope`
- **V2**: `v2_common.yaml: caps.hourly.*`, `caps.cap_scope`
- **Acceptance**:
  - V2 enforces max orders per ticker and max total per hour (for hourly interval).
  - When caps suppress orders, we log a clear reason in telemetry.

### No-trade windows
- **Legacy**: `common.yaml: no_trade_windows.*` (global)
- **V2**: `v2_common.yaml: no_trade_windows.*`
- **Acceptance**:
  - Entries are suppressed during no-trade windows; exits remain allowed (legacy intent).

## Parity matrix (implementation)

### Market / window selection
- **Legacy**: `bot/market.py:get_current_hour_market_id`, `get_market_context`
- **V2**: `bot/pipeline/run_unified.py` already uses `get_current_hour_market_id` for non-15m intervals
- **Acceptance**:
  - For a given asset + clock time, V2 targets the same market_id as legacy hourly.

### Entry selection algorithm
- **Legacy**: `bot/strategy.py:generate_signals_farthest` fed by `bot/market.py:fetch_eligible_tickers`
- **V2**: `HourlySignalsFarthestStrategy.evaluate_entry` (either reuses those helpers directly or ports logic)
- **Acceptance**:
  - Given identical inputs, V2 produces the same (ticker, side, price) candidates.

### Exit algorithm
- **Legacy**: `bot/exit_criteria.py:run_exit_criteria` (interval=\"hourly\")
- **V2**: `HourlySignalsFarthestStrategy.evaluate_exit` and/or executor-side exit helpers
- **Acceptance**:
  - Stop-loss/panic/roll/hard-flip behaviors are either parity-implemented or explicitly disabled with config + telemetry evidence.

## Validation checklist (what we must verify)
- **Boot validation**: strict V2 config schema plus strategy-specific type/range checks.
- **Parity tests**: deterministic unit-style comparisons using recorded snapshots.
- **Dry-run integration**: `dry_run=true` (or execute=false) pipeline cycles log intended actions without placing orders.
- **DB validation**: `v2_order_registry`, `v2_strategy_reports`, `v2_tick_log`, and hourly telemetry tables populate correctly.
- **Canary rollout**: enable a single asset via feature flag; confirm full lifecycle; rollback by removing asset from enabled list.

