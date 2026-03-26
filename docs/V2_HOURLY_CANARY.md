## V2 hourly canary (BTC-only) and rollback

### Safety model
- **Hourly is gated by feature flag**: `config.feature_flags.v2_hourly.enabled_assets`.
  - If empty, hourly does nothing even if `intervals.hourly.enabled: true`.
- Optional: `config.feature_flags.v2_hourly.shadow_mode: true` forces hourly to **dry-run** (no orders) even if global `dry_run: false`.

### One-shot dry-run canary (recommended)
Runs a single hourly pipeline cycle and exits (no threads).

```bash
python3 tools/v2_hourly_canary_once.py --asset btc --shadow --enable-strategies
python3 tools/verify_v2_dry_run.py
```

Notes:
- If you see `No Kalshi auth (KALSHI_API_KEY/KALSHI_PRIVATE_KEY)` in logs, the cycle will likely skip market fetches.
- Hourly telemetry tables populate only when hourly markets are fetched and evaluated:
  - `v2_telemetry_hourly_signals`
  - `v2_telemetry_hourly_last90s`

### Full bot canary (threads)
1) In `config/v2_common.yaml`:
- Set `intervals.hourly.enabled: true`
- Set `feature_flags.v2_hourly.enabled_assets: [btc]`
- (Optional) Set `feature_flags.v2_hourly.shadow_mode: true`

2) In `config/v2_hourly.yaml`:
- Set `hourly.strategies.hourly_signals_farthest.enabled: true`
- Set `hourly.strategies.hourly_last_90s_limit_99.enabled: true`

3) Start:

```bash
python3 -m bot.v2_main
```

### Rollback (instant)
- Remove `btc` from `feature_flags.v2_hourly.enabled_assets` (or set it to `[]`).
- Hourly stops evaluating/placing immediately (next tick) while leaving 15-min unaffected.

