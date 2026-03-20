# Troubleshooting

Common errors, causes, and fixes. Commands are run from **project root** unless stated otherwise.

---

## UUID not JSON serializable

**Symptom:** Error when saving state (e.g. "Object of type UUID is not JSON serializable").

**Cause:** Order ID (UUID) was stored in state without converting to string.

**Fix:** Order IDs are already converted at source: `broker/alpaca/orders.py` returns `str(o.id)`. Pending order dict stores `"order_id": str(order_id)`. If you added code that stores a raw UUID, convert it to string before putting in state. State store uses `JSONEncoder` in `core/state_store.py` to handle UUID if any slip through.

**Verify:**
```bash
grep -n "o.id\|order_id" broker/alpaca/orders.py core/engine.py
# Should see str(order_id) or str(o.id)
```

---

## Repeated buys after restart

**Symptom:** Bot buys the same symbol again after restart even though broker already has a position.

**Cause:** State was deleted or out of sync; reconciliation did not run or watchlist does not include that symbol.

**Fix:**
1. Ensure reconciliation runs: log should show "Startup reconcile complete." and at start of each cycle state is reconciled in `run_cycle()`.  
2. Do not delete `state/state.json` without reason; if you do, restart once so reconciliation repopulates from broker.  
3. Ensure the symbol is in `config/watchlist.yaml` under `equities` (or underlying under `options`) so reconciliation and engine process it.

**Verify:**
```bash
# Broker positions (use Alpaca dashboard or API)
# State after one cycle
cat state/state.json | python3 -m json.tool | head -80
```
Check `positions` has the symbol with correct qty. If state is empty but broker has position, next cycle should repopulate (reconcile runs at start of cycle).

---

## Market closed spam logs

**Symptom:** Log fills with "Market closed (equities/options). Waiting..." every 5 minutes.

**Cause:** Normal behavior when market is closed; the loop wakes every 5 minutes, checks `is_market_open_now()`, and logs once.

**Fix:** No code fix required. To reduce log volume you could change the logger to log at debug for that message, or increase sleep (would require changing `core/scheduler.py` to support configurable cadence).

**Verify:** Check `config/settings.py`: `MARKET_OPEN_HHMM`, `MARKET_CLOSE_HHMM` (Pacific). Confirm your local time vs Pacific so you expect “Market closed” outside 6:30–13:00 PT.

---

## Missing options quotes / no option BUY

**Symptom:** Options signal is BUY but no order; log may say "reco ... has no ask" or no reco.

**Cause:** Option chain missing, or no contract passes filters (DTE/delta/IV/spread), or quote (bid/ask) missing for chosen contract.

**Fix:**  
- Check profile: `dte_min`, `dte_max`, `delta_min`, `delta_max`, `iv_max`, `spread_pct_max` in `config/profiles.yaml` for `options_call_overlay_v1`.  
- Alpaca options data: OPRA feed may require subscription; code falls back to INDICATIVE.  
  *Source: `options/selector.py` — `_get_chain()` tries OPRA then INDICATIVE.*

**Verify:**
```bash
# Run options pick test if present
python scripts/options_pick_test.py
# Or inspect logs for "[underlying] fetching option chain..." and any exception
grep -i "option\|reco\|chain" logs/events.log | tail -30
```

---

## State corruption

**Symptom:** JSON decode error when loading state, or inconsistent keys.

**Cause:** Crash during write, or manual edit that broke JSON, or missing keys in old state file.

**Fix:**  
- State is written atomically (tmp + replace) in `core/state_store.py`; partial writes should not leave a broken file. If the file is corrupted, restore from backup or delete it and let reconciliation repopulate from broker (you may lose local entry_price/entry_time for repopulated positions).  
- Load self-heals missing keys: `load_state()` does `raw.setdefault("positions", {})`, etc. So an old state file missing new keys should still load.

**Verify:**
```bash
python3 -c "
from core.state_store import load_state
s = load_state()
print('keys:', list(s.keys()))
print('positions count:', len(s.get('positions', {})))
"
```

---

## Mismatched broker vs state

**Symptom:** Broker shows a position but state does not, or the opposite.

**Cause:** Symbol not in watchlist (reconcile only considers watchlist symbols/underlyings); or reconcile failed for that symbol (exception); or state was edited.

**Fix:**  
1. Add symbol/underlying to `config/watchlist.yaml` if you want it tracked.  
2. Run one cycle; reconcile runs at start. Check log for "reconcile error" for that symbol.  
3. For options, if broker has multiple contracts per underlying, state holds one (largest qty); see [Architecture](architecture.md).

**Verify:**
```bash
# State positions
cat state/state.json | grep -A 20 '"positions"'

# Compare with broker (Alpaca paper dashboard or API)
# Equities: symbols in watchlist should match
# Options: underlyings in watchlist; state key OPT:<underlying>
```

---

## Kill switch not working / orders still placed

**Symptom:** You set `KILL_SWITCH = False` but orders are placed.

**Cause:** Wrong config file, or process using cached import, or typo (e.g. `Kill_Switch`).

**Fix:** Set in `config/settings.py`: `KILL_SWITCH = False` (capital letters). Restart the process so it reloads config.

**Verify:**
```bash
python3 -c "from config.settings import KILL_SWITCH; print('KILL_SWITCH', KILL_SWITCH)"
# Should print False
# Run one cycle; log should show "SKIP: kill_switch_off"
```

---

## EOD report empty or wrong date

**Symptom:** `generate_eod_report.py` writes a file but summary is empty or for wrong day.

**Cause:** No rows in `logs/trades.csv` for that date (time column format YYYY-MM-DD); or you passed wrong date.

**Fix:** Use explicit date: `python scripts/generate_eod_report.py 2026-02-09`. Trades are filtered by `(row.get("time") or "")[:10] == date_str`. Ensure ledger has `time` in ISO format (e.g. 2026-02-09T14:30:00).

**Verify:**
```bash
head -5 logs/trades.csv
python scripts/generate_eod_report.py 2026-02-09
cat logs/daily_report_2026-02-09.md | head -20
```

---

## Portfolio lock triggered, then no new trades

**Symptom:** After a big loss or profit, bot exits all and then does not open new positions for a while.

**Cause:** Intended: after portfolio profit lock or loss limit, `set_portfolio_cooldown()` is called; `can_place_orders` is False until `portfolio_cooldown_until` is in the past.

**Fix:** Wait for cooldown (e.g. 60 minutes per `PORTFOLIO_COOLDOWN_MINUTES`), or temporarily clear cooldown in state for testing (remove or set `portfolio_cooldown_until` to past time). Do not do this in production without understanding risk.

**Verify:**
```bash
cat state/state.json | grep portfolio_cooldown_until
# Log should show "SKIP: portfolio_lock (cooldown active)"
```

---

## Source of truth

- State: `core/state_store.py` — `load_state()`, `save_state()`, `_default_state()`
- Reconciliation: `core/engine.py` — `reconcile_equity_positions_from_broker()`, `reconcile_option_positions_from_broker()`
- Kill switch: `config/settings.py` — `KILL_SWITCH`; `core/engine.py` — `can_place_orders`
- EOD: `scripts/generate_eod_report.py` — date filter and PnL pairing

---

## What can go wrong

- **Grep / cat fails:** Paths assume project root; use full path or `cd` to project root first.
- **State has pending_orders from stuck order:** If an order is stuck "accepted" (e.g. market closed), state keeps the pending order; next cycle may retry (e.g. portfolio exit uses short timeout and leaves pending). Manually clearing pending_orders in state can cause duplicate SELL if the first order later fills; prefer letting the bot retry or cancel via broker.

---

## How to verify

- Run `python main.py` and confirm no Python tracebacks; check logs for expected SKIP or order messages.
- After any config change, restart the process and re-run the verification commands above as needed.
