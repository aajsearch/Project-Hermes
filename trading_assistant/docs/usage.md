# Usage

How to run the bot locally, under PM2, recommended workflow, modes, and commands.

---

## Running locally

1. **From project root:**
   ```bash
   python main.py
   ```
   Or explicitly:
   ```bash
   python3 main.py
   ```
   Requires `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` in environment (e.g. `.env` via python-dotenv).  
   *Source: `broker/alpaca/client.py` — `load_dotenv()`, `os.getenv()`.*

2. **Behavior:**  
   - Connects to Alpaca **paper** (see [Security](security.md)).  
   - Loads `config/watchlist.yaml` and `config/profiles.yaml`.  
   - Runs startup reconciliation, then loop: if market open (Pacific), runs one cycle; else logs "Market closed. Waiting..." and sleeps until next 5-minute boundary.  
   *Source: `main.py`.*

3. **Stop:** Ctrl+C in the terminal.

---

## Running under PM2

PM2 is not part of the repo; you can use it to keep the bot running in the background.

**Example (run from project root):**
```bash
pm2 start main.py --name trading_assistant --interpreter python3
pm2 logs trading_assistant
pm2 stop trading_assistant
pm2 restart trading_assistant
```

**ASSUMPTION:** No `ecosystem.config.js` or PM2 config is in the repo; the above is a minimal way to run. Verify working directory and env (e.g. `env` file) when using PM2 so the process sees `config/`, `state/`, `logs/` and env vars.

---

## Modes

| Mode | How | Effect |
|------|-----|--------|
| **Signals only (no orders)** | `KILL_SWITCH = False` in `config/settings.py` | Strategies run, signals appended to `logs/signals.csv`, log "SKIP: kill_switch_off"; no orders. |
| **Paper execution** | Default: `make_trading_client(paper=True)` in `main.py` | Orders go to Alpaca paper. |
| **Equities only** | `EXECUTE_OPTION_TRADES = False` | Option signals run but no option orders. |
| **Options only** | `EXECUTE_TRADES = False` | Equity signals run but no equity orders. |
| **Single cycle** | `RUN_ONCE = True` | One cycle then exit. |
| **Force run when closed** | `FORCE_RUN_WHEN_MARKET_CLOSED = True` | Cycle runs even when market closed (last available data). Use with `ALLOW_EXECUTION_WHEN_CLOSED` / `ALLOW_OPTION_EXECUTION_WHEN_CLOSED` for after-hours testing. |

*Source: `main.py`; `core/engine.py` — `can_place_orders`, `can_execute`, `can_execute_opt`; `config/settings.py`.*

---

## Commands

| Command | Purpose |
|---------|--------|
| `python main.py` | Run bot (loop until stopped). |
| `python scripts/generate_eod_report.py` | EOD report for **today** → `logs/daily_report_YYYY-MM-DD.md`. |
| `python scripts/generate_eod_report.py 2026-02-09` | EOD report for given date. |
| `python3 -m streamlit run ui/app.py` | Start Streamlit dashboard (from project root). Browser opens at http://localhost:8501. |

**EOD report:** Reads `logs/trades.csv`, filters by date, pairs BUY/SELL for PnL, writes markdown to `logs/daily_report_YYYY-MM-DD.md`.  
*Source: `scripts/generate_eod_report.py`.*

**Streamlit UI:** Read-only; loads `logs/trades.csv`, `logs/signals.csv`, `state/state.json`; date/symbol/asset/profile filters, KPIs, charts, tables, state snapshot, sanity checks, export.  
*Source: `ui/app.py`, `ui/data.py`; see `ui/README.md`.*

---

## Recommended workflow

1. **Before market open:** Ensure `KILL_SWITCH = True`, `FORCE_RUN_WHEN_MARKET_CLOSED = False`, `RUN_ONCE = False`. Check `.env` and paper credentials.
2. **Start:** `python main.py` (or PM2). Confirm "Startup reconcile complete." and then cycle or "Market closed. Waiting...".
3. **During day:** Monitor `logs/` (or Streamlit). No need to restart unless you change config or code.
4. **After close:** Run `python scripts/generate_eod_report.py` for the date; review `logs/daily_report_YYYY-MM-DD.md`.
5. **Testing:** Use `RUN_ONCE = True` or `KILL_SWITCH = False` as in [README — How to safely test changes](../README.md#how-to-safely-test-changes).

---

## Source of truth

- Entry point: `main.py` — `main()`
- Scheduler: `core/scheduler.py` — `is_market_open_now()`, `sleep_until_next_5min()`
- EOD: `scripts/generate_eod_report.py` — `main()`
- UI: `ui/app.py` (and `ui/data.py`)

---

## What can go wrong

- **ModuleNotFoundError:** Run from project root so `config/`, `core/`, `broker/`, etc. are on the path. If using `python -m`, e.g. `python -m streamlit run ui/app.py`, project root is typically the cwd.
- **Streamlit not found:** Use `python3 -m streamlit run ui/app.py` or ensure `streamlit` is installed for the same Python (e.g. `pip3 install streamlit`).
- **EOD report empty:** No trades in `logs/trades.csv` for that date, or CSV missing/empty.

---

## How to verify

- Run `python main.py`; expect "Trading Assistant started...", "Paper account buying_power=...", "Startup reconcile complete.", then cycle or "Market closed. Waiting...".
- Run `python scripts/generate_eod_report.py`; expect "Wrote logs/daily_report_YYYY-MM-DD.md".
- Run `python3 -m streamlit run ui/app.py`; expect browser to open and dashboard to load (or message if files missing).
