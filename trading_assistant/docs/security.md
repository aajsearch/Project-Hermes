# Security

API keys, paper-only default, guardrails, and logging. No implementation changes in this doc; it describes current behavior and recommendations.

---

## API keys and environment

- **Credentials:** Alpaca API key and secret are read from the environment: `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`.  
  *Source: `broker/alpaca/client.py` — `os.getenv("ALPACA_API_KEY")`, `os.getenv("ALPACA_SECRET_KEY")`.*

- **Loading:** `broker/alpaca/client.py` calls `load_dotenv()`, so a `.env` file in the current working directory is loaded. Do not commit `.env`; use `.env.example` (without real keys) as a template.

- **Failure:** If either env var is missing, `make_trading_client()` and `make_stock_data_client()` raise `ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")`. Do not catch and ignore this in production.

**Recommendation:** Keep keys only in environment or a non-committed `.env`; restrict file permissions (`chmod 600 .env`). Do not log or print API keys or secrets.

---

## Paper only by default

- **Trading client:** In `main.py`, the bot uses `make_trading_client(paper=True)`, so all orders go to Alpaca **paper** by default.  
  *Source: `main.py`; `broker/alpaca/client.py` — `TradingClient(API_KEY, SECRET_KEY, paper=paper)`.*

- **Live trading:** To use live trading you would have to change the code to `paper=False` and use live credentials. Not recommended unless you explicitly intend to trade real money.

**Recommendation:** Keep paper=True for all normal and test runs. If you ever add a live mode, gate it (e.g. env flag) and document it clearly.

---

## Guardrails

- **Kill switch:** `config/settings.py` has `KILL_SWITCH`. When `False`, the bot does not place any order (signals and logging still run). Use this to disable execution without stopping the process.  
  *Source: `core/engine.py` — `can_place_orders = (KILL_SWITCH is True) and ...`.*

- **Market closed:** Execution is gated by `is_market_open_now()` (Pacific) and by `ALLOW_EXECUTION_WHEN_CLOSED` / `ALLOW_OPTION_EXECUTION_WHEN_CLOSED` (default False). So by default no orders when the market is closed.  
  *Source: `core/scheduler.py`; `core/engine.py` — `can_execute`, `can_execute_opt`.*

- **Portfolio cooldown:** After a portfolio-level exit (profit lock or loss limit), no new entries until `portfolio_cooldown_until` has passed.  
  *Source: `core/engine.py` — `is_portfolio_in_cooldown(state)`.*

These are operational safeguards; they do not replace proper key and account security.

---

## Logging and PII

- **What is logged:** Startup message, account buying_power/cash (numbers only), symbol, signal, SKIP reasons, order IDs (UUIDs as strings), errors. No code path should log the raw API secret.  
  *Source: `core/logger.py` — `log()`; `core/engine.py` — log calls; `broker/alpaca/client.py` — no log of SECRET_KEY.*

- **Recommendation:** Do not log `ALPACA_SECRET_KEY` or full API key. If you add new log lines, avoid including secrets or sensitive PII. Rotate keys if they are ever exposed.

---

## State and ledger files

- **State:** `state/state.json` contains positions, order IDs, cooldowns, and option recos. It does not contain API keys. Restrict read/write to the process user.  
- **Ledger:** `logs/trades.csv` contains trade data (symbol, qty, price, profile, etc.). No secrets. Same recommendation: restrict access.

---

## Source of truth

- Env and clients: `broker/alpaca/client.py` — `load_dotenv()`, `os.getenv()`, `make_trading_client(paper=True)`
- Kill switch and execution gating: `config/settings.py`; `core/engine.py` — `can_place_orders`, `can_execute`, `can_execute_opt`
- Logger: `core/logger.py` — `log()`

---

## What can go wrong

- **Keys in repo:** Never commit `.env` or any file with real keys. Use `.gitignore` for `.env` and `.env.*` (except `.env.example`).
- **Logging secrets:** A future code change might log request params; review any log that includes request bodies or headers.

---

## How to verify

- `grep -r "SECRET_KEY\|API_KEY" --include="*.py" .` should not show printing or logging of secret.
- Confirm `main.py` uses `make_trading_client(paper=True)`.
- Run with `KILL_SWITCH = False` and confirm no orders are placed (see [Troubleshooting](troubleshooting.md)).
