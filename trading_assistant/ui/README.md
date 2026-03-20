# Trading Bot Dashboard (Streamlit)

Lightweight, local Streamlit UI for viewing trades, signals, state, and KPIs. Read-only; no trading logic is modified.

## Requirements

- Python 3.8+
- `streamlit`, `pandas`, `matplotlib`

## Install

```bash
pip install streamlit pandas matplotlib
```

## Run

From the **project root** (the directory that contains `ui/`, `logs/`, `state/`, and `config/`):

```bash
python3 -m streamlit run ui/app.py
```

If the `streamlit` command is on your PATH you can instead use:

```bash
streamlit run ui/app.py
```

The app will open in your browser (default: http://localhost:8501).

## Data sources

- **Ledger:** `logs/trades.csv` (time, symbol, asset_type, side, qty, price, profile, strategy_version, signal_reason, exit_reason, mfe_pct, mae_pct)
- **Signals:** `logs/signals.csv`
- **State:** `state/state.json` (positions, cooldowns, option_recos)
- **Daily report (optional):** `logs/daily_report_YYYY-MM-DD.md`

Missing files are handled gracefully (empty tables and messages).

## Features

- **Date filter:** Default today; filters trades by the `time` field.
- **Symbol / asset type / profile filters** in the sidebar.
- **Completed trades:** BUY paired with the next SELL for the same symbol (and asset_type when present) within the selected date.
- **Open trades:** BUY without a matching SELL in the selected date.
- **PnL:** Realized PnL $ and %; multiplier 1 for equities, 100 for options.
- **KPIs:** Total PnL $, win rate, # completed trades, avg win/loss, profit factor, max drawdown on realized equity curve.
- **Charts:** Exit reason bar, realized equity curve, MFE vs PnL scatter, MAE vs PnL scatter.
- **Tables:** Completed trades, open trades, best 5 / worst 5 by PnL $.
- **State snapshot:** Positions, cooldowns, option recos (expandable).
- **Sanity checks:** Today’s BUY signal count vs BUY fills; warnings if state and trades are inconsistent.
- **Export:** Download filtered completed trades as CSV; export markdown summary to `logs/ui_summary_YYYY-MM-DD.md`.
