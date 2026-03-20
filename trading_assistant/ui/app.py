"""
Streamlit dashboard for the trading bot. Read-only; no trading logic changes.
Run from project root: streamlit run ui/app.py
"""
import io
import sys
from pathlib import Path
from datetime import datetime

# Ensure project root is on path when running: streamlit run ui/app.py
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from ui.data import (
    PROJECT_ROOT,
    load_trades_csv,
    load_signals_csv,
    load_state_json,
    trades_for_date,
    pair_trades,
    pnl_metrics,
    exit_reason_counts,
    equity_curve,
    mfe_mae_scatter_data,
    sanity_buy_counts,
)

st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")

st.title("Trading Bot Dashboard")

# --- Data load (with graceful missing files) ---
trades_df = load_trades_csv()
signals_df = load_signals_csv()
state = load_state_json()

if trades_df.empty and not Path(PROJECT_ROOT / "logs" / "trades.csv").exists():
    st.info("No trades file found at `logs/trades.csv`. Showing empty data.")
if signals_df.empty and not Path(PROJECT_ROOT / "logs" / "signals.csv").exists():
    st.caption("No signals file at `logs/signals.csv`.")
if not state and not Path(PROJECT_ROOT / "state" / "state.json").exists():
    st.caption("No state file at `state/state.json`.")

# --- Sidebar filters ---
st.sidebar.header("Filters")
date_str = st.sidebar.date_input(
    "Date",
    value=datetime.now().date(),
    key="date_filter",
).strftime("%Y-%m-%d")

# Unique values from trades (for selected date)
trades_date = trades_for_date(trades_df, date_str)
symbols = ["All"] + sorted(trades_date["symbol"].dropna().astype(str).unique().tolist()) if not trades_date.empty else ["All"]
asset_types = ["All"] + sorted(trades_date["asset_type"].dropna().astype(str).unique().tolist()) if not trades_date.empty else ["All"]
profiles = ["All"] + sorted(trades_date["profile"].dropna().astype(str).unique().tolist()) if not trades_date.empty else ["All"]
symbol_choice = st.sidebar.selectbox("Symbol", symbols, key="sym")
asset_choice = st.sidebar.selectbox("Asset type", asset_types, key="asset")
contract_type_choice = st.sidebar.selectbox("Contract type (options)", ["All", "CALL", "PUT"], key="ct")
profile_choice = st.sidebar.selectbox("Profile", profiles, key="profile")

symbol_filter = None if symbol_choice == "All" else [symbol_choice]
asset_filter = None if asset_choice == "All" else [asset_choice]
profile_filter = None if profile_choice == "All" else [profile_choice]
contract_type_filter = None if contract_type_choice == "All" else [contract_type_choice.strip().upper()]

# --- Paired trades for selected date and filters ---
completed, open_trades = pair_trades(
    trades_date,
    symbol_filter=symbol_filter,
    asset_filter=asset_filter,
    profile_filter=profile_filter,
    contract_type_filter=contract_type_filter,
)

metrics = pnl_metrics(completed)

# --- KPI cards ---
st.subheader("KPIs")
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("Total realized PnL ($)", f"{metrics['total_pnl_$']:.2f}")
with col2:
    st.metric("Win rate (%)", f"{metrics['win_rate_pct']:.1f}")
with col3:
    st.metric("Completed trades", metrics["n_completed"])
with col4:
    st.metric("Avg win ($)", f"{metrics['avg_win_$']:.2f}")
with col5:
    st.metric("Avg loss ($)", f"{metrics['avg_loss_$']:.2f}")
with col6:
    st.metric("Profit factor", f"{metrics['profit_factor']:.2f}")

st.caption(f"Max drawdown (realized): ${metrics['max_drawdown_$']:.2f}")

# --- Options: CALL vs PUT summary ---
opt_completed = [t for t in completed if (t.get("asset_type") or "").strip().upper() == "OPTION"]
if opt_completed:
    st.subheader("Options: CALL vs PUT")
    call_t = [t for t in opt_completed if (t.get("contract_type") or "").strip().upper() == "CALL"]
    put_t = [t for t in opt_completed if (t.get("contract_type") or "").strip().upper() == "PUT"]
    oc1, oc2 = st.columns(2)
    with oc1:
        if call_t:
            c_pnl = sum(x["pnl_$"] for x in call_t)
            c_wins = len([x for x in call_t if x["pnl_$"] > 0])
            st.metric("CALL exits", len(call_t), f"PnL $ {c_pnl:.2f} | Win rate {c_wins/len(call_t)*100:.1f}%")
        else:
            st.caption("CALL: no completed option trades")
    with oc2:
        if put_t:
            p_pnl = sum(x["pnl_$"] for x in put_t)
            p_wins = len([x for x in put_t if x["pnl_$"] > 0])
            st.metric("PUT exits", len(put_t), f"PnL $ {p_pnl:.2f} | Win rate {p_wins/len(put_t)*100:.1f}%")
        else:
            st.caption("PUT: no completed option trades")

# --- Charts ---
st.subheader("Charts")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    # Exit reason distribution
    exit_counts = exit_reason_counts(completed)
    if exit_counts:
        fig, ax = plt.subplots(figsize=(5, 3))
        reasons = list(exit_counts.keys())
        counts = list(exit_counts.values())
        ax.bar(reasons, counts, color="steelblue", edgecolor="navy", alpha=0.8)
        ax.set_title("Exit reason distribution")
        ax.set_ylabel("Count")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    else:
        st.caption("No exit reasons (no completed trades).")

with chart_col2:
    # Realized equity curve
    times, cum = equity_curve(completed)
    if times and cum:
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.plot(range(len(cum)), cum, color="green", marker="o", markersize=3)
        ax.axhline(0, color="gray", linestyle="--")
        ax.set_title("Realized equity curve")
        ax.set_ylabel("Cumulative PnL ($)")
        ax.set_xlabel("Trade index")
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    else:
        st.caption("No equity curve (no completed trades).")

# MFE / MAE scatter
mfe_x, mfe_y, mae_x, mae_y = mfe_mae_scatter_data(completed)
scatter_col1, scatter_col2 = st.columns(2)
with scatter_col1:
    if mfe_x and mfe_y:
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.scatter(mfe_x, mfe_y, alpha=0.7)
        ax.axhline(0, color="gray", linestyle="--")
        ax.axvline(0, color="gray", linestyle="--")
        ax.set_title("MFE % vs realized PnL %")
        ax.set_xlabel("MFE %")
        ax.set_ylabel("PnL %")
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    else:
        st.caption("No MFE data for scatter.")
with scatter_col2:
    if mae_x and mae_y:
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.scatter(mae_x, mae_y, alpha=0.7)
        ax.axhline(0, color="gray", linestyle="--")
        ax.axvline(0, color="gray", linestyle="--")
        ax.set_title("MAE % vs realized PnL %")
        ax.set_xlabel("MAE %")
        ax.set_ylabel("PnL %")
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    else:
        st.caption("No MAE data for scatter.")

# --- Tables ---
st.subheader("Completed trades")
if completed:
    comp_df = pd.DataFrame(completed)
    # Format for display
    def _pct_fmt(x):
        if x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip() == "":
            return ""
        try:
            return f"{float(x) * 100:.2f}%"
        except (TypeError, ValueError):
            return ""

    if "pnl_%" in comp_df.columns:
        comp_df["pnl_%"] = comp_df["pnl_%"].apply(_pct_fmt)
    if "mfe_pct" in comp_df.columns:
        comp_df["mfe_pct"] = comp_df["mfe_pct"].apply(_pct_fmt)
    if "mae_pct" in comp_df.columns:
        comp_df["mae_pct"] = comp_df["mae_pct"].apply(_pct_fmt)
    st.dataframe(comp_df, use_container_width=True, hide_index=True)
else:
    st.caption("No completed trades for the selected filters.")

st.subheader("Open trades (BUY without SELL in selected date)")
if open_trades:
    st.dataframe(pd.DataFrame(open_trades), use_container_width=True, hide_index=True)
else:
    st.caption("No open trades for the selected filters.")

st.subheader("Best 5 / Worst 5 by PnL ($)")
if completed:
    sorted_completed = sorted(completed, key=lambda x: x["pnl_$"], reverse=True)
    best5 = sorted_completed[:5]
    worst5 = sorted_completed[-5:] if len(sorted_completed) >= 5 else sorted_completed
    b5_df = pd.DataFrame(best5)
    w5_df = pd.DataFrame(worst5)
    disp_cols = [c for c in ["symbol", "asset_type", "contract_type", "sell_time", "pnl_$", "pnl_%", "exit_reason"] if c in b5_df.columns]
    if disp_cols:
        b5_df = b5_df[disp_cols]
        w5_df = w5_df[disp_cols]
    t1, t2 = st.columns(2)
    with t1:
        st.caption("Best 5")
        st.dataframe(b5_df, use_container_width=True, hide_index=True)
    with t2:
        st.caption("Worst 5")
        st.dataframe(w5_df, use_container_width=True, hide_index=True)
else:
    st.caption("No completed trades.")

# --- State snapshot ---
st.subheader("State snapshot")
if state:
    with st.expander("Positions", expanded=True):
        pos = state.get("positions") or {}
        if pos:
            st.json(pos)
        else:
            st.caption("No positions.")
    with st.expander("Cooldowns"):
        cd = state.get("cooldowns") or {}
        if cd:
            st.json(cd)
        else:
            st.caption("No cooldowns.")
    with st.expander("Option recos"):
        recos = state.get("option_recos") or {}
        if recos:
            st.json(recos)
        else:
            st.caption("No option recos.")
    with st.expander("Other (pending_orders, portfolio_cooldown_until, etc.)"):
        other = {k: v for k, v in state.items() if k not in ("positions", "cooldowns", "option_recos")}
        st.json(other)
else:
    st.caption("No state loaded.")

# --- Sanity checks ---
st.subheader("Sanity checks")
sanity = sanity_buy_counts(trades_df, signals_df, date_str, state)
st.write(f"**Date:** {date_str}")
st.write(f"BUY signals (today): **{sanity['buy_signals_today']}** | BUY fills from trades (today): **{sanity['buy_fills_today']}** | State position count: **{sanity['state_position_count']}**")
if sanity["warnings"]:
    for w in sanity["warnings"]:
        st.warning(w)
else:
    st.success("No sanity warnings.")

# --- Export ---
st.subheader("Export")
exp_col1, exp_col2 = st.columns(2)
with exp_col1:
    if completed:
        comp_export = pd.DataFrame(completed)
        buf = io.StringIO()
        comp_export.to_csv(buf, index=False)
        st.download_button(
            "Download completed trades (CSV)",
            data=buf.getvalue(),
            file_name=f"completed_trades_{date_str}.csv",
            mime="text/csv",
            key="dl_csv",
        )
    else:
        st.caption("No completed trades to export.")
with exp_col2:
    summary_lines = [
        f"# UI Summary {date_str}",
        "",
        "## KPIs",
        f"- Total realized PnL ($): {metrics['total_pnl_$']:.2f}",
        f"- Win rate (%): {metrics['win_rate_pct']:.1f}",
        f"- Completed trades: {metrics['n_completed']}",
        f"- Avg win ($): {metrics['avg_win_$']:.2f}",
        f"- Avg loss ($): {metrics['avg_loss_$']:.2f}",
        f"- Profit factor: {metrics['profit_factor']:.2f}",
        f"- Max drawdown ($): {metrics['max_drawdown_$']:.2f}",
        "",
        "## Sanity",
        f"- BUY signals (today): {sanity['buy_signals_today']}",
        f"- BUY fills (today): {sanity['buy_fills_today']}",
        f"- State positions: {sanity['state_position_count']}",
    ]
    summary_md = "\n".join(summary_lines)
    out_path = PROJECT_ROOT / "logs" / f"ui_summary_{date_str}.md"
    if st.button("Export markdown summary to logs/", key="export_md"):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            f.write(summary_md)
        st.success(f"Wrote {out_path}")
    st.download_button(
        "Download markdown summary",
        data=summary_md,
        file_name=f"ui_summary_{date_str}.md",
        mime="text/markdown",
        key="dl_md",
    )
