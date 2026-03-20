"""
CSV loaders and aggregations for the trading dashboard.
Read-only; no changes to trading logic or files (except optional export from UI).
"""
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd

# Default base path: project root (parent of ui/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_ts(s: Any) -> Optional[datetime]:
    """Parse timestamp robustly. Handles ISO format, date-only, and common variants."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    if isinstance(s, datetime):
        return s
    s = str(s).strip()
    if not s:
        return None
    # Try ISO with T
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(s[:26].rstrip("Z"), fmt)
        except ValueError:
            continue
    return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    if x is None or (isinstance(x, float) and pd.isna(x)) or x == "":
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


TRADES_CSV_COLUMNS = [
    "time", "asset_type", "contract_type", "symbol", "side", "qty", "price",
    "strategy_version", "profile", "signal_reason", "exit_reason",
    "mfe_pct", "mae_pct", "entry_snapshot_json",
]


def load_trades_csv(base_path: Optional[Path] = None) -> pd.DataFrame:
    """Load logs/trades.csv. Returns empty DataFrame with expected columns if missing."""
    base = base_path or PROJECT_ROOT
    path = base / "logs" / "trades.csv"
    cols = list(TRADES_CSV_COLUMNS)
    if not path.exists():
        return pd.DataFrame(columns=cols)
    try:
        # Use explicit column names so we handle files with or without header
        df = pd.read_csv(path, names=cols, header=None)
        # If file had a header row, first row will have time=="time"; drop it
        if len(df) > 0 and str(df.iloc[0].get("time", "")).strip().lower() == "time":
            df = df.iloc[1:].reset_index(drop=True)
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df
    except Exception:
        return pd.DataFrame(columns=cols)


def load_signals_csv(base_path: Optional[Path] = None) -> pd.DataFrame:
    """Load logs/signals.csv. Returns empty DataFrame if missing."""
    base = base_path or PROJECT_ROOT
    path = base / "logs" / "signals.csv"
    if not path.exists():
        return pd.DataFrame(columns=["time", "asset", "symbol", "profile", "signal", "reason"])
    try:
        df = pd.read_csv(path)
        for c in ["time", "asset", "symbol", "profile", "signal", "reason"]:
            if c not in df.columns:
                df[c] = None
        return df
    except Exception:
        return pd.DataFrame(columns=["time", "asset", "symbol", "profile", "signal", "reason"])


def load_state_json(base_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load state/state.json. Returns {} if missing or invalid."""
    base = base_path or PROJECT_ROOT
    path = base / "state" / "state.json"
    if not path.exists():
        return {}
    try:
        import json
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def trades_for_date(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Filter trades by date using the 'time' field. date_str = 'YYYY-MM-DD'."""
    if df.empty or "time" not in df.columns:
        return df.copy()
    df = df.copy()
    df["_date"] = df["time"].apply(lambda x: (parse_ts(x) or datetime.min).strftime("%Y-%m-%d"))
    return df[df["_date"] == date_str].drop(columns=["_date"], errors="ignore")


def _multiplier(asset_type: Any) -> float:
    if asset_type is None or pd.isna(asset_type):
        return 1.0
    return 100.0 if str(asset_type).strip().upper() == "OPTION" else 1.0


def pair_trades(
    df: pd.DataFrame,
    symbol_filter: Optional[List[str]] = None,
    asset_filter: Optional[List[str]] = None,
    profile_filter: Optional[List[str]] = None,
    contract_type_filter: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Pair BUY with next SELL for same symbol (and asset_type if present).
    Returns (completed_trades, open_trades).
    completed_trades: list of dicts with buy_time, sell_time, symbol, asset_type, qty, buy_price, sell_price,
                     pnl_$, pnl_%, exit_reason, mfe_pct, mae_pct, profile, strategy_version, etc.
    open_trades: list of dicts with buy_time, symbol, asset_type, qty, buy_price, profile, strategy_version.
    """
    if df.empty:
        return [], []

    df = df.sort_values("time").copy()
    if "asset_type" not in df.columns:
        df["asset_type"] = ""
    if "profile" not in df.columns:
        df["profile"] = ""
    if "strategy_version" not in df.columns:
        df["strategy_version"] = ""
    if "exit_reason" not in df.columns:
        df["exit_reason"] = ""
    if "mfe_pct" not in df.columns:
        df["mfe_pct"] = None
    if "mae_pct" not in df.columns:
        df["mae_pct"] = None
    if "contract_type" not in df.columns:
        df["contract_type"] = ""

    def apply_filters(gdf: pd.DataFrame) -> pd.DataFrame:
        if symbol_filter and "symbol" in gdf.columns:
            gdf = gdf[gdf["symbol"].astype(str).str.strip().isin(symbol_filter)]
        if asset_filter and "asset_type" in gdf.columns:
            gdf = gdf[gdf["asset_type"].astype(str).str.strip().isin(asset_filter)]
        if contract_type_filter and "contract_type" in gdf.columns:
            gdf = gdf[gdf["contract_type"].astype(str).str.strip().str.upper().isin(contract_type_filter)]
        if profile_filter and "profile" in gdf.columns:
            gdf = gdf[gdf["profile"].astype(str).str.strip().isin(profile_filter)]
        return gdf

    df = apply_filters(df)
    completed = []
    open_trades = []

    # Group by (symbol, asset_type) and pair BUY -> SELL in order
    key_cols = ["symbol", "asset_type"] if "asset_type" in df.columns else ["symbol"]
    for key, grp in df.groupby([df["symbol"].astype(str), df["asset_type"].astype(str)], dropna=False):
        sym, at = (key[0], key[1]) if isinstance(key, tuple) else (key, "")
        grp = grp.sort_values("time").reset_index(drop=True)
        i = 0
        while i < len(grp):
            row = grp.iloc[i]
            side = (row.get("side") or "").strip().upper()
            if side != "BUY":
                i += 1
                continue
            qty = _safe_float(row.get("qty"), 0)
            if qty <= 0:
                i += 1
                continue
            buy_price = _safe_float(row.get("price"), 0)
            buy_time = row.get("time")
            profile = row.get("profile") or ""
            strategy_version = row.get("strategy_version") or ""
            # look for next SELL
            j = i + 1
            while j < len(grp):
                sell_row = grp.iloc[j]
                if (sell_row.get("side") or "").strip().upper() == "SELL":
                    sell_qty = _safe_float(sell_row.get("qty"), 0)
                    if sell_qty <= 0:
                        sell_qty = qty
                    sell_price = _safe_float(sell_row.get("price"), 0)
                    mult = _multiplier(sell_row.get("asset_type") or at)
                    pnl_d = (sell_price - buy_price) * sell_qty * mult
                    pnl_pct = (sell_price - buy_price) / buy_price if buy_price and buy_price > 0 else None
                    ct = (sell_row.get("contract_type") or row.get("contract_type") or "").strip().upper() or ""
                    completed.append({
                        "buy_time": buy_time,
                        "sell_time": sell_row.get("time"),
                        "symbol": sym,
                        "asset_type": at or (sell_row.get("asset_type") or ""),
                        "contract_type": ct,
                        "qty": sell_qty,
                        "buy_price": buy_price,
                        "sell_price": sell_price,
                        "pnl_$": pnl_d,
                        "pnl_%": pnl_pct,
                        "exit_reason": (sell_row.get("exit_reason") or "").strip(),
                        "mfe_pct": sell_row.get("mfe_pct"),
                        "mae_pct": sell_row.get("mae_pct"),
                        "profile": profile,
                        "strategy_version": strategy_version,
                    })
                    i = j + 1
                    break
                j += 1
            else:
                ct = (row.get("contract_type") or "").strip().upper() or ""
                open_trades.append({
                    "buy_time": buy_time,
                    "symbol": sym,
                    "asset_type": at or (row.get("asset_type") or ""),
                    "contract_type": ct,
                    "qty": qty,
                    "buy_price": buy_price,
                    "profile": profile,
                    "strategy_version": strategy_version,
                })
                i += 1

    return completed, open_trades


def pnl_metrics(completed: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute KPIs from completed trades."""
    if not completed:
        return {
            "total_pnl_$": 0.0,
            "win_rate_pct": 0.0,
            "n_completed": 0,
            "avg_win_$": 0.0,
            "avg_loss_$": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_$": 0.0,
        }
    pnls = [t["pnl_$"] for t in completed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total = sum(pnls)
    n = len(pnls)
    win_rate = (len(wins) / n * 100) if n else 0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    sum_losses = abs(sum(losses))
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0
    profit_factor = (sum(wins) / sum_losses) if sum_losses else (sum(wins) if wins else 0.0)
    # equity curve and max drawdown
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
    return {
        "total_pnl_$": total,
        "win_rate_pct": win_rate,
        "n_completed": n,
        "avg_win_$": avg_win,
        "avg_loss_$": avg_loss,
        "profit_factor": profit_factor,
        "max_drawdown_$": abs(max_dd),
    }


def exit_reason_counts(completed: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count exit_reason from completed (SELL) side."""
    counts: Dict[str, int] = {}
    for t in completed:
        r = (t.get("exit_reason") or "").strip() or "—"
        counts[r] = counts.get(r, 0) + 1
    return counts


def equity_curve(completed: List[Dict[str, Any]]) -> Tuple[List[str], List[float]]:
    """Cumulative PnL by time (sell_time). Returns (times, cumulative_pnl_$)."""
    if not completed:
        return [], []
    sorted_t = sorted(completed, key=lambda x: (x.get("sell_time") or ""))
    times = []
    cum = []
    c = 0.0
    for t in sorted_t:
        c += t["pnl_$"]
        times.append(str(t.get("sell_time") or ""))
        cum.append(c)
    return times, cum


def mfe_mae_scatter_data(completed: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float], List[float]]:
    """(mfe_pct list, pnl_% list for MFE scatter), (mae_pct list, pnl_% list for MAE scatter)."""
    mfe_x, mfe_y = [], []
    mae_x, mae_y = [], []
    for t in completed:
        pnl_pct = t.get("pnl_%")
        if pnl_pct is None:
            continue
        try:
            p = float(pnl_pct)
        except (TypeError, ValueError):
            continue
        mfe = t.get("mfe_pct")
        if mfe is not None and str(mfe).strip() != "":
            try:
                mfe_x.append(float(mfe))
                mfe_y.append(p * 100)
            except (TypeError, ValueError):
                pass
        mae = t.get("mae_pct")
        if mae is not None and str(mae).strip() != "":
            try:
                mae_x.append(float(mae))
                mae_y.append(p * 100)
            except (TypeError, ValueError):
                pass
    return mfe_x, mfe_y, mae_x, mae_y


def sanity_buy_counts(
    trades_df: pd.DataFrame,
    signals_df: pd.DataFrame,
    date_str: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """Today's BUY signal count vs filled BUY count; warnings for state vs trades."""
    trades_today = trades_for_date(trades_df, date_str)
    buy_fills = 0
    if not trades_today.empty and "side" in trades_today.columns:
        buy_fills = (trades_today["side"].astype(str).str.strip().str.upper() == "BUY").sum()

    signals_today = pd.DataFrame()
    if not signals_df.empty and "time" in signals_df.columns:
        signals_df = signals_df.copy()
        signals_df["_date"] = signals_df["time"].apply(lambda x: (parse_ts(x) or datetime.min).strftime("%Y-%m-%d"))
        signals_today = signals_df[signals_df["_date"] == date_str]

    buy_signals = 0
    if not signals_today.empty and "signal" in signals_today.columns:
        buy_signals = (signals_today["signal"].astype(str).str.strip().str.upper() == "BUY").sum()

    state_positions = (state.get("positions") or {})
    n_state_positions = len(state_positions)
    warnings = []
    if buy_fills > 0 and n_state_positions == 0:
        warnings.append("Trades show BUY fills but state has no positions.")
    if n_state_positions > 0 and buy_fills == 0:
        warnings.append("State has positions but no BUY fills in trades for this date.")

    return {
        "buy_signals_today": int(buy_signals),
        "buy_fills_today": int(buy_fills),
        "state_position_count": n_state_positions,
        "warnings": warnings,
    }
