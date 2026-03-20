import csv
import os
import re
import yaml
from datetime import datetime, timedelta
from typing import List

from alpaca.trading.enums import TimeInForce
from alpaca.data.requests import OptionLatestQuoteRequest

from config.settings import (
    SIGNALS_CSV,
    LOG_DIR,
    EXECUTE_TRADES,
    ALLOW_EXECUTION_WHEN_CLOSED,
    EXECUTE_OPTION_TRADES,
    ALLOW_OPTION_EXECUTION_WHEN_CLOSED,
    OPTION_TRADE_NOTIONAL_USD,
    OPTION_STOP_LOSS_PCT,
    OPTION_TAKE_PROFIT_PCT,
    OPTION_MAX_HOLD_MINUTES,
    OPTION_COOLDOWN_MINUTES,
    KILL_SWITCH,
    MAX_POSITION_PER_SYMBOL,
    MAX_BUYS_PER_CYCLE,
    MAX_BUYS_PER_HOUR,
    PORTFOLIO_PROFIT_LOCK_PCT,
    PORTFOLIO_LOSS_LIMIT_PCT,
    PORTFOLIO_COOLDOWN_MINUTES,
    STRATEGY_VERSION,
)

from core.logger import log
from core.models import Position
from core.state_store import (
    load_state,
    save_state,
    now_iso,
    get_position,
    set_position,
    get_pending_order,
    set_pending_order,
    set_cooldown,
    get_cooldown,
    get_option_reco,
    set_option_reco,
    get_option_position,
    set_option_position,
    get_option_pending_order,
    set_option_pending_order,
    is_option_in_cooldown,
    set_option_cooldown,
    is_portfolio_in_cooldown,
    set_portfolio_cooldown,
    init_mfe_mae_tracker,
    update_mfe_mae_tracker,
    get_and_clear_mfe_mae,
    count_buys_in_last_hour,
    append_buy_time,
)
from core.ledger import append_trade, SKIP_COOLDOWN, SKIP_PENDING_ORDER, SKIP_ALREADY_HOLDING, SKIP_NEAR_OPEN, SKIP_KILL_SWITCH, SKIP_PORTFOLIO_LOCK, SKIP_MAX_POSITION, SKIP_MAX_BUYS_CYCLE, SKIP_MAX_BUYS_HOUR

from core.risk import compute_qty_from_notional, should_exit_position, is_in_cooldown, skip_near_open
from broker.alpaca.market_data import get_bars_df, get_latest_mid
from broker.alpaca.positions import get_open_position_qty
from broker.alpaca.orders import submit_market_order, wait_for_order

from strategies.lev_etf_trend import generate_signal as lev_etf_signal
from strategies.options_overlay import generate_options_signal
from options.selector import pick_best_call, pick_best_put
from indicators.ta import ema, rsi, atr

os.makedirs(LOG_DIR, exist_ok=True)


# ----------------------------
# Utilities
# ----------------------------
def _append_signal_row(row: dict):
    file_exists = os.path.exists(SIGNALS_CSV)
    with open(SIGNALS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _cooldown_until(minutes: int) -> str:
    return (datetime.now() + timedelta(minutes=minutes)).isoformat(timespec="seconds")


def _held_minutes(entry_time_iso: str) -> float:
    try:
        entry_dt = datetime.fromisoformat(entry_time_iso)
        return (datetime.now() - entry_dt).total_seconds() / 60.0
    except Exception:
        return 0.0


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _looks_like_option_contract(symbol: str) -> bool:
    # OCC-like: AMZN260417C00215000
    if not symbol:
        return False
    return bool(re.match(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$", symbol))


def _underlying_from_option_contract(contract: str) -> str:
    m = re.match(r"^([A-Z]{1,6})\d{6}[CP]\d{8}$", contract or "")
    return m.group(1) if m else ""


def _opt_key(underlying: str) -> str:
    return f"OPT:{underlying}"


def _equity_entry_snapshot(df, profile: dict, profile_name: str, price: float) -> str:
    """Build JSON snapshot for ledger at equity BUY (EMA/RSI/ATR/volume, timeframe, lookback) for tuning."""
    try:
        if df is None or df.empty or "close" not in df:
            return ""
        close = df["close"]
        ema_fast_len = int(profile.get("ema_fast", 20))
        ema_slow_len = int(profile.get("ema_slow", 50))
        rsi_len = int(profile.get("rsi_len", 14))
        atr_len = 14
        efast = ema(close, ema_fast_len)
        eslow = ema(close, ema_slow_len)
        r = rsi(close, rsi_len)
        snap = {
            "asset_type": "EQUITY",
            "profile": profile_name,
            "timeframe": profile.get("timeframe", ""),
            "lookback_bars": profile.get("lookback_bars", 0),
            "close": float(close.iloc[-1]) if len(close) else price,
            "ema_fast": float(efast.iloc[-1]) if len(efast) else None,
            "ema_slow": float(eslow.iloc[-1]) if len(eslow) else None,
            "rsi": float(r.iloc[-1]) if len(r) else None,
            "trend_gap_pct": None,
            "atr": None,
            "atr_pct": None,
            "volume": None,
        }
        if snap["ema_fast"] and snap["ema_slow"] and snap["ema_slow"] > 0:
            snap["trend_gap_pct"] = (snap["ema_fast"] - snap["ema_slow"]) / snap["ema_slow"]
        if "high" in df.columns and "low" in df.columns:
            atr_ser = atr(df["high"], df["low"], close, atr_len)
            if len(atr_ser) > 0:
                atr_val = float(atr_ser.iloc[-1])
                snap["atr"] = atr_val
                if snap["close"] and snap["close"] > 0:
                    snap["atr_pct"] = round(atr_val / snap["close"], 6)
        if "volume" in df.columns and len(df["volume"]) > 0:
            try:
                snap["volume"] = float(df["volume"].iloc[-1])
            except (TypeError, ValueError):
                pass
        import json
        return json.dumps(snap)
    except Exception:
        return ""


def _option_entry_snapshot(best, profile_name: str, underlying: str, underlying_price: float, contract_type: str = "CALL") -> str:
    """Build JSON snapshot for ledger at option BUY (delta, iv, spread_pct, dte, underlying price, contract_type)."""
    try:
        if best is None:
            return ""
        import json
        snap = {
            "asset_type": "OPTION",
            "contract_type": contract_type,
            "profile": profile_name,
            "underlying": underlying,
            "underlying_price": underlying_price,
            "delta": getattr(best, "delta", None),
            "iv": getattr(best, "iv", None),
            "spread_pct": getattr(best, "spread_pct", None),
            "dte": getattr(best, "dte", None),
        }
        return json.dumps(snap)
    except Exception:
        return ""


def _compute_portfolio_invested_and_pnl(
    state,
    trading_client,
    stock_data_client,
    option_data_client,
    equity_symbols: List[str],
    option_underlyings: List[str],
) -> tuple:
    """Returns (invested_notional, unrealized_pnl). Broker is source of truth for qty."""
    invested = 0.0
    unrealized = 0.0
    positions = state.get("positions", {}) or {}
    for key, p in list(positions.items()):
        if not p:
            continue
        try:
            pos = Position(**p) if isinstance(p, dict) else p
            qty = _safe_float(pos.qty, 0.0)
            entry = _safe_float(pos.entry_price, 0.0)
            if qty <= 0 or entry <= 0:
                continue
            if pos.asset_type == "EQUITY" or (key in equity_symbols):
                sym = pos.symbol
                broker_qty = _safe_float(get_open_position_qty(trading_client, sym), 0.0)
                if broker_qty <= 0:
                    continue
                invested += entry * broker_qty
                mid = _safe_float(get_latest_mid(stock_data_client, sym), 0.0)
                if mid > 0:
                    unrealized += (mid - entry) * broker_qty
            elif pos.asset_type == "OPTION" or key.startswith("OPT:"):
                contract = pos.symbol
                broker_qty = _safe_float(get_open_position_qty(trading_client, contract), 0.0)
                if broker_qty <= 0:
                    continue
                invested += entry * broker_qty * 100.0
                mid = None
                try:
                    qreq = OptionLatestQuoteRequest(symbol_or_symbols=[contract])
                    quotes = option_data_client.get_option_latest_quote(qreq)
                    q = quotes.get(contract) if isinstance(quotes, dict) else None
                    if q:
                        bid = _safe_float(getattr(q, "bid_price", None), 0.0)
                        ask = _safe_float(getattr(q, "ask_price", None), 0.0)
                        if bid > 0 and ask > 0:
                            mid = (bid + ask) / 2.0
                        else:
                            mid = ask or bid
                except Exception:
                    pass
                if mid is not None and mid > 0:
                    unrealized += (mid - entry) * broker_qty * 100.0
        except Exception:
            continue
    return (invested, unrealized)


def _exit_all_positions(
    state,
    trading_client,
    watchlist_cfg,
    profiles_cfg,
    stock_data_client,
    option_data_client,
    now: str,
    exit_reason_str: str = "portfolio_exit_all",
    market_open: bool = True,
) -> None:
    """Close every position in state; set per-symbol cooldowns and portfolio cooldown.
    When market is closed, uses a short wait timeout so we don't block; orders still
    pending are left in state and may fill at next open."""
    wait_timeout_s = 15 if not market_open else 60
    profiles = profiles_cfg.get("profiles", {})
    positions = state.get("positions", {}) or {}
    for key in list(positions.keys()):
        p = positions.get(key)
        if not p:
            continue
        try:
            pos = Position(**p) if isinstance(p, dict) else p
            sym = pos.symbol
            pending = get_pending_order(state, key)
            if pending and (pending.get("side") or "").upper() == "SELL":
                order_id = pending.get("order_id")
                if order_id:
                    final = wait_for_order(trading_client, str(order_id), timeout_s=wait_timeout_s)
                    if final.status not in ("filled", "canceled", "rejected"):
                        log(f"portfolio_exit_all: {sym} SELL still {final.status} (market closed?); leaving pending")
                        continue
                    log(f"portfolio_exit_all: {sym} SELL {order_id} status={final.status}")
                    set_pending_order(state, key, None)
                    if final.status != "filled":
                        continue
                    qty = _safe_float(get_open_position_qty(trading_client, sym), 0.0)
                    entry = _safe_float(pos.entry_price, 0.0)
                    mfe_pct, mae_pct = get_and_clear_mfe_mae(state, key, entry)
                    fqty = _safe_float(getattr(final, "filled_qty", None) or qty, qty)
                    fpx = _safe_float(getattr(final, "filled_avg_price", None) or 0, 0)
                    profile_name = None
                    for item in watchlist_cfg.get("equities", []) + watchlist_cfg.get("options", []):
                        if item.get("symbol") == key or item.get("underlying") == key.replace("OPT:", ""):
                            profile_name = item.get("profile", "")
                            break
                    set_position(state, key, None)
                    if key.startswith("OPT:"):
                        set_option_cooldown(state, key.replace("OPT:", ""), _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES), "portfolio_exit_all")
                    else:
                        set_cooldown(state, key, _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES), "portfolio_exit_all")
                    atype = "OPTION" if key.startswith("OPT:") else "EQUITY"
                    octype = (getattr(pos, "contract_type", None) or ("PUT" if len(sym) >= 9 and sym[-9] == "P" else "CALL")) if atype == "OPTION" else ""
                    append_trade(now, sym, "SELL", fqty, fpx, STRATEGY_VERSION, profile_name or "", "", exit_reason_str, mfe_pct=mfe_pct, mae_pct=mae_pct, asset_type=atype, contract_type=octype)
                    save_state(state)
                continue
            qty = _safe_float(get_open_position_qty(trading_client, sym), 0.0)
            if qty <= 0:
                set_position(state, key, None)
                continue
            entry = _safe_float(pos.entry_price, 0.0)
            mfe_pct, mae_pct = get_and_clear_mfe_mae(state, key, entry)
            order_id = submit_market_order(trading_client, sym, "SELL", qty)
            set_pending_order(state, key, {"order_id": str(order_id), "side": "SELL", "time": now})
            save_state(state)
            final = wait_for_order(trading_client, str(order_id), timeout_s=wait_timeout_s)
            log(f"portfolio_exit_all: {sym} SELL {order_id} status={final.status}")
            if final.status not in ("filled", "canceled", "rejected"):
                log(f"portfolio_exit_all: {sym} order still {final.status} (market closed?); will retry next cycle")
                continue
            set_pending_order(state, key, None)
            if final.status != "filled":
                continue
            set_position(state, key, None)
            profile_name = None
            for item in watchlist_cfg.get("equities", []) + watchlist_cfg.get("options", []):
                if item.get("symbol") == key or item.get("underlying") == key.replace("OPT:", ""):
                    profile_name = item.get("profile", "")
                    break
            if key.startswith("OPT:"):
                set_option_cooldown(state, key.replace("OPT:", ""), _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES), "portfolio_exit_all")
            else:
                set_cooldown(state, key, _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES), "portfolio_exit_all")
            fqty = _safe_float(getattr(final, "filled_qty", None) or qty, qty)
            fpx = _safe_float(getattr(final, "filled_avg_price", None) or 0, 0)
            atype = "OPTION" if key.startswith("OPT:") else "EQUITY"
            octype = (getattr(pos, "contract_type", None) or ("PUT" if len(sym) >= 9 and sym[-9] == "P" else "CALL")) if atype == "OPTION" else ""
            append_trade(now, sym, "SELL", fqty, fpx, STRATEGY_VERSION, profile_name or "", "", exit_reason_str, mfe_pct=mfe_pct, mae_pct=mae_pct, asset_type=atype, contract_type=octype)
        except Exception as e:
            log(f"portfolio_exit_all error {key}: {e}")
    set_portfolio_cooldown(state, _cooldown_until(PORTFOLIO_COOLDOWN_MINUTES))
    save_state(state)
    log(f"portfolio_exit_all done; cooldown {PORTFOLIO_COOLDOWN_MINUTES}min")


# ----------------------------
# Reconcile state with broker
# ----------------------------
def reconcile_equity_positions_from_broker(state, trading_client, stock_data_client, symbols: List[str]):
    for sym in symbols:
        try:
            broker_qty = _safe_float(get_open_position_qty(trading_client, sym) or 0.0, 0.0)
            st_pos = get_position(state, sym)

            if broker_qty <= 0:
                if st_pos is not None:
                    set_position(state, sym, None)
                continue

            if st_pos is None or _safe_float(st_pos.qty) != broker_qty:
                px = _safe_float(get_latest_mid(stock_data_client, sym) or 0.0, 0.0)
                set_position(state, sym, Position(
                    asset_type="EQUITY",
                    key=sym,
                    symbol=sym,
                    qty=broker_qty,
                    entry_price=px if px > 0 else 0.0,
                    entry_time=now_iso(),
                ))
        except Exception as e:
            log(f"{sym}: reconcile error: {e}")


def reconcile_option_positions_from_broker(state, trading_client, option_underlyings: List[str]):
    """
    Reconcile option state with broker (broker is source of truth).
    - If broker has no position for an underlying we track → clear state.
    - If broker has position → create/update state (including qty).
    """
    try:
        broker_positions = trading_client.get_all_positions()
    except Exception as e:
        log(f"options reconcile: could not fetch broker positions: {e}")
        return

    # Collect all option positions per underlying (broker may have multiple contracts per und)
    broker_option_by_und = {}  # und -> list of {symbol, qty, avg_entry_price}
    for p in broker_positions:
        sym = getattr(p, "symbol", None)
        if not sym:
            continue
        qty = _safe_float(getattr(p, "qty", 0) or 0.0, 0.0)
        if qty <= 0:
            continue
        asset_class = str(getattr(p, "asset_class", "") or "").lower()
        is_opt = ("option" in asset_class) or _looks_like_option_contract(sym)
        if not is_opt:
            continue
        und = _underlying_from_option_contract(sym)
        if not und or und not in option_underlyings:
            continue
        entry = _safe_float(getattr(p, "avg_entry_price", 0) or 0.0, 0.0)
        # Detect CALL vs PUT from OCC symbol (right at -9)
        ctype = "PUT" if len(sym) >= 9 and sym[-9] == "P" else "CALL"
        broker_option_by_und.setdefault(und, []).append({"symbol": sym, "qty": qty, "avg_entry_price": entry, "contract_type": ctype})

    for und in option_underlyings:
        candidates = broker_option_by_und.get(und, [])
        if len(candidates) > 1:
            log(f"reconcile: OPT:{und} multiple broker positions ({len(candidates)}), picking largest qty")
        broker_info = max(candidates, key=lambda x: x["qty"]) if candidates else None
        key = _opt_key(und)
        st_pos = get_position(state, key)
        if broker_info is None:
            if st_pos is not None:
                set_position(state, key, None)
                log(f"reconcile: cleared OPT:{und} (broker qty=0)")
            continue
        if st_pos is None or _safe_float(st_pos.qty) != broker_info["qty"]:
            set_position(state, key, Position(
                asset_type="OPTION",
                key=key,
                symbol=broker_info["symbol"],
                qty=broker_info["qty"],
                entry_price=broker_info["avg_entry_price"],
                entry_time=(st_pos.entry_time if st_pos else None) or now_iso(),
                underlying=und,
                contract=broker_info["symbol"],
                contract_type=broker_info.get("contract_type", "CALL"),
            ))


# ----------------------------
# Main cycle
# ----------------------------
def run_cycle(stock_data_client, trading_client, option_data_client, watchlist_cfg, profiles_cfg, market_open: bool):
    profiles = profiles_cfg["profiles"]
    now = datetime.now().isoformat(timespec="seconds")

    state = load_state()

    equity_symbols = [i["symbol"] for i in watchlist_cfg.get("equities", [])]
    option_underlyings = [i["underlying"] for i in watchlist_cfg.get("options", [])]

    # reconcile every cycle (safe); prevents duplicate buys after crash/restart
    reconcile_equity_positions_from_broker(state, trading_client, stock_data_client, equity_symbols)
    reconcile_option_positions_from_broker(state, trading_client, option_underlyings)
    save_state(state)

    # Portfolio-level risk: exit all if profit/loss limit hit
    try:
        invested, unrealized = _compute_portfolio_invested_and_pnl(
            state, trading_client, stock_data_client, option_data_client, equity_symbols, option_underlyings
        )
        if invested > 0:
            pnl_pct = unrealized / invested
            if pnl_pct >= float(PORTFOLIO_PROFIT_LOCK_PCT) or pnl_pct <= -float(PORTFOLIO_LOSS_LIMIT_PCT):
                reason = "portfolio_profit_lock" if pnl_pct >= float(PORTFOLIO_PROFIT_LOCK_PCT) else "portfolio_loss_limit"
                log(f"PORTFOLIO_LOCK_TRIGGERED: {reason} pnl_pct={pnl_pct:.2%} invested={invested:.0f}")
                _exit_all_positions(
                    state, trading_client, watchlist_cfg, profiles_cfg,
                    stock_data_client, option_data_client, now,
                    exit_reason_str=reason,
                    market_open=market_open,
                )
    except Exception as e:
        log(f"portfolio risk check error: {e}")

    # Global: no new orders when kill switch is OFF (False) or portfolio cooldown
    can_place_orders = (KILL_SWITCH is True) and (not is_portfolio_in_cooldown(state))
    if not can_place_orders:
        if not KILL_SWITCH:
            log("SKIP: kill_switch_off (no orders placed)")
        elif is_portfolio_in_cooldown(state):
            log("SKIP: portfolio_lock (cooldown active)")

    # -------------------------
    # EQUITIES
    # -------------------------
    buys_this_cycle = 0
    buys_in_last_hour = count_buys_in_last_hour(state, 60)
    for item in watchlist_cfg.get("equities", []):
        sym = item["symbol"]
        profile_name = item["profile"]
        profile = profiles[profile_name]

        try:
            df = get_bars_df(stock_data_client, sym, lookback=profile["lookback_bars"], timeframe=profile["timeframe"])
            sig = lev_etf_signal(df, profile)

            _append_signal_row({
                "time": now,
                "asset": "EQUITY",
                "symbol": sym,
                "profile": profile_name,
                "signal": sig.action,
                "reason": sig.reason,
            })

            log(f"{sym} [{profile_name}] -> {sig.action} | {sig.reason}")

            can_execute = EXECUTE_TRADES and (market_open or ALLOW_EXECUTION_WHEN_CLOSED) and can_place_orders
            if not can_execute:
                continue

            if get_pending_order(state, sym):
                log(f"{sym}: SKIP {SKIP_PENDING_ORDER}")
                continue

            if is_in_cooldown(state, sym):
                cd = get_cooldown(state, sym)
                log(f"{sym}: SKIP {SKIP_COOLDOWN} until {cd.get('until')}")
                continue

            price = _safe_float(get_latest_mid(stock_data_client, sym), 0.0)
            pos = get_position(state, sym)
            broker_qty = _safe_float(get_open_position_qty(trading_client, sym), 0.0)

            # exit if holding (state or broker has position)
            if pos or broker_qty > 0:
                if pos:
                    update_mfe_mae_tracker(state, sym, price)
                exit_now, exit_reason = (should_exit_position(pos, price, profile) if pos else (True, "reconcile_orphan"))
                # Strategy SELL only after min_hold_minutes (stops/trail/time still immediate)
                min_hold = int(profile.get("min_hold_minutes", 0))
                if sig.action == "SELL" and not exit_now and min_hold > 0 and pos:
                    if _held_minutes(pos.entry_time) < min_hold:
                        exit_now, exit_reason = False, ""
                if sig.action == "SELL" or exit_now:
                    qty = _safe_float(get_open_position_qty(trading_client, sym), 0.0)
                    if qty <= 0:
                        log(f"{sym}: broker qty=0; clearing state.")
                        set_position(state, sym, None)
                        save_state(state)
                        continue

                    if not can_place_orders:
                        continue  # will have logged SKIP above
                    entry_for_mfe = _safe_float(pos.entry_price, 0.0) if pos else 0.0
                    mfe_pct, mae_pct = get_and_clear_mfe_mae(state, sym, entry_for_mfe) if entry_for_mfe and pos else (None, None)
                    order_id = submit_market_order(trading_client, sym, "SELL", qty)
                    set_pending_order(state, sym, {"order_id": str(order_id), "side": "SELL", "time": now_iso()})
                    save_state(state)

                    final = wait_for_order(trading_client, str(order_id))
                    log(f"{sym}: SELL order {order_id} status={final.status}")
                    fqty = _safe_float(getattr(final, "filled_qty", None) or qty, qty)
                    fpx = _safe_float(getattr(final, "filled_avg_price", None) or price, price)
                    append_trade(now_iso(), sym, "SELL", fqty, fpx, STRATEGY_VERSION, profile_name, "", exit_reason or sig.reason, mfe_pct=mfe_pct, mae_pct=mae_pct, asset_type="EQUITY")

                    set_pending_order(state, sym, None)
                    set_position(state, sym, None)
                    cd_min = int(profile.get("cooldown_minutes", 60))
                    set_cooldown(state, sym, _cooldown_until(cd_min), f"exit: {exit_reason or sig.reason}")
                    save_state(state)
                continue

            # entry if not holding
            if sig.action == "BUY":
                if broker_qty > 0:
                    log(f"{sym}: SKIP {SKIP_ALREADY_HOLDING} (broker qty={broker_qty})")
                    continue
                if buys_this_cycle >= MAX_BUYS_PER_CYCLE:
                    log(f"{sym}: SKIP {SKIP_MAX_BUYS_CYCLE} ({buys_this_cycle})")
                    continue
                if buys_in_last_hour >= MAX_BUYS_PER_HOUR:
                    log(f"{sym}: SKIP {SKIP_MAX_BUYS_HOUR} ({buys_in_last_hour})")
                    continue
                if skip_near_open(profile):
                    log(f"{sym}: SKIP {SKIP_NEAR_OPEN}")
                    continue

                notional = float(profile.get("trade_notional_usd", 500))
                qty = compute_qty_from_notional(price, notional)
                if qty <= 0:
                    log(f"{sym}: qty=0 at price={price:.2f}; skipping.")
                    continue

                order_id = submit_market_order(trading_client, sym, "BUY", qty)
                set_pending_order(state, sym, {"order_id": str(order_id), "side": "BUY", "time": now_iso()})
                save_state(state)

                final = wait_for_order(trading_client, str(order_id))
                log(f"{sym}: BUY order {order_id} status={final.status}")

                if str(final.status).lower() == "filled":
                    filled_avg = _safe_float(getattr(final, "filled_avg_price", None) or price, price)
                    filled_qty = _safe_float(getattr(final, "filled_qty", None) or qty, qty)
                    set_position(state, sym, Position(
                        asset_type="EQUITY",
                        key=sym,
                        symbol=sym,
                        qty=filled_qty,
                        entry_price=filled_avg,
                        entry_time=now_iso(),
                    ))
                    init_mfe_mae_tracker(state, sym, filled_avg)
                    snapshot_json = _equity_entry_snapshot(df, profile, profile_name, price) if df is not None and not df.empty else ""
                    append_trade(now_iso(), sym, "BUY", filled_qty, filled_avg, STRATEGY_VERSION, profile_name, sig.reason, "", entry_snapshot_json=snapshot_json, asset_type="EQUITY")
                    buys_this_cycle += 1
                    append_buy_time(state)
                    buys_in_last_hour = count_buys_in_last_hour(state, 60)

                set_pending_order(state, sym, None)
                save_state(state)

        except Exception as e:
            log(f"{sym} data/signal/exec error: {e}")

    # -------------------------
    # OPTIONS
    # -------------------------
    buys_in_last_hour = count_buys_in_last_hour(state, 60)
    for item in watchlist_cfg.get("options", []):
        und = item["underlying"]
        profile_name = item["profile"]
        profile = profiles[profile_name]

        try:
            df = get_bars_df(stock_data_client, und, lookback=profile["lookback_bars"], timeframe=profile["timeframe"])
            sig = generate_options_signal(df, profile)

            can_execute_opt = EXECUTE_OPTION_TRADES and (market_open or ALLOW_OPTION_EXECUTION_WHEN_CLOSED) and can_place_orders

            if get_option_pending_order(state, und):
                log(f"{und}: SKIP {SKIP_PENDING_ORDER} (option)")
                continue

            if is_option_in_cooldown(state, und):
                cd = get_cooldown(state, _opt_key(und))
                log(f"{und}: SKIP {SKIP_COOLDOWN} until {cd.get('until')}")
                continue

            opt_pos = get_option_position(state, und)

            # exit if holding (one option position per underlying: CALL or PUT)
            if opt_pos:
                contract = opt_pos.symbol
                entry = _safe_float(opt_pos.entry_price, 0.0)
                held_m = _held_minutes(opt_pos.entry_time)
                # contract_type: from state or infer from OCC symbol (right at -9)
                ctype = getattr(opt_pos, "contract_type", None) or ("PUT" if len(contract) >= 9 and contract[-9] == "P" else "CALL")

                mid = None
                try:
                    qreq = OptionLatestQuoteRequest(symbol_or_symbols=[contract])
                    quotes = option_data_client.get_option_latest_quote(qreq)
                    q = quotes.get(contract) if isinstance(quotes, dict) else None
                    if q:
                        bid = _safe_float(getattr(q, "bid_price", None), 0.0)
                        ask = _safe_float(getattr(q, "ask_price", None), 0.0)
                        if bid > 0 and ask > 0:
                            mid = (bid + ask) / 2.0
                        else:
                            mid = ask or bid or None
                except Exception:
                    mid = None

                if mid is not None:
                    update_mfe_mae_tracker(state, _opt_key(und), mid)
                exit_reason = None
                if mid is not None and entry > 0:
                    pnl_pct = (mid - entry) / entry
                    if pnl_pct <= -float(OPTION_STOP_LOSS_PCT):
                        exit_reason = f"stop_loss pnl={pnl_pct:.2%}"
                    elif pnl_pct >= float(OPTION_TAKE_PROFIT_PCT):
                        exit_reason = f"take_profit pnl={pnl_pct:.2%}"

                if held_m >= float(OPTION_MAX_HOLD_MINUTES):
                    exit_reason = f"max_hold {held_m:.0f}m"

                if sig.action == "SELL":
                    exit_reason = "underlying_sell_signal"
                # Direction flip: exit CALL when signal says BUY PUT; exit PUT when signal says BUY CALL (re-enter next cycle)
                if sig.action == "BUY" and getattr(sig, "direction", None):
                    if sig.direction == "PUT" and ctype == "CALL":
                        exit_reason = "underlying_bearish_exit_call"
                    elif sig.direction == "CALL" and ctype == "PUT":
                        exit_reason = "underlying_bullish_exit_put"

                if exit_reason and can_execute_opt:
                    qty = _safe_float(get_open_position_qty(trading_client, contract), 0.0)
                    if qty <= 0:
                        log(f"{und}: broker qty=0 for {contract}; clearing state.")
                        set_option_position(state, und, None)
                        save_state(state)
                    else:
                        mfe_pct, mae_pct = get_and_clear_mfe_mae(state, _opt_key(und), entry)
                        log(f"{und}: EXIT {ctype} {contract} qty={qty} reason={exit_reason}")
                        order_id = submit_market_order(trading_client, contract, "SELL", qty, time_in_force=TimeInForce.DAY)
                        set_option_pending_order(state, und, {"order_id": str(order_id), "side": "SELL", "time": now_iso()})
                        save_state(state)

                        final = wait_for_order(trading_client, str(order_id))
                        log(f"{und}: OPTION SELL order {order_id} status={final.status}")
                        fqty = _safe_float(getattr(final, "filled_qty", None) or qty, qty)
                        fpx = _safe_float(getattr(final, "filled_avg_price", None) or entry, entry)
                        append_trade(now_iso(), contract, "SELL", fqty, fpx, STRATEGY_VERSION, profile_name, "", exit_reason, mfe_pct=mfe_pct, mae_pct=mae_pct, asset_type="OPTION", contract_type=ctype)

                        set_option_pending_order(state, und, None)
                        set_option_position(state, und, None)
                        set_option_cooldown(state, und, _cooldown_until(int(OPTION_COOLDOWN_MINUTES)), f"exit: {exit_reason}")
                        save_state(state)

                _append_signal_row({
                    "time": now,
                    "asset": "OPTION_UNDERLYING",
                    "symbol": und,
                    "profile": profile_name,
                    "signal": sig.action,
                    "reason": f"{sig.reason} | holding={contract}",
                })
                log(f"{und} [options] -> {sig.action} | {sig.reason} | holding {contract}")
                continue

            # not holding: pick+buy on BUY (CALL for bullish, PUT for bearish)
            reco_str = ""
            reco_obj = None
            want_put = getattr(sig, "direction", None) == "PUT"

            if sig.action == "BUY":
                if want_put:
                    best = pick_best_put(
                        option_client=option_data_client,
                        underlying=und,
                        dte_min=int(profile.get("dte_min", 60)),
                        dte_max=int(profile.get("dte_max", 120)),
                        delta_min=float(profile.get("put_delta_min", profile.get("delta_min", 0.40))),
                        delta_max=float(profile.get("put_delta_max", profile.get("delta_max", 0.55))),
                        iv_max=float(profile.get("iv_max", 0.50)),
                        spread_pct_max=float(profile.get("spread_pct_max", 0.05)),
                    )
                    ctype = "PUT"
                else:
                    best = pick_best_call(
                        option_client=option_data_client,
                        underlying=und,
                        dte_min=int(profile.get("dte_min", 60)),
                        dte_max=int(profile.get("dte_max", 120)),
                        delta_min=float(profile.get("delta_min", 0.40)),
                        delta_max=float(profile.get("delta_max", 0.55)),
                        iv_max=float(profile.get("iv_max", 0.50)),
                        spread_pct_max=float(profile.get("spread_pct_max", 0.05)),
                    )
                    ctype = "CALL"

                if best:
                    reco_str = (
                        f" | RECO {ctype} {best.symbol} DTE={best.dte} "
                        f"delta={best.delta:.3f} IV={best.iv if best.iv is not None else 'NA'} "
                        f"spread%={best.spread_pct * 100:.2f}%"
                    )

                    reco_obj = {
                        "contract": best.symbol,
                        "contract_type": ctype,
                        "dte": best.dte,
                        "delta": best.delta,
                        "iv": best.iv,
                        "bid": best.bid,
                        "ask": best.ask,
                        "spread_pct": best.spread_pct,
                        "time": now_iso(),
                        "profile": profile_name,
                        "signal": sig.action,
                    }
                    set_option_reco(state, und, reco_obj)
                    save_state(state)

                    broker_contract_qty = _safe_float(get_open_position_qty(trading_client, best.symbol), 0.0)
                    if broker_contract_qty > 0:
                        log(f"{und}: broker already holds {best.symbol} qty={broker_contract_qty}; skipping buy.")
                    elif buys_in_last_hour >= MAX_BUYS_PER_HOUR:
                        log(f"{und}: SKIP {SKIP_MAX_BUYS_HOUR} ({buys_in_last_hour})")
                    elif can_execute_opt:
                        ask = _safe_float(best.ask, 0.0)
                        if ask <= 0:
                            log(f"{und}: reco {best.symbol} has no ask; skipping execution.")
                        else:
                            notional = float(profile.get("option_trade_notional_usd", OPTION_TRADE_NOTIONAL_USD))
                            contracts = max(1, int(notional / (ask * 100.0)))

                            log(f"{und}: OPTION BUY {ctype} {best.symbol} contracts={contracts} est_ask={ask:.2f}")
                            order_id = submit_market_order(trading_client, best.symbol, "BUY", contracts, time_in_force=TimeInForce.DAY)
                            set_option_pending_order(state, und, {"order_id": str(order_id), "side": "BUY", "time": now_iso()})
                            save_state(state)

                            final = wait_for_order(trading_client, str(order_id))
                            log(f"{und}: OPTION BUY order {order_id} status={final.status}")

                            if str(final.status).lower() == "filled":
                                filled_avg = _safe_float(getattr(final, "filled_avg_price", None) or ask, ask)
                                filled_qty = _safe_float(getattr(final, "filled_qty", None) or contracts, contracts)
                                set_option_position(state, und, Position(
                                    asset_type="OPTION",
                                    key=_opt_key(und),
                                    symbol=best.symbol,
                                    qty=filled_qty,
                                    entry_price=filled_avg,
                                    entry_time=now_iso(),
                                    underlying=und,
                                    contract=best.symbol,
                                    contract_type=ctype,
                                ))
                                init_mfe_mae_tracker(state, _opt_key(und), filled_avg)
                                underlying_price = _safe_float(get_latest_mid(stock_data_client, und), 0.0)
                                opt_snapshot = _option_entry_snapshot(best, profile_name, und, underlying_price, contract_type=ctype)
                                append_trade(now_iso(), best.symbol, "BUY", filled_qty, filled_avg, STRATEGY_VERSION, profile_name, sig.reason, "", asset_type="OPTION", entry_snapshot_json=opt_snapshot, contract_type=ctype)
                                append_buy_time(state)
                                buys_in_last_hour = count_buys_in_last_hour(state, 60)

                            set_option_pending_order(state, und, None)
                            save_state(state)

            else:
                set_option_reco(state, und, None)
                save_state(state)

            _append_signal_row({
                "time": now,
                "asset": "OPTION_UNDERLYING",
                "symbol": und,
                "profile": profile_name,
                "signal": sig.action,
                "reason": sig.reason,
                "reco_contract": reco_obj["contract"] if reco_obj else "",
            })

            log(f"{und} [options] -> {sig.action} | {sig.reason}{reco_str}")

        except Exception as e:
            log(f"{und} options-signal/exec error: {e}")
