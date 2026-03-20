from __future__ import annotations

import logging
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import pytz
from alpaca.data.requests import OptionLatestQuoteRequest
from alpaca.trading.enums import OrderSide, PositionIntent
from alpaca.trading.client import TradingClient

from bot.alpaca_put_spread.alerts import (
    alert_api_error,
    alert_close_filled,
    alert_close_submitted,
    alert_entry_filled,
    alert_entry_submitted,
    alert_loss_cap_hit,
    alert_sl_triggered,
    alert_tp_triggered,
)
from bot.alpaca_put_spread.config import AlpacaPutSpreadConfig
from bot.alpaca_put_spread.db import (
    close_spread as db_close_spread,
    get_daily_pnl,
    get_pnl_by_underlying_today,
    init_alpaca_db,
    insert_order as db_insert_order,
    insert_spread as db_insert_spread,
    log_event,
    update_order_status as db_update_order_status,
)
from bot.alpaca_put_spread.execution import cancel_order, submit_mleg_limit_order, wait_for_order
from bot.alpaca_put_spread.option_symbol import option_expiry_utc, parse_occ_option_symbol
from bot.alpaca_put_spread.put_spread_logic import PutSpreadCandidate, tp_sl_triggered
from bot.alpaca_put_spread.put_spread_selector import select_bull_put_credit_spread
from bot.alpaca_put_spread.state import load_state, save_state

from trading_assistant.broker.alpaca.market_data import get_latest_mid
from trading_assistant.broker.alpaca.positions import list_open_positions


logger = logging.getLogger(__name__)


def _option_mid(option_data_client, symbol: str) -> Optional[float]:
    qreq = OptionLatestQuoteRequest(symbol_or_symbols=[symbol])
    quotes = option_data_client.get_option_latest_quote(qreq)
    q = quotes.get(symbol) if isinstance(quotes, dict) else None
    if not q:
        return None
    bid = getattr(q, "bid_price", None)
    ask = getattr(q, "ask_price", None)
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        if bid_f is not None and ask_f is not None and bid_f > 0 and ask_f > 0:
            return (bid_f + ask_f) / 2.0
        if ask_f is not None and ask_f > 0:
            return ask_f
        if bid_f is not None and bid_f > 0:
            return bid_f
    except Exception:
        return None
    return None


def _compute_distance_points(short_put_symbol: str, underlying_mid: float) -> Optional[float]:
    parts = parse_occ_option_symbol(short_put_symbol)
    if not parts:
        return None
    # distance = short_strike - underlying_spot
    return float(parts.strike) - float(underlying_mid)


def _compute_otm_percent(short_put_symbol: str, underlying_mid: float) -> Optional[float]:
    """
    OTM% for the short put relative to current underlying:
      OTM% = (underlying_mid - short_strike) / underlying_mid
    Positive when underlying is above the put strike (put is OTM),
    and approaches 0 as underlying moves down toward/through the strike.
    """
    if underlying_mid <= 0:
        return None
    parts = parse_occ_option_symbol(short_put_symbol)
    if not parts:
        return None
    return (float(underlying_mid) - float(parts.strike)) / float(underlying_mid)


def _entry_to_order_legs(candidate: PutSpreadCandidate) -> list[Dict[str, Any]]:
    # Entry for bull put credit spread:
    #   Leg 1 (short): sell_to_open higher strike put
    #   Leg 2 (long):  buy_to_open lower strike put
    return [
        {
            "symbol": candidate.short_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_OPEN,
        },
        {
            "symbol": candidate.long_put_symbol,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_OPEN,
        },
    ]


def _exit_to_order_legs(open_spread: Dict[str, Any]) -> list[Dict[str, Any]]:
    # Closing bull put credit spread:
    #   Leg 1 (short): buy_to_close the higher strike put
    #   Leg 2 (long):  sell_to_close the lower strike put
    short_sym = open_spread["short_put_symbol"]
    long_sym = open_spread["long_put_symbol"]
    return [
        {
            "symbol": short_sym,
            "ratio_qty": 1.0,
            "side": OrderSide.BUY,
            "position_intent": PositionIntent.BUY_TO_CLOSE,
        },
        {
            "symbol": long_sym,
            "ratio_qty": 1.0,
            "side": OrderSide.SELL,
            "position_intent": PositionIntent.SELL_TO_CLOSE,
        },
    ]


class AlpacaPutSpreadRunner:
    def __init__(
        self,
        *,
        trading_client: TradingClient,
        stock_data_client: Any,
        option_data_client: Any,
        cfg: AlpacaPutSpreadConfig,
    ) -> None:
        self.trading_client = trading_client
        self.stock_data_client = stock_data_client
        self.option_data_client = option_data_client
        self.cfg = cfg
        self.state = load_state()

        # Ensure expected shape
        self.state.setdefault("open_spread_by_underlying", {})
        self.state.setdefault("pending_entry_order_by_underlying", {})
        self.state.setdefault("pending_close_order_by_underlying", {})
        self.state.setdefault("cooldown_until_ts_by_underlying", {})
        self.state.setdefault("entry_retry_count_by_underlying", {})
        self.state.setdefault("entry_disabled_by_underlying", {})

    def _is_terminal_order_status(self, status: str) -> bool:
        s = (status or "").lower()
        s = s.split(".")[-1]
        return s in ("filled", "canceled", "rejected", "expired")

    def _get_order_status(self, order_id: str) -> str:
        try:
            o = self.trading_client.get_order_by_id(order_id)
            s = str(getattr(o, "status", "")).lower()
            return s.split(".")[-1]
        except Exception:
            # If we can't query, treat as non-terminal to avoid duplicate order submission.
            return "unknown"

    def _within_hunt_window(self) -> bool:
        """
        Gate opening/hunting new positions by local weekday + time window.
        If not configured, returns True.
        """
        if not self.cfg.trade_window_timezone or not self.cfg.trade_window_start_time_local or not self.cfg.trade_window_end_time_local:
            return True

        tz = pytz.timezone(self.cfg.trade_window_timezone)
        now_local = datetime.now(tz)

        if now_local.weekday() not in self.cfg.trade_window_weekdays:
            return False

        start_t = datetime.strptime(self.cfg.trade_window_start_time_local, "%H:%M").time()
        end_t = datetime.strptime(self.cfg.trade_window_end_time_local, "%H:%M").time()

        if start_t <= end_t:
            return start_t <= now_local.time() <= end_t

        # Window wraps midnight (e.g., 22:00 -> 02:00)
        return now_local.time() >= start_t or now_local.time() <= end_t

    def _clear_open_spread_if_not_in_positions(self, underlying: str) -> None:
        open_spread = (self.state.get("open_spread_by_underlying") or {}).get(underlying)
        if not open_spread:
            return
        # If we already have a close order pending for this underlying, don't
        # clear state yet (prevents duplicate close orders).
        if (self.state.get("pending_close_order_by_underlying") or {}).get(underlying):
            return
        try:
            open_positions = list_open_positions(self.trading_client)
        except Exception:
            # If we can't query, leave state as-is.
            return
        short_sym = open_spread.get("short_put_symbol")
        long_sym = open_spread.get("long_put_symbol")
        short_qty = float(open_positions.get(short_sym, {}).get("qty", 0.0)) if short_sym else 0.0
        long_qty = float(open_positions.get(long_sym, {}).get("qty", 0.0)) if long_sym else 0.0
        if short_qty == 0.0 and long_qty == 0.0:
            logger.info("[%s] Clearing stale open spread state (legs no longer open)", underlying)
            self.state["open_spread_by_underlying"].pop(underlying, None)
            save_state(self.state)

    def _maybe_close(self, underlying: str, open_spread: Dict[str, Any]) -> bool:
        """
        Returns True if we submitted/filled a close and cleared state, else False.
        """
        reason = None

        # Underlying distance buffer / gating.
        # Requirement: TP/SL should not close the trade unless distance-gate is met.
        underlying_mid = get_latest_mid(self.stock_data_client, underlying)
        if underlying_mid <= 0:
            return False

        distance_gate_met = False
        distance_gate_reason = None

        if self.cfg.distance_buffer_otm_fraction_of_min_short_otm is not None:
            threshold_otm_pct = float(self.cfg.min_short_otm_percent) * float(
                self.cfg.distance_buffer_otm_fraction_of_min_short_otm
            )
            otm_pct = _compute_otm_percent(open_spread["short_put_symbol"], underlying_mid)
            if otm_pct is not None:
                # "distance moved below 1%" => OTM% <= threshold => gate met
                if otm_pct <= threshold_otm_pct:
                    distance_gate_met = True
                    distance_gate_reason = (
                        f"otm_gate otm_pct={otm_pct:.4f} <= threshold={threshold_otm_pct:.4f}"
                    )
        else:
            # Backward compatible absolute-distance rule.
            distance = _compute_distance_points(open_spread["short_put_symbol"], underlying_mid)
            if distance is not None and distance >= self.cfg.distance_buffer_points:
                distance_gate_met = True
                distance_gate_reason = f"distance_buffer distance={distance:.4f}"

        if distance_gate_met:
            reason = distance_gate_reason

        # TP/SL based on net spread mid from option quotes
        # Only check TP/SL once distance gate is met.
        if distance_gate_met:
            mid_short = _option_mid(self.option_data_client, open_spread["short_put_symbol"])
            mid_long = _option_mid(self.option_data_client, open_spread["long_put_symbol"])
            if mid_short is not None and mid_long is not None:
                current_net_credit = mid_short - mid_long
                entry_credit = float(open_spread["entry_net_credit_mid"])
                triggered, sl_reason = tp_sl_triggered(
                    current_net_credit_mid=current_net_credit,
                    entry_net_credit_mid=entry_credit,
                    tp_pct=self.cfg.tp_pct,
                    sl_pct=self.cfg.sl_pct,
                )
                if triggered:
                    reason = sl_reason if not reason else (reason + f" + {sl_reason}")

        # 0DTE expiry cutoff (approx in US/Eastern 16:00 by default)
        expiry_utc = option_expiry_utc(open_spread["short_put_symbol"])
        if expiry_utc is not None:
            now_utc = datetime.now(timezone.utc)
            cutoff = expiry_utc - timedelta(minutes=self.cfg.exit_before_minutes)
            if now_utc >= cutoff:
                reason = reason or "expiry_cutoff"

        if not reason:
            return False

        logger.info("[%s] Closing bull put spread (%s)", underlying, reason)
        if "tp" in (reason or "").lower():
            alert_tp_triggered(underlying, reason or "")
        elif "sl" in (reason or "").lower():
            alert_sl_triggered(underlying, reason or "")
        log_event("close_triggered", reason or "", underlying=underlying, extra={"reason": reason})

        if not self.cfg.execute:
            # Paper config: do not modify state because no order was placed.
            logger.info("[%s] execute=false; would close spread now", underlying)
            return False

        # Close order already pending: poll its status and return.
        pending_close = (self.state.get("pending_close_order_by_underlying") or {}).get(underlying)
        if pending_close:
            order_id = pending_close.get("order_id")
            submitted_at_ts = float(pending_close.get("submitted_at_ts", 0.0) or 0.0)
            if not order_id:
                self.state["pending_close_order_by_underlying"].pop(underlying, None)
                save_state(self.state)
                return False

            status = self._get_order_status(order_id)
            if status == "filled":
                open_spread = (self.state.get("open_spread_by_underlying") or {}).get(underlying)
                spread_id = open_spread.get("spread_id") if open_spread else None
                entry_credit = float(open_spread.get("entry_net_credit_mid", 0.0)) if open_spread else 0.0
                close_debit = float(pending_close.get("close_debit_limit", 0.0) or 0.0)
                if spread_id and close_debit > 0:
                    db_close_spread(spread_id, order_id, close_debit, pending_close.get("close_reason", "close"), entry_credit, self.cfg.order_qty)
                    pnl = (entry_credit - close_debit) * 100.0 * self.cfg.order_qty
                    alert_close_filled(underlying, order_id, pending_close.get("close_reason", "close"), pnl)
                db_update_order_status(order_id, "filled")
                log_event("close_filled", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id})
                self.state["pending_close_order_by_underlying"].pop(underlying, None)
                self.state["open_spread_by_underlying"].pop(underlying, None)
                self.state["cooldown_until_ts_by_underlying"][underlying] = time.time() + (
                    int(self.cfg.exit_cooldown_minutes) * 60
                )
                save_state(self.state)
                logger.info("[%s] Close filled; pending+state cleared", underlying)
                return True

            if self._is_terminal_order_status(status) and status != "filled":
                # Terminal but not filled => clear pending; allow close retry next loop.
                self.state["pending_close_order_by_underlying"].pop(underlying, None)
                save_state(self.state)
                logger.warning("[%s] Close order terminal status=%s; state cleared for retry", underlying, status)
                return False

            # Not terminal yet: if it's been too long, cancel and retry.
            if submitted_at_ts and (time.time() - submitted_at_ts) >= self.cfg.order_fill_timeout_seconds:
                try:
                    cancel_order(self.trading_client, order_id)
                except Exception:
                    pass
                self.state["pending_close_order_by_underlying"].pop(underlying, None)
                save_state(self.state)
                logger.warning("[%s] Close order timed out; canceled pending close for retry", underlying)
                return False

            # Still working: don't submit a new close order.
            return False

        # Close by submitting an mleg debit limit.
        # debit_limit ~= current_net_credit_mid (credit spread value) * (1 + slippage)
        # If we couldn't compute current net credit, fall back to entry credit.
        current_net_credit = mid_short - mid_long if (mid_short is not None and mid_long is not None) else float(
            open_spread.get("entry_net_credit_mid", 0.0)
        )
        if current_net_credit <= 0:
            logger.warning("[%s] Cannot compute positive debit to close; skipping close", underlying)
            return False
        close_debit_limit = current_net_credit * (1.0 + self.cfg.close_limit_slippage_pct)

        close_legs = _exit_to_order_legs(open_spread)
        client_order_id = f"alpaca_put_spread_close:{underlying}:{int(time.time())}"
        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(self.cfg.order_qty),
            limit_price=float(close_debit_limit),
            legs=close_legs,
            client_order_id=client_order_id,
        )
        logger.info("[%s] Submitted close order_id=%s limit_debit=%.4f", underlying, order_id, close_debit_limit)

        db_insert_order(
            order_id=order_id,
            underlying=underlying,
            side="close",
            status="submitted",
            client_order_id=client_order_id,
            short_put_symbol=open_spread.get("short_put_symbol"),
            long_put_symbol=open_spread.get("long_put_symbol"),
            limit_price=close_debit_limit,
            qty=self.cfg.order_qty,
        )
        log_event("close_submitted", f"order_id={order_id} reason={reason}", underlying=underlying, extra={"order_id": order_id})
        alert_close_submitted(underlying, order_id, reason or "close", close_debit_limit)

        # Persist pending close order immediately so we don't resubmit on restart.
        self.state["pending_close_order_by_underlying"][underlying] = {
            "order_id": order_id,
            "submitted_at_ts": time.time(),
            "close_debit_limit": close_debit_limit,
            "close_reason": reason,
        }
        save_state(self.state)

        final = wait_for_order(
            self.trading_client,
            order_id,
            timeout_seconds=self.cfg.order_fill_timeout_seconds,
        )
        status = str(getattr(final, "status", "")).lower()
        if status != "filled":
            try:
                cancel_order(self.trading_client, order_id)
            except Exception:
                pass
            # Clear pending so we can retry later (or stop, depending on retry logic).
            self.state["pending_close_order_by_underlying"].pop(underlying, None)
            save_state(self.state)
            logger.warning("[%s] Close order not filled (status=%s); leaving state", underlying, status)
            return False

        # Filled => DB, alerts, then clear state (same as async poll path)
        pending_close = self.state.get("pending_close_order_by_underlying", {}).get(underlying, {})
        spread_id = open_spread.get("spread_id")
        entry_credit = float(open_spread.get("entry_net_credit_mid", 0.0))
        close_debit = float(pending_close.get("close_debit_limit", 0.0) or 0.0)
        close_reason = pending_close.get("close_reason", "close")
        if spread_id and close_debit > 0:
            db_close_spread(spread_id, order_id, close_debit, close_reason, entry_credit, self.cfg.order_qty)
            pnl = (entry_credit - close_debit) * 100.0 * self.cfg.order_qty
            alert_close_filled(underlying, order_id, close_reason, pnl)
        db_update_order_status(order_id, "filled")
        log_event("close_filled", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id})

        self.state["pending_close_order_by_underlying"].pop(underlying, None)
        self.state["open_spread_by_underlying"].pop(underlying, None)
        # After a close fill, start cooldown gating for next entry hunting.
        self.state["cooldown_until_ts_by_underlying"][underlying] = time.time() + (
            int(self.cfg.exit_cooldown_minutes) * 60
        )
        save_state(self.state)
        logger.info("[%s] Close filled; state cleared", underlying)
        return True

    def _maybe_open(self, underlying: str) -> None:
        logger.info("[%s] scanning for candidate bull put credit spread...", underlying)
        open_spread = (self.state.get("open_spread_by_underlying") or {}).get(underlying)
        if open_spread:
            return
        if (self.state.get("entry_disabled_by_underlying") or {}).get(underlying):
            return

        pending_entry = (self.state.get("pending_entry_order_by_underlying") or {}).get(underlying)
        if pending_entry:
            order_id = pending_entry.get("order_id")
            submitted_at_ts = float(pending_entry.get("submitted_at_ts", 0.0) or 0.0)
            if not order_id:
                self.state["pending_entry_order_by_underlying"].pop(underlying, None)
                save_state(self.state)
                return

            status = self._get_order_status(order_id)
            if status == "filled":
                # Build open_spread from the stored candidate snapshot.
                short_sym = pending_entry.get("short_put_symbol")
                long_sym = pending_entry.get("long_put_symbol")
                entry_credit_mid = float(pending_entry.get("entry_net_credit_mid", 0.0))
                if short_sym and long_sym:
                    # Best-effort recompute entry credit from latest quotes.
                    mid_short = _option_mid(self.option_data_client, short_sym)
                    mid_long = _option_mid(self.option_data_client, long_sym)
                    if mid_short is not None and mid_long is not None:
                        entry_credit_mid = float(mid_short - mid_long)
                    underlying_mid = get_latest_mid(self.stock_data_client, underlying)
                    spread_id = db_insert_spread(
                        underlying=underlying,
                        short_put_symbol=short_sym,
                        long_put_symbol=long_sym,
                        entry_credit_mid=entry_credit_mid,
                        entry_order_id=order_id,
                    )
                    db_update_order_status(order_id, "filled")
                    open_spread = {
                        "spread_id": spread_id,
                        "entry_order_id": order_id,
                        "short_put_symbol": short_sym,
                        "long_put_symbol": long_sym,
                        "entry_net_credit_mid": float(entry_credit_mid),
                        "entry_underlying_mid": float(underlying_mid) if underlying_mid else None,
                        "opened_at_ts": float(pending_entry.get("submitted_at_ts", time.time())),
                    }
                    self.state["open_spread_by_underlying"][underlying] = open_spread
                    log_event("entry_filled", f"order_id={order_id} spread_id={spread_id}", underlying=underlying, extra={"order_id": order_id, "spread_id": spread_id})
                    alert_entry_filled(underlying, order_id, entry_credit_mid)
                self.state["pending_entry_order_by_underlying"].pop(underlying, None)
                self.state["entry_retry_count_by_underlying"].pop(underlying, None)
                self.state["entry_disabled_by_underlying"].pop(underlying, None)
                save_state(self.state)
                logger.info("[%s] Entry filled; pending->open state stored", underlying)
                return

            if self._is_terminal_order_status(status) and status != "filled":
                # Don't retry: clear the pending order and disable further hunting
                # for this underlying until you restart / manually change config/state.
                self.state["pending_entry_order_by_underlying"].pop(underlying, None)
                self.state["entry_disabled_by_underlying"][underlying] = True
                logger.warning("[%s] Entry order terminal status=%s; not retrying => disabling entry", underlying, status)
                save_state(self.state)
                return

            # Still working: keep polling on every loop; do not submit a new entry.
            return

        # Entry retry disabled/attempt tracking is handled above.
        # Only gate hunting when we are about to submit a *new* entry (no pending order).
        if not self._within_hunt_window():
            logger.info(
                "[%s] entry hunting gated by trade window (tz=%s weekdays=%s start=%s end=%s); skipping open",
                underlying,
                self.cfg.trade_window_timezone,
                sorted(self.cfg.trade_window_weekdays),
                self.cfg.trade_window_start_time_local,
                self.cfg.trade_window_end_time_local,
            )
            return

        # Loss cap gating: block new entries if daily PnL exceeds configured caps.
        if self.cfg.max_daily_loss_dollars > 0:
            daily_pnl = get_daily_pnl()
            if daily_pnl <= -self.cfg.max_daily_loss_dollars:
                logger.warning(
                    "[%s] Entry hunting blocked by daily loss cap: daily_pnl=%.2f cap=%.2f",
                    underlying, daily_pnl, self.cfg.max_daily_loss_dollars,
                )
                log_event("loss_cap_block", f"daily PnL={daily_pnl:.2f} <= -{self.cfg.max_daily_loss_dollars}", underlying=underlying)
                alert_loss_cap_hit(None, daily_pnl, self.cfg.max_daily_loss_dollars)
                return
        if self.cfg.max_loss_per_underlying_dollars > 0:
            und_pnl = get_pnl_by_underlying_today(underlying)
            if und_pnl <= -self.cfg.max_loss_per_underlying_dollars:
                logger.warning(
                    "[%s] Entry hunting blocked by per-underlying loss cap: pnl=%.2f cap=%.2f",
                    underlying, und_pnl, self.cfg.max_loss_per_underlying_dollars,
                )
                log_event("loss_cap_block", f"{underlying} PnL={und_pnl:.2f} <= -{self.cfg.max_loss_per_underlying_dollars}", underlying=underlying)
                alert_loss_cap_hit(underlying, und_pnl, self.cfg.max_loss_per_underlying_dollars)
                return

        # Cooldown gating: after a close fills, wait N minutes before hunting
        # for a new entry for the same underlying.
        cooldown_until_ts = float(
            (self.state.get("cooldown_until_ts_by_underlying") or {}).get(underlying, 0.0) or 0.0
        )
        now_ts = time.time()
        if cooldown_until_ts and now_ts < cooldown_until_ts:
            remaining_s = cooldown_until_ts - now_ts
            logger.info(
                "[%s] Entry hunting cooldown active: %.0fs remaining",
                underlying,
                remaining_s,
            )
            return

        # Select candidate bull put credit spread using option chain filters
        try:
            underlying_mid = get_latest_mid(self.stock_data_client, underlying)
            candidate = select_bull_put_credit_spread(
                underlying=underlying,
                option_data_client=self.option_data_client,
                cfg=self.cfg,
                underlying_spot_mid=underlying_mid if underlying_mid and underlying_mid > 0 else None,
            )
        except Exception as e:
            logger.warning("[%s] option-chain selection failed: %s", underlying, e)
            log_event("api_error", str(e), underlying=underlying, extra={"operation": "option_chain"})
            alert_api_error(underlying, "option_chain", str(e))
            return

        if not candidate:
            logger.info("[%s] no candidate spread found (filters/target not met).", underlying)
            return

        logger.info(
            "[%s] Candidate bull put credit spread: long=%s short=%s entry_net_credit_mid=%.4f",
            underlying,
            candidate.long_put_symbol,
            candidate.short_put_symbol,
            candidate.entry_net_credit_mid,
        )

        if not self.cfg.execute:
            logger.info("[%s] execute=false; would submit entry mleg now", underlying)
            return

        # Submit entry mleg:
        #   For mleg limit_price, negative => credit.
        entry_limit_credit = float(self.cfg.target_credit)
        entry_limit_price = -entry_limit_credit
        entry_legs = _entry_to_order_legs(candidate)
        client_order_id = f"alpaca_put_spread_open:{underlying}:{int(time.time())}"

        order_id = submit_mleg_limit_order(
            self.trading_client,
            qty=int(self.cfg.order_qty),
            limit_price=entry_limit_price,
            legs=entry_legs,
            client_order_id=client_order_id,
        )

        db_insert_order(
            order_id=order_id,
            underlying=underlying,
            side="open",
            status="submitted",
            client_order_id=client_order_id,
            short_put_symbol=candidate.short_put_symbol,
            long_put_symbol=candidate.long_put_symbol,
            limit_price=-entry_limit_credit,
            qty=self.cfg.order_qty,
            raw_snapshot={"short": candidate.short_put_symbol, "long": candidate.long_put_symbol},
        )
        log_event("entry_submitted", f"order_id={order_id}", underlying=underlying, extra={"order_id": order_id})
        alert_entry_submitted(underlying, order_id, entry_limit_credit)

        # Persist pending entry order immediately so we don't open multiples
        # for this underlying and can recover after restart.
        pending_entry_snapshot = {
            "order_id": order_id,
            "submitted_at_ts": time.time(),
            "short_put_symbol": candidate.short_put_symbol,
            "long_put_symbol": candidate.long_put_symbol,
            "entry_net_credit_mid": float(candidate.entry_net_credit_mid),
        }
        self.state["pending_entry_order_by_underlying"][underlying] = pending_entry_snapshot
        save_state(self.state)

        logger.info(
            "[%s] Submitted entry order_id=%s net_credit_limit=%.4f; will poll status until filled",
            underlying,
            order_id,
            entry_limit_credit,
        )
        # IMPORTANT: do not wait/retry here. We will evaluate TP/SL only after
        # the order status becomes 'filled' in subsequent loop iterations.
        return

    def run_forever(self) -> None:
        loop_no = 0
        while True:
            loop_no += 1
            for underlying in self.cfg.get_underlyings_for_today():
                try:
                    self._clear_open_spread_if_not_in_positions(underlying)
                    open_spread = (self.state.get("open_spread_by_underlying") or {}).get(underlying)
                    if open_spread:
                        self._maybe_close(underlying, open_spread)
                    else:
                        self._maybe_open(underlying)
                except Exception as e:
                    logger.exception("[%s] Loop error: %s", underlying, e)

            time.sleep(self.cfg.loop_sleep_seconds)

