"""
Email alerts for Alpaca put spread bot. Uses SMTP env vars (SMTP_HOST, SMTP_PORT, etc.).
Fails silently if SMTP not configured so the bot keeps running.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def _to_email() -> Optional[str]:
    return (os.environ.get("SMTP_TO") or "").strip() or None


def _alerts_enabled() -> bool:
    """
    Alert sending toggle.
    Default is disabled to avoid noisy SMTP failures during stress tests.
    Set ALPACA_ALERTS_ENABLED=1/true/yes/on to enable.
    """
    raw = (os.environ.get("ALPACA_ALERTS_ENABLED") or "").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def send_alert(subject: str, body: str) -> bool:
    """
    Send email alert. Returns True if sent, False if skipped (no SMTP_TO) or failed.
    """
    if not _alerts_enabled():
        logger.debug("Alerts disabled; skipping alert: %s", subject[:60])
        return False

    to_addr = _to_email()
    if not to_addr:
        logger.debug("SMTP_TO not set; skipping alert: %s", subject[:60])
        return False
    try:
        from bot.email_reporter import send_ledger_email
        send_ledger_email(to_email=to_addr, subject=subject, body_text=body, attachments=None)
        logger.info("Alert sent: %s", subject[:60])
        return True
    except Exception as e:
        logger.warning("Failed to send alert: %s", e)
        return False


def alert_entry_submitted(underlying: str, order_id: str, limit_credit: float) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Entry submitted {underlying}",
        body=f"Entry order submitted.\nUnderlying: {underlying}\nOrder ID: {order_id}\nLimit credit: {limit_credit:.4f}",
    )


def alert_entry_filled(underlying: str, order_id: str, entry_credit: float) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Entry filled {underlying}",
        body=f"Entry order filled.\nUnderlying: {underlying}\nOrder ID: {order_id}\nEntry credit mid: {entry_credit:.4f}",
    )


def alert_close_submitted(underlying: str, order_id: str, reason: str, limit_debit: float) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Close submitted {underlying} ({reason})",
        body=f"Close order submitted.\nUnderlying: {underlying}\nOrder ID: {order_id}\nReason: {reason}\nLimit debit: {limit_debit:.4f}",
    )


def alert_close_filled(underlying: str, order_id: str, reason: str, pnl_dollars: float) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Close filled {underlying} ({reason}) PnL=${pnl_dollars:.2f}",
        body=f"Close order filled.\nUnderlying: {underlying}\nOrder ID: {order_id}\nReason: {reason}\nPnL: ${pnl_dollars:.2f}",
    )


def alert_sl_triggered(underlying: str, reason: str) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Stop-loss triggered {underlying}",
        body=f"Stop-loss triggered.\nUnderlying: {underlying}\nReason: {reason}",
    )


def alert_tp_triggered(underlying: str, reason: str) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] Take-profit triggered {underlying}",
        body=f"Take-profit triggered.\nUnderlying: {underlying}\nReason: {reason}",
    )


def alert_api_error(underlying: str, operation: str, error: str) -> None:
    send_alert(
        subject=f"[Alpaca Put Spread] API error {underlying}",
        body=f"API error during {operation}.\nUnderlying: {underlying}\nError: {error}",
    )


def alert_loss_cap_hit(underlying: Optional[str], daily_pnl: float, cap_dollars: float) -> None:
    scope = f"underlying {underlying}" if underlying else "global daily"
    send_alert(
        subject=f"[Alpaca Put Spread] Loss cap hit ({scope})",
        body=f"Loss cap reached. {scope}\nDaily PnL: ${daily_pnl:.2f}\nCap: ${cap_dollars:.2f}\nNo new entries until next day.",
    )
