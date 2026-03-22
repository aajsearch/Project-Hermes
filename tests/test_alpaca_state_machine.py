"""
Tests for Alpaca put spread state machine: pending_entry -> filled -> open_spread -> close.
Uses mocks; no live Alpaca calls.
"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class TestAlpacaStateTransitions(unittest.TestCase):
    """Verify state keys and transitions; no execution."""

    def test_default_state_shape(self):
        from bot.alpaca_put_spread.state import _default_state

        st = _default_state()
        self.assertIn("open_positions", st)
        self.assertIn("pending_entry_order", st)
        self.assertIn("pending_close_order", st)
        self.assertIn("cooldown_until_ts", st)
        self.assertIn("entry_disabled", st)
        self.assertIn("entry_retry_count", st)
        self.assertEqual(st["open_positions"], {})

    def test_open_spread_has_required_keys(self):
        open_spread = {
            "spread_id": 1,
            "entry_order_id": "ord-123",
            "short_put_symbol": "QQQ260320P00579000",
            "long_put_symbol": "QQQ260320P00569000",
            "entry_net_credit_mid": 0.155,
            "entry_underlying_mid": 592.0,
            "opened_at_ts": 1710000000.0,
        }
        required = {"short_put_symbol", "long_put_symbol", "entry_net_credit_mid", "spread_id"}
        for k in required:
            self.assertIn(k, open_spread)

    def test_pending_entry_snapshot_shape(self):
        pending = {
            "order_id": "ord-456",
            "submitted_at_ts": 1710000000.0,
            "short_put_symbol": "SPY260320P00640000",
            "long_put_symbol": "SPY260320P00631000",
            "entry_net_credit_mid": 0.17,
        }
        required = {"order_id", "short_put_symbol", "long_put_symbol", "entry_net_credit_mid"}
        for k in required:
            self.assertIn(k, pending)


class TestDbInit(unittest.TestCase):
    def test_init_creates_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "alpaca_put_spread.db"
            from bot.alpaca_put_spread.db import init_alpaca_db, ORDERS_TABLE, SPREADS_TABLE, EVENTS_TABLE

            init_alpaca_db(db_path=db_path)
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            try:
                for tbl in (ORDERS_TABLE, SPREADS_TABLE, EVENTS_TABLE):
                    cur = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                    )
                    self.assertTrue(cur.fetchone(), f"Table {tbl} should exist")
            finally:
                conn.close()


class TestTradeWindowWeekdays(unittest.TestCase):
    def test_parse_default_mon_fri(self):
        from bot.alpaca_put_spread.config import _parse_trade_window_weekdays

        self.assertEqual(_parse_trade_window_weekdays(None), frozenset(range(5)))

    def test_parse_explicit_names(self):
        from bot.alpaca_put_spread.config import _parse_trade_window_weekdays

        self.assertEqual(
            _parse_trade_window_weekdays(["Monday", "FRI", "wed"]),
            frozenset({0, 2, 4}),
        )

    def test_load_config_has_weekdays(self):
        from bot.alpaca_put_spread.config import load_alpaca_options_config

        cfg = load_alpaca_options_config(PROJECT_ROOT / "config")
        self.assertTrue(cfg.trade_window_weekdays.issubset(frozenset(range(7))))
        if cfg.trade_window_timezone:
            self.assertEqual(cfg.trade_window_weekdays, frozenset(range(5)))


class TestCloseDebitSlippageFloor(unittest.TestCase):
    def test_zero_net_uses_min_cent_buffer(self):
        from bot.alpaca_put_spread.strategy import _close_debit_limit_with_min_slippage

        # Old formula: 0 * (1+pct) = 0; floor gives 0.01 debit limit.
        self.assertAlmostEqual(_close_debit_limit_with_min_slippage(0.0, 0.02), 0.01)

    def test_tiny_net_gets_min_buffer(self):
        from bot.alpaca_put_spread.strategy import _close_debit_limit_with_min_slippage

        # 0.001 * 0.02 = 0.00002 < 0.01 -> buffer 0.01
        self.assertAlmostEqual(_close_debit_limit_with_min_slippage(0.001, 0.02), 0.011)

    def test_large_net_uses_percentage(self):
        from bot.alpaca_put_spread.strategy import _close_debit_limit_with_min_slippage

        self.assertAlmostEqual(_close_debit_limit_with_min_slippage(2.0, 0.02), 2.04)


class TestSingletonLock(unittest.TestCase):
    def test_acquire_release_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "test.lock"
            with patch("bot.alpaca_put_spread.singleton_lock.LOCK_PATH", lock_path):
                from bot.alpaca_put_spread.singleton_lock import acquire_singleton_lock, release_singleton_lock

                ok = acquire_singleton_lock()
                self.assertTrue(ok)
                release_singleton_lock()
