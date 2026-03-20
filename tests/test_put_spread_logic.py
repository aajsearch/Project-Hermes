"""
Tests for bot.alpaca_put_spread.put_spread_logic: TP/SL math, entry condition.
"""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.alpaca_put_spread.put_spread_logic import (
    entry_condition_met,
    net_credit_mid,
    tp_sl_triggered,
)


class TestNetCreditMid(unittest.TestCase):
    def test_basic(self):
        self.assertAlmostEqual(net_credit_mid(2.0, 1.0), 1.0)
        self.assertAlmostEqual(net_credit_mid(0.25, 0.10), 0.15)

    def test_zero_long(self):
        self.assertAlmostEqual(net_credit_mid(0.20, 0.0), 0.20)


class TestEntryConditionMet(unittest.TestCase):
    def test_ge_pass(self):
        self.assertTrue(entry_condition_met(0.20, 0.15, ">="))
        self.assertTrue(entry_condition_met(0.15, 0.15, ">="))

    def test_ge_fail(self):
        self.assertFalse(entry_condition_met(0.14, 0.15, ">="))

    def test_le_pass(self):
        self.assertTrue(entry_condition_met(0.10, 0.15, "<="))
        self.assertTrue(entry_condition_met(0.15, 0.15, "<="))

    def test_le_fail(self):
        self.assertFalse(entry_condition_met(0.16, 0.15, "<="))


class TestTpSlTriggered(unittest.TestCase):
    def test_tp_triggers_when_credit_decays(self):
        # entry=0.20, tp_pct=0.60 => tp_thr = 0.20 * 0.4 = 0.08
        # current <= 0.08 => TP
        triggered, reason = tp_sl_triggered(0.07, 0.20, 0.60, 0.75)
        self.assertTrue(triggered)
        self.assertEqual(reason, "tp")

    def test_tp_boundary(self):
        triggered, reason = tp_sl_triggered(0.08, 0.20, 0.60, 0.75)
        self.assertTrue(triggered)
        self.assertEqual(reason, "tp")

    def test_sl_triggers_when_credit_widens(self):
        # entry=0.20, sl_pct=2.0 => sl_thr = 0.20 * 3 = 0.60
        # current >= 0.60 => SL
        triggered, reason = tp_sl_triggered(0.61, 0.20, 0.60, 2.0)
        self.assertTrue(triggered)
        self.assertEqual(reason, "sl")

    def test_sl_boundary(self):
        # Exactly at sl_thr (0.20 * 3 = 0.60); use slightly above to avoid float edge
        triggered, reason = tp_sl_triggered(0.601, 0.20, 0.60, 2.0)
        self.assertTrue(triggered)
        self.assertEqual(reason, "sl")

    def test_neither_triggers_in_range(self):
        triggered, reason = tp_sl_triggered(0.15, 0.20, 0.60, 2.0)
        self.assertFalse(triggered)
        self.assertEqual(reason, "")

    def test_zero_entry_returns_false(self):
        triggered, reason = tp_sl_triggered(0.10, 0.0, 0.60, 2.0)
        self.assertFalse(triggered)
        self.assertEqual(reason, "")

    def test_tp_takes_precedence_when_both_would_trigger(self):
        # If current is below tp_thr, TP fires first (we check tp before sl)
        triggered, reason = tp_sl_triggered(0.0, 0.20, 0.60, 2.0)
        self.assertTrue(triggered)
        self.assertEqual(reason, "tp")
