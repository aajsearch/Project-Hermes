"""
Tests for bot.alpaca_put_spread.pricing_logic: TP/SL math, entry condition, leg net credit.
"""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.alpaca_put_spread.domain import Leg
from bot.alpaca_put_spread.pricing_logic import (
    current_net_credit_mid_from_legs,
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


class TestCurrentNetCreditFromLegs(unittest.TestCase):
    def test_pcs_two_legs_matches_short_minus_long(self):
        legs = (
            Leg(symbol="S", side="sell", intent="open", ratio=1),
            Leg(symbol="L", side="buy", intent="open", ratio=1),
        )

        def quotes(sym: str):
            if sym == "S":
                return (0.20, 0.22)
            if sym == "L":
                return (0.08, 0.10)
            return (None, None)

        # mids 0.21 and 0.09 -> 0.21 - 0.09 = 0.12
        self.assertAlmostEqual(current_net_credit_mid_from_legs(legs, quotes), 0.12)

    def test_ratio_doubles_contribution(self):
        legs = (Leg(symbol="S", side="sell", intent="open", ratio=2),)

        def quotes(sym: str):
            return (0.10, 0.10)

        self.assertAlmostEqual(current_net_credit_mid_from_legs(legs, quotes), 0.20)

    def test_missing_quote_returns_none(self):
        legs = (Leg(symbol="X", side="sell", intent="open", ratio=1),)

        def quotes(sym: str):
            return (None, None)

        self.assertIsNone(current_net_credit_mid_from_legs(legs, quotes))


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

    def test_tp_via_legs_and_bid_ask(self):
        legs = (
            Leg(symbol="S", side="sell", intent="open", ratio=1),
            Leg(symbol="L", side="buy", intent="open", ratio=1),
        )

        def quotes(sym: str):
            # mids 0.10 and 0.03 => net 0.07, entry 0.20 tp 60% => thr 0.08
            if sym == "S":
                return (0.10, 0.10)
            if sym == "L":
                return (0.03, 0.03)
            return (None, None)

        triggered, reason = tp_sl_triggered(
            None,
            0.20,
            0.60,
            0.75,
            legs=legs,
            bid_ask_for=quotes,
        )
        self.assertTrue(triggered)
        self.assertEqual(reason, "tp")

    def test_no_trigger_when_legs_missing_quote(self):
        legs = (Leg(symbol="S", side="sell", intent="open", ratio=1),)

        def quotes(sym: str):
            return (None, None)

        triggered, reason = tp_sl_triggered(
            None,
            0.20,
            0.60,
            0.75,
            legs=legs,
            bid_ask_for=quotes,
        )
        self.assertFalse(triggered)
        self.assertEqual(reason, "")
