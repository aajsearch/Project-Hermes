"""Tests for Kalshi market-sell exit price parsing (fills-based, no 100-x inversion)."""
from __future__ import annotations

import unittest

from bot.pipeline.executor import _exit_price_cents_from_fill_rows


class TestExitPriceFromFills(unittest.TestCase):
    def test_no_side_weighted_average(self):
        fills = [
            {"count_fp": "5.00", "no_price_dollars": "0.6800"},
            {"count_fp": "5.00", "no_price_dollars": "0.6900"},
        ]
        # (5*68 + 5*69) / 10 = 68.5 -> 69
        self.assertEqual(_exit_price_cents_from_fill_rows(fills, "no"), 69)

    def test_yes_side_uses_yes_price_dollars(self):
        fills = [
            {"count_fp": "10.00", "yes_price_dollars": "0.4250"},
        ]
        self.assertEqual(_exit_price_cents_from_fill_rows(fills, "yes"), 43)

    def test_deprecated_fixed_alias(self):
        fills = [{"count_fp": "2.00", "no_price_fixed": "0.3150"}]
        self.assertEqual(_exit_price_cents_from_fill_rows(fills, "no"), 32)

    def test_empty(self):
        self.assertIsNone(_exit_price_cents_from_fill_rows([], "no"))
        self.assertIsNone(_exit_price_cents_from_fill_rows([{}], "no"))


if __name__ == "__main__":
    unittest.main()
