"""Unit tests for the dynamic pricing formula."""

from __future__ import annotations

import math
import unittest

from pricing.model import calculate_pressure, calculate_price_multiplier


class PricingModelTest(unittest.TestCase):
    def test_balanced_market_returns_one(self) -> None:
        self.assertEqual(calculate_pressure(10, 10), 0.0)
        self.assertEqual(calculate_price_multiplier(10, 10), 1.0)

    def test_more_demand_increases_multiplier(self) -> None:
        self.assertGreater(calculate_price_multiplier(15, 10), 1.0)

    def test_more_supply_lowers_multiplier(self) -> None:
        self.assertLess(calculate_price_multiplier(5, 10), 1.0)

    def test_min_multiplier_clamp(self) -> None:
        self.assertEqual(calculate_price_multiplier(0, 1000), 0.9)

    def test_max_multiplier_clamp(self) -> None:
        self.assertEqual(calculate_price_multiplier(1000, 1, pressure_weight=1.0), 1.5)

    def test_zero_supply_with_demand_returns_max(self) -> None:
        self.assertEqual(calculate_pressure(5, 0), math.inf)
        self.assertEqual(calculate_price_multiplier(5, 0), 1.5)

    def test_zero_demand_and_zero_supply_returns_neutral(self) -> None:
        self.assertEqual(calculate_pressure(0, 0), 0.0)
        self.assertEqual(calculate_price_multiplier(0, 0), 1.0)


if __name__ == "__main__":
    unittest.main()
