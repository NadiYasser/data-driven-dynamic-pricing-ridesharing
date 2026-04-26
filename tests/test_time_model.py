"""Unit tests for producer.time_model."""

from __future__ import annotations

import copy
import unittest
from datetime import datetime

from producer.time_model import get_day_type, get_time_factors, validate_time_patterns_config


TIME_PATTERNS_CONFIG = {
    "time_patterns": [
        {
            "pattern_id": "weekday_morning_peak",
            "day_type": "weekday",
            "hours": [7, 8, 9],
            "zone_type_multipliers": {
                "residential": {"demand_factor": 1.8, "supply_factor": 1.1},
                "downtown": {"demand_factor": 1.5, "supply_factor": 1.3},
                "suburb": {"demand_factor": 1.4, "supply_factor": 0.9},
                "airport": {"demand_factor": 1.1, "supply_factor": 1.2},
            },
        },
        {
            "pattern_id": "weekday_night",
            "day_type": "weekday",
            "hours": [20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6],
            "zone_type_multipliers": {
                "residential": {"demand_factor": 0.6, "supply_factor": 0.7},
                "downtown": {"demand_factor": 0.8, "supply_factor": 0.8},
                "suburb": {"demand_factor": 0.4, "supply_factor": 0.5},
                "airport": {"demand_factor": 0.9, "supply_factor": 0.8},
            },
        },
        {
            "pattern_id": "weekend_day",
            "day_type": "weekend",
            "hours": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            "zone_type_multipliers": {
                "residential": {"demand_factor": 1.1, "supply_factor": 1.0},
                "downtown": {"demand_factor": 1.3, "supply_factor": 1.2},
                "suburb": {"demand_factor": 1.0, "supply_factor": 0.9},
                "airport": {"demand_factor": 1.4, "supply_factor": 1.3},
            },
        },
        {
            "pattern_id": "weekend_night",
            "day_type": "weekend",
            "hours": [18, 19, 20, 21, 22, 23, 0, 1, 2],
            "zone_type_multipliers": {
                "residential": {"demand_factor": 1.3, "supply_factor": 0.9},
                "downtown": {"demand_factor": 2.0, "supply_factor": 1.2},
                "suburb": {"demand_factor": 0.8, "supply_factor": 0.6},
                "airport": {"demand_factor": 1.2, "supply_factor": 1.0},
            },
        },
    ],
    "default": {"demand_factor": 1.0, "supply_factor": 1.0},
}


class TimeModelTest(unittest.TestCase):
    def test_get_day_type(self) -> None:
        self.assertEqual(get_day_type(datetime(2026, 4, 24, 8)), "weekday")
        self.assertEqual(get_day_type(datetime(2026, 4, 25, 8)), "weekend")

    def test_weekday_morning_peak(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 24, 8), "residential", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 1.8, "supply_factor": 1.1})

    def test_weekday_night(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 24, 23), "downtown", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 0.8, "supply_factor": 0.8})

    def test_weekend_day(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 25, 12), "airport", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 1.4, "supply_factor": 1.3})

    def test_weekend_night(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 25, 22), "downtown", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 2.0, "supply_factor": 1.2})

    def test_missing_zone_type_fallback(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 24, 8), "industrial", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 1.0, "supply_factor": 1.0})

    def test_no_matching_hour_fallback(self) -> None:
        factors = get_time_factors(datetime(2026, 4, 25, 5), "downtown", TIME_PATTERNS_CONFIG)
        self.assertEqual(factors, {"demand_factor": 1.0, "supply_factor": 1.0})

    def test_validation_rejects_invalid_hour(self) -> None:
        config = copy.deepcopy(TIME_PATTERNS_CONFIG)
        config["time_patterns"][0]["hours"] = [24]
        with self.assertRaises(ValueError):
            validate_time_patterns_config(config)

    def test_validation_rejects_negative_factor(self) -> None:
        config = copy.deepcopy(TIME_PATTERNS_CONFIG)
        config["time_patterns"][0]["zone_type_multipliers"]["residential"]["demand_factor"] = -1
        with self.assertRaises(ValueError):
            validate_time_patterns_config(config)


if __name__ == "__main__":
    unittest.main()
