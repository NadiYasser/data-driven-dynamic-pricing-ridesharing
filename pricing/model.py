"""Reusable dynamic pricing formula."""

from __future__ import annotations

import math


DEFAULT_MIN_MULTIPLIER = 0.9
DEFAULT_MAX_MULTIPLIER = 1.5
DEFAULT_PRESSURE_WEIGHT = 0.4


def calculate_pressure(demand_count: int, available_driver_count: int) -> float:
    """Return market pressure for one zone aggregation window."""

    if demand_count <= 0 and available_driver_count <= 0:
        return 0.0
    if available_driver_count <= 0:
        return math.inf
    return (float(demand_count) / float(available_driver_count)) - 1.0


def calculate_price_multiplier(
    demand_count: int,
    available_driver_count: int,
    *,
    min_multiplier: float = DEFAULT_MIN_MULTIPLIER,
    max_multiplier: float = DEFAULT_MAX_MULTIPLIER,
    pressure_weight: float = DEFAULT_PRESSURE_WEIGHT,
) -> float:
    """Calculate a clamped price multiplier from demand and available supply."""

    if min_multiplier <= 0:
        raise ValueError("min_multiplier must be positive.")
    if max_multiplier < min_multiplier:
        raise ValueError("max_multiplier must be greater than or equal to min_multiplier.")
    if pressure_weight <= 0:
        raise ValueError("pressure_weight must be positive.")

    pressure = calculate_pressure(demand_count, available_driver_count)
    if pressure == math.inf:
        return max_multiplier

    price = 1.0 + pressure_weight * math.tanh(pressure)
    return min(max_multiplier, max(min_multiplier, price))

