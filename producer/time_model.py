"""Time-of-day behavior model for ride-sharing event simulation."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)
VALID_DAY_TYPES = {"weekday", "weekend"}
REQUIRED_PATTERN_FIELDS = {"pattern_id", "day_type", "hours", "zone_type_multipliers"}
DEFAULT_FACTORS = {"demand_factor": 1.0, "supply_factor": 1.0}


def get_day_type(timestamp: datetime) -> str:
    """Return weekday/weekend classification for a timestamp."""

    return "weekday" if timestamp.weekday() < 5 else "weekend"


def load_time_patterns_config(path: str | Path = "config/time_patterns.json") -> dict[str, Any]:
    """Load and validate time-pattern simulation config."""

    with Path(path).open("r", encoding="utf-8") as file:
        config = json.load(file)
    validate_time_patterns_config(config)
    return config


def get_time_factors(
    timestamp: datetime,
    zone_type: str,
    time_patterns_config: dict[str, Any],
) -> dict[str, float]:
    """Return demand/supply factors for a timestamp and zone type.

    If no pattern matches, or the matching pattern does not define the zone
    type, this returns the config default factors.
    """

    hour = timestamp.hour
    day_type = get_day_type(timestamp)
    default = _default_factors(time_patterns_config)

    for pattern in time_patterns_config.get("time_patterns", []):
        if pattern["day_type"] == day_type and hour in pattern["hours"]:
            factors = pattern["zone_type_multipliers"].get(zone_type)
            if factors is None:
                LOGGER.debug(
                    "Time pattern %s matched for day_type=%s hour=%s, but zone_type=%s is missing. "
                    "Using default factors.",
                    pattern["pattern_id"],
                    day_type,
                    hour,
                    zone_type,
                )
                return default

            LOGGER.debug(
                "Selected time pattern %s for day_type=%s hour=%s zone_type=%s",
                pattern["pattern_id"],
                day_type,
                hour,
                zone_type,
            )
            return {
                "demand_factor": float(factors["demand_factor"]),
                "supply_factor": float(factors["supply_factor"]),
            }

    LOGGER.debug(
        "No time pattern matched for day_type=%s hour=%s zone_type=%s. Using default factors.",
        day_type,
        hour,
        zone_type,
    )
    return default


def validate_time_patterns_config(config: dict[str, Any]) -> None:
    """Validate time-pattern config structure and positive multiplier values."""

    if not isinstance(config, dict):
        raise ValueError("Time patterns config must be a JSON object.")

    patterns = config.get("time_patterns")
    if not isinstance(patterns, list) or not patterns:
        raise ValueError("time_patterns must be a non-empty list.")

    default = config.get("default", DEFAULT_FACTORS)
    _validate_factors(default, "default")

    for index, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            raise ValueError(f"time_patterns[{index}] must be an object.")

        missing = REQUIRED_PATTERN_FIELDS - set(pattern)
        if missing:
            raise ValueError(
                f"time_patterns[{index}] is missing required fields: {sorted(missing)}"
            )

        pattern_id = pattern["pattern_id"]
        day_type = pattern["day_type"]
        hours = pattern["hours"]
        multipliers = pattern["zone_type_multipliers"]

        if not isinstance(pattern_id, str) or not pattern_id:
            raise ValueError(f"time_patterns[{index}].pattern_id must be a non-empty string.")

        if day_type not in VALID_DAY_TYPES:
            raise ValueError(
                f"time_patterns[{index}].day_type must be one of {sorted(VALID_DAY_TYPES)}."
            )

        if not isinstance(hours, list) or not hours:
            raise ValueError(f"time_patterns[{index}].hours must be a non-empty list.")

        for hour in hours:
            if not isinstance(hour, int) or hour < 0 or hour > 23:
                raise ValueError(
                    f"time_patterns[{index}].hours contains invalid hour {hour!r}; "
                    "hours must be integers between 0 and 23."
                )

        if not isinstance(multipliers, dict) or not multipliers:
            raise ValueError(
                f"time_patterns[{index}].zone_type_multipliers must be a non-empty object."
            )

        for zone_type, factors in multipliers.items():
            if not isinstance(zone_type, str) or not zone_type:
                raise ValueError(f"time_patterns[{index}] has an invalid zone type key.")
            _validate_factors(factors, f"time_patterns[{index}].{zone_type}")


def _default_factors(config: dict[str, Any]) -> dict[str, float]:
    default = config.get("default", DEFAULT_FACTORS)
    return {
        "demand_factor": float(default["demand_factor"]),
        "supply_factor": float(default["supply_factor"]),
    }


def _validate_factors(factors: Any, context: str) -> None:
    if not isinstance(factors, dict):
        raise ValueError(f"{context} factors must be an object.")

    for field in ("demand_factor", "supply_factor"):
        value = factors.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{context}.{field} must be a positive number.")
