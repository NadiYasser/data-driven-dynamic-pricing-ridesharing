"""Kafka producer for simulated ride-sharing marketplace events.

The generator reads zone instances from MongoDB, applies time-of-day behavior
from config/time_patterns.json, and emits ride request and driver update events
to Kafka.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from producer.time_model import get_time_factors, load_time_patterns_config


LOGGER = logging.getLogger(__name__)

RIDE_REQUESTS_TOPIC = os.getenv("RIDE_REQUESTS_TOPIC", "ride_requests")
DRIVER_UPDATES_TOPIC = os.getenv("DRIVER_UPDATES_TOPIC", "driver_updates")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_API_VERSION = tuple(int(part) for part in os.getenv("KAFKA_API_VERSION", "3.9").split("."))
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27017/dynamic_pricing?authSource=admin",
)
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "dynamic_pricing")
MONGO_ZONES_COLLECTION = os.getenv("MONGO_ZONES_COLLECTION", "zones")
TIME_PATTERNS_PATH = os.getenv("TIME_PATTERNS_PATH", "config/time_patterns.json")
EMIT_INTERVAL_SECONDS = float(os.getenv("EMIT_INTERVAL_SECONDS", "1"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


def configure_logging() -> None:
    """Configure concise console logging."""

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def create_kafka_producer() -> KafkaProducer:
    """Connect to Kafka with retries while Docker services start."""

    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                api_version=KAFKA_API_VERSION,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                linger_ms=20,
                retries=5,
            )
        except NoBrokersAvailable:
            LOGGER.info("Kafka is not ready yet, retrying (%s/30)...", attempt)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")


def load_zones_from_mongo() -> list[dict[str, Any]]:
    """Load generated H3 zone metadata from MongoDB."""

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    for attempt in range(1, 31):
        try:
            client.admin.command("ping")
            break
        except ServerSelectionTimeoutError:
            LOGGER.info("MongoDB is not ready yet, retrying (%s/30)...", attempt)
            time.sleep(2)
    else:
        raise RuntimeError(f"Could not connect to MongoDB with URI {MONGO_URI}")

    collection = client[MONGO_DATABASE][MONGO_ZONES_COLLECTION]
    zones = list(
        collection.find(
            {},
            {
                "_id": 0,
                "zone_id": 1,
                "h3_zone": 1,
                "type": 1,
                "base_demand": 1,
                "base_supply": 1,
                "avg_distance": 1,
                "price_sensitivity": 1,
                "center_lat": 1,
                "center_lon": 1,
            },
        ).sort("h3_zone", 1)
    )
    client.close()

    if not zones:
        raise RuntimeError(
            "No zones found in MongoDB. Run `python scripts/seed_zones.py` before starting "
            "the event generator."
        )

    required_fields = {
        "zone_id",
        "h3_zone",
        "type",
        "base_demand",
        "base_supply",
        "avg_distance",
        "price_sensitivity",
        "center_lat",
        "center_lon",
    }
    for zone in zones:
        missing = required_fields - set(zone)
        if missing:
            raise ValueError(f"Zone {zone.get('h3_zone', '<unknown>')} missing fields: {sorted(missing)}")

    LOGGER.info("Loaded %s zones from MongoDB collection %s.%s", len(zones), MONGO_DATABASE, MONGO_ZONES_COLLECTION)
    return zones


def jitter_location(zone: dict[str, Any]) -> tuple[float, float]:
    """Generate a point near the H3 cell center."""

    lat = float(zone["center_lat"]) + random.uniform(-0.0025, 0.0025)
    lon = float(zone["center_lon"]) + random.uniform(-0.0025, 0.0025)
    return round(lat, 6), round(lon, 6)


def build_ride_request(zone: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
    """Create one ride request event."""

    lat, lon = jitter_location(zone)
    avg_distance = float(zone["avg_distance"])
    ride_distance = max(0.3, random.gauss(avg_distance, max(0.1, avg_distance * 0.25)))
    price_sensitivity = float(zone["price_sensitivity"])
    generosity = max(0.0, min(1.0, random.gauss(1.0 / (1.0 + price_sensitivity), 0.1)))

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "user_id": f"user_{random.randint(1, 50000):05d}",
        "lat": lat,
        "lon": lon,
        "h3_zone": zone["h3_zone"],
        "ride_distance": round(ride_distance, 2),
        "generosity": round(generosity, 3),
    }


def build_driver_update(zone: dict[str, Any], timestamp: datetime) -> dict[str, Any]:
    """Create one driver status update event."""

    lat, lon = jitter_location(zone)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "driver_id": f"driver_{random.randint(1, 20000):05d}",
        "lat": lat,
        "lon": lon,
        "h3_zone": zone["h3_zone"],
        "status": "available" if random.random() < 0.75 else "busy",
    }


def emit_events_for_tick(
    producer: KafkaProducer,
    zones: list[dict[str, Any]],
    time_patterns_config: dict[str, Any],
    timestamp: datetime,
) -> tuple[int, int]:
    """Emit one simulation tick for every generated zone."""

    total_requests = 0
    total_drivers = 0

    for zone in zones:
        zone_type = zone["type"]
        factors = get_time_factors(timestamp, zone_type, time_patterns_config)

        lambda_zt = float(zone["base_demand"]) * factors["demand_factor"]
        request_count = int(np.random.poisson(lambda_zt))

        small_noise = random.uniform(-2.0, 2.0)
        driver_count = float(zone["base_supply"]) * factors["supply_factor"] + small_noise
        driver_count = max(1, int(driver_count))

        LOGGER.debug(
            "zone=%s type=%s demand_factor=%.2f supply_factor=%.2f requests=%s drivers=%s",
            zone["h3_zone"],
            zone_type,
            factors["demand_factor"],
            factors["supply_factor"],
            request_count,
            driver_count,
        )

        for _ in range(request_count):
            event = build_ride_request(zone, timestamp)
            producer.send(RIDE_REQUESTS_TOPIC, key=zone["h3_zone"], value=event)
            total_requests += 1

        for _ in range(driver_count):
            event = build_driver_update(zone, timestamp)
            producer.send(DRIVER_UPDATES_TOPIC, key=zone["h3_zone"], value=event)
            total_drivers += 1

    producer.flush(timeout=10)
    return total_requests, total_drivers


def main() -> None:
    """Run the continuous simulation."""

    configure_logging()
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    zones = load_zones_from_mongo()
    time_patterns_config = load_time_patterns_config(TIME_PATTERNS_PATH)
    producer = create_kafka_producer()
    running = True

    def stop_handler(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    LOGGER.info(
        "Producing ride-sharing events to Kafka topics %s and %s",
        RIDE_REQUESTS_TOPIC,
        DRIVER_UPDATES_TOPIC,
    )

    while running:
        timestamp = utc_now()
        request_count, driver_count = emit_events_for_tick(
            producer,
            zones,
            time_patterns_config,
            timestamp,
        )
        LOGGER.info(
            "Emitted ride_requests=%s driver_updates=%s timestamp=%s",
            request_count,
            driver_count,
            timestamp.isoformat(),
        )
        time.sleep(EMIT_INTERVAL_SECONDS)

    producer.close(timeout=10)
    LOGGER.info("Producer stopped cleanly.")


if __name__ == "__main__":
    main()
