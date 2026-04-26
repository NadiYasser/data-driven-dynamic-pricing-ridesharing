"""Generate H3 zones from config/zones.json and store them in MongoDB."""

from __future__ import annotations

import json
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h3
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pymongo.errors import ServerSelectionTimeoutError


CONFIG_PATH = Path(os.getenv("ZONES_CONFIG_PATH", "config/zones.json"))
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27017/dynamic_pricing?authSource=admin",
)
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "dynamic_pricing")
MONGO_COLLECTION = os.getenv("MONGO_ZONES_COLLECTION", "zones")
EARTH_RADIUS_KM = 6371.0088


def latlng_to_cell(lat: float, lon: float, resolution: int) -> str:
    """Return an H3 cell ID with compatibility for h3-py 3.x and 4.x."""

    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, resolution)
    return h3.geo_to_h3(lat, lon, resolution)


def cell_to_latlng(cell: str) -> tuple[float, float]:
    """Return the center point of an H3 cell."""

    if hasattr(h3, "cell_to_latlng"):
        lat, lon = h3.cell_to_latlng(cell)
    else:
        lat, lon = h3.h3_to_geo(cell)
    return float(lat), float(lon)


def grid_disk(cell: str, radius: int) -> set[str]:
    """Return all H3 cells within k rings from a center cell."""

    if hasattr(h3, "grid_disk"):
        return set(h3.grid_disk(cell, radius))
    return set(h3.k_ring(cell, radius))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two coordinates."""

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def average_edge_length_km(resolution: int) -> float:
    """Return average H3 edge length for estimating the search radius."""

    if hasattr(h3, "average_hexagon_edge_length"):
        return float(h3.average_hexagon_edge_length(resolution, unit="km"))
    return float(h3.edge_length(resolution, unit="km"))


def load_config() -> dict[str, Any]:
    """Load and validate the zone generation config."""

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = json.load(file)

    required_top_level = {"h3_resolution", "random_seed", "area", "zone_types"}
    missing = required_top_level - set(config)
    if missing:
        raise ValueError(f"Missing required config keys: {sorted(missing)}")

    probability_sum = sum(zone_type["probability"] for zone_type in config["zone_types"])
    if not math.isclose(probability_sum, 1.0, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"Zone type probabilities must sum to 1.0, got {probability_sum}")

    return config


def generate_h3_cells(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate H3 cells inside the configured circular area."""

    resolution = int(config["h3_resolution"])
    area = config["area"]
    center_lat = float(area["center_lat"])
    center_lon = float(area["center_lon"])
    radius_km = float(area["radius_km"])

    center_cell = latlng_to_cell(center_lat, center_lon, resolution)
    edge_km = average_edge_length_km(resolution)
    ring_radius = max(1, math.ceil(radius_km / edge_km) + 2)

    cells = []
    for cell in grid_disk(center_cell, ring_radius):
        lat, lon = cell_to_latlng(cell)
        distance_from_center_km = haversine_km(center_lat, center_lon, lat, lon)
        if distance_from_center_km <= radius_km:
            cells.append(
                {
                    "h3_zone": cell,
                    "center_lat": round(lat, 7),
                    "center_lon": round(lon, 7),
                    "distance_from_center_km": round(distance_from_center_km, 4),
                }
            )

    return sorted(cells, key=lambda item: item["h3_zone"])


def assign_zone_types(
    cells: list[dict[str, Any]],
    zone_types: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach a zone type profile to every generated H3 cell."""

    choices = zone_types
    weights = [zone_type["probability"] for zone_type in choices]
    now = datetime.now(timezone.utc)

    assigned = []
    for index, cell in enumerate(cells, start=1):
        selected = random.choices(choices, weights=weights, k=1)[0]
        assigned.append(
            {
                "zone_id": f"zone_{index:04d}",
                **cell,
                "zone_type_id": selected["zone_type_id"],
                "type": selected["type"],
                "base_demand": selected["base_demand"],
                "base_supply": selected["base_supply"],
                "price_sensitivity": selected["price_sensitivity"],
                "avg_distance": selected["avg_distance"],
                "selection_probability": selected["probability"],
                "created_at": now,
                "updated_at": now,
            }
        )

    return assigned


def normalize_resolution(zones: list[dict[str, Any]], resolution: int) -> list[dict[str, Any]]:
    """Populate resolution after assignment without duplicating config plumbing."""

    for zone in zones:
        zone["h3_resolution"] = resolution
    return zones


def upsert_zones(zones: list[dict[str, Any]]) -> None:
    """Create indexes and upsert generated zone documents into MongoDB."""

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    for attempt in range(1, 31):
        try:
            client.admin.command("ping")
            break
        except ServerSelectionTimeoutError:
            print(f"MongoDB is not ready yet, retrying ({attempt}/30)...", flush=True)
            time.sleep(2)
    else:
        raise RuntimeError(f"Could not connect to MongoDB with URI {MONGO_URI}")

    collection = client[MONGO_DATABASE][MONGO_COLLECTION]

    collection.create_index([("h3_zone", ASCENDING)], unique=True)
    collection.create_index([("type", ASCENDING)])
    collection.create_index([("zone_type_id", ASCENDING)])

    operations = [
        ReplaceOne({"h3_zone": zone["h3_zone"]}, zone, upsert=True)
        for zone in zones
    ]
    if operations:
        result = collection.bulk_write(operations, ordered=False)
        print(
            "Seeded zones:",
            f"matched={result.matched_count}",
            f"modified={result.modified_count}",
            f"upserted={len(result.upserted_ids)}",
            f"total={collection.count_documents({})}",
            flush=True,
        )
    else:
        print("No zones generated from the current config.", flush=True)

    client.close()


def main() -> None:
    """Generate zone documents and store them in MongoDB."""

    config = load_config()
    random.seed(int(config["random_seed"]))

    cells = generate_h3_cells(config)
    zones = assign_zone_types(cells, config["zone_types"])
    zones = normalize_resolution(zones, int(config["h3_resolution"]))

    print(
        f"Generated {len(zones)} H3 zones at resolution {config['h3_resolution']} "
        f"from {CONFIG_PATH}",
        flush=True,
    )
    upsert_zones(zones)


if __name__ == "__main__":
    main()
