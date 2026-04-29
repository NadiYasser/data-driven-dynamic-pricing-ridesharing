"""Streamlit dashboard for H3 zones, live events, and price multipliers."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import h3
import pydeck as pdk
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))


RIDE_REQUESTS_TOPIC = os.getenv("RIDE_REQUESTS_TOPIC", "ride_requests")
DRIVER_UPDATES_TOPIC = os.getenv("DRIVER_UPDATES_TOPIC", "driver_updates")
PRICING_TOPIC = os.getenv("PRICING_TOPIC", "zone_pricing")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_API_VERSION = tuple(int(part) for part in os.getenv("KAFKA_API_VERSION", "3.9").split("."))
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27017/dynamic_pricing?authSource=admin",
)
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "dynamic_pricing")
MONGO_ZONES_COLLECTION = os.getenv("MONGO_ZONES_COLLECTION", "zones")
DEFAULT_EVENT_WINDOW_SECONDS = int(os.getenv("DASHBOARD_EVENT_WINDOW_SECONDS", "60"))
INITIAL_TAIL_MESSAGES = int(os.getenv("DASHBOARD_INITIAL_TAIL_MESSAGES", "5000"))


def parse_timestamp(value: str | None) -> datetime:
    """Parse an ISO timestamp, falling back to now if the event is malformed."""

    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def cell_boundary(cell: str) -> list[list[float]]:
    """Return an H3 boundary as [lon, lat] points for PyDeck."""

    if hasattr(h3, "cell_to_boundary"):
        boundary = h3.cell_to_boundary(cell)
    else:
        boundary = h3.h3_to_geo_boundary(cell)
    return [[float(lon), float(lat)] for lat, lon in boundary]


@st.cache_data(ttl=30)
def load_zones() -> list[dict[str, Any]]:
    """Load zone metadata and polygons from MongoDB."""

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
        collection = client[MONGO_DATABASE][MONGO_ZONES_COLLECTION]
        zones = list(
            collection.find(
                {},
                {
                    "_id": 0,
                    "zone_id": 1,
                    "h3_zone": 1,
                    "type": 1,
                    "center_lat": 1,
                    "center_lon": 1,
                },
            ).sort("h3_zone", 1)
        )
    except ServerSelectionTimeoutError:
        return []
    finally:
        client.close()

    for zone in zones:
        zone["polygon"] = cell_boundary(zone["h3_zone"])
        zone["label_position"] = [float(zone["center_lon"]), float(zone["center_lat"])]
    return zones


def dashboard_topics() -> list[str]:
    """Return the Kafka topics rendered by the dashboard."""

    return [RIDE_REQUESTS_TOPIC, DRIVER_UPDATES_TOPIC, PRICING_TOPIC]


def create_consumer() -> KafkaConsumer | None:
    """Create a manually assigned Kafka consumer for dashboard topics."""

    topics = dashboard_topics()
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            api_version=KAFKA_API_VERSION,
            enable_auto_commit=False,
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
        topic_partitions = []
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic) or set()
            topic_partitions.extend(TopicPartition(topic, partition) for partition in partitions)

        if not topic_partitions:
            consumer.close()
            st.session_state.kafka_status = "Connected to Kafka, but dashboard topics have no partitions yet."
            return None

        consumer.assign(topic_partitions)
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        for topic_partition in topic_partitions:
            start_offset = max(
                beginning_offsets[topic_partition],
                end_offsets[topic_partition] - INITIAL_TAIL_MESSAGES,
            )
            consumer.seek(topic_partition, start_offset)

        st.session_state.kafka_status = (
            "Connected to Kafka topics: "
            + ", ".join(sorted({topic_partition.topic for topic_partition in topic_partitions}))
        )
        return consumer
    except NoBrokersAvailable:
        st.session_state.kafka_status = "Kafka is not available from the dashboard process."
        return None
    except Exception as exc:
        st.session_state.kafka_status = f"Kafka dashboard consumer error: {type(exc).__name__}: {exc}"
        return None


def ensure_state() -> None:
    """Initialize Streamlit session state for live data."""

    if "events" not in st.session_state:
        st.session_state.events = []
    if "pricing_by_zone" not in st.session_state:
        st.session_state.pricing_by_zone = {}
    if "consumer" not in st.session_state:
        st.session_state.consumer = create_consumer()
    if "kafka_status" not in st.session_state:
        st.session_state.kafka_status = "Kafka consumer has not been initialized."
    if "kafka_records_read" not in st.session_state:
        st.session_state.kafka_records_read = 0


def consume_messages(max_records: int = 2000) -> None:
    """Poll Kafka and update session-state event/pricing buffers."""

    consumer = st.session_state.consumer
    if consumer is None:
        st.session_state.consumer = create_consumer()
        return

    records = consumer.poll(timeout_ms=200, max_records=max_records)
    for topic_partition_records in records.values():
        for record in topic_partition_records:
            event = record.value
            st.session_state.kafka_records_read += 1
            if record.topic == PRICING_TOPIC:
                st.session_state.pricing_by_zone[event["h3_zone"]] = event
            elif record.topic in {RIDE_REQUESTS_TOPIC, DRIVER_UPDATES_TOPIC}:
                event["topic"] = record.topic
                event["received_at"] = datetime.now(timezone.utc)
                event["event_time"] = parse_timestamp(event.get("timestamp"))
                st.session_state.events.append(event)


def trim_events(window_seconds: int) -> None:
    """Keep only recent raw point events in memory."""

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    st.session_state.events = [
        event
        for event in st.session_state.events
        if event.get("event_time", event.get("received_at", cutoff)) >= cutoff
    ]


def color_for_multiplier(multiplier: float) -> list[int]:
    """Return a green-to-red fill color for a multiplier value."""

    normalized = min(1.0, max(0.0, (multiplier - 0.9) / 0.6))
    red = int(70 + normalized * 185)
    green = int(180 - normalized * 130)
    return [red, green, 80, 150]


def color_for_metric(value: float, max_value: float) -> list[int]:
    """Return a blue intensity fill color for count/pressure views."""

    normalized = 0.0 if max_value <= 0 else min(1.0, max(0.0, value / max_value))
    return [50, int(120 + normalized * 90), int(150 + normalized * 90), 145]


def build_zone_rows(zones: list[dict[str, Any]], color_by: str) -> list[dict[str, Any]]:
    """Merge zone geometry with latest pricing results for map rendering."""

    rows = []
    pricing_by_zone = st.session_state.pricing_by_zone
    max_demand = max((pricing.get("demand_count", 0) for pricing in pricing_by_zone.values()), default=0)
    max_supply = max((pricing.get("available_driver_count", 0) for pricing in pricing_by_zone.values()), default=0)
    max_pressure = max((abs(pricing.get("pressure") or 0.0) for pricing in pricing_by_zone.values()), default=0.0)

    for zone in zones:
        pricing = pricing_by_zone.get(zone["h3_zone"], {})
        multiplier = float(pricing.get("price_multiplier", 1.0))
        demand = int(pricing.get("demand_count", 0))
        supply = int(pricing.get("available_driver_count", 0))
        pressure = float(pricing.get("pressure") or 0.0)

        if color_by == "Demand count":
            fill_color = color_for_metric(demand, max_demand)
        elif color_by == "Supply count":
            fill_color = color_for_metric(supply, max_supply)
        elif color_by == "Pressure":
            fill_color = color_for_metric(abs(pressure), max_pressure)
        else:
            fill_color = color_for_multiplier(multiplier)

        rows.append(
            {
                **zone,
                "price_multiplier": multiplier,
                "demand_count": demand,
                "available_driver_count": supply,
                "pressure": pressure,
                "fill_color": fill_color,
                "line_color": [35, 35, 35, 190],
                "label": f"{multiplier:.2f}x\nD {demand} / S {supply}",
            }
        )
    return rows


def build_event_rows(topic: str) -> list[dict[str, Any]]:
    """Return recent point events for one Kafka topic."""

    rows = []
    for event in st.session_state.events:
        if event.get("topic") != topic:
            continue
        if "lat" not in event or "lon" not in event:
            continue
        rows.append(
            {
                "lat": float(event["lat"]),
                "lon": float(event["lon"]),
                "h3_zone": event.get("h3_zone"),
                "status": event.get("status", "request"),
            }
        )
    return rows


def event_counts() -> tuple[int, int]:
    """Return current ride request and driver point counts in memory."""

    request_count = 0
    driver_count = 0
    for event in st.session_state.events:
        if event.get("topic") == RIDE_REQUESTS_TOPIC:
            request_count += 1
        elif event.get("topic") == DRIVER_UPDATES_TOPIC:
            driver_count += 1
    return request_count, driver_count


def render_map(
    zone_rows: list[dict[str, Any]],
    show_requests: bool,
    show_drivers: bool,
    show_labels: bool,
) -> None:
    """Render all configured map layers."""

    if not zone_rows:
        st.warning("No H3 zones found. Run `python scripts/seed_zones.py` first.")
        return

    center_lat = sum(float(row["center_lat"]) for row in zone_rows) / len(zone_rows)
    center_lon = sum(float(row["center_lon"]) for row in zone_rows) / len(zone_rows)

    layers = [
        pdk.Layer(
            "PolygonLayer",
            data=zone_rows,
            get_polygon="polygon",
            get_fill_color="fill_color",
            get_line_color="line_color",
            line_width_min_pixels=1,
            pickable=True,
            stroked=True,
            filled=True,
            opacity=0.45,
        )
    ]

    if show_requests:
        request_rows = build_event_rows(RIDE_REQUESTS_TOPIC)
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=request_rows,
                get_position="[lon, lat]",
                get_fill_color=[20, 70, 255, 245],
                get_line_color=[255, 255, 255, 230],
                get_radius=85,
                line_width_min_pixels=1,
                pickable=True,
                stroked=True,
            )
        )

    if show_drivers:
        driver_rows = build_event_rows(DRIVER_UPDATES_TOPIC)
        for row in driver_rows:
            row["color"] = [35, 170, 85, 180] if row["status"] == "available" else [120, 120, 120, 150]
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=driver_rows,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_line_color=[255, 255, 255, 230],
                get_radius=70,
                line_width_min_pixels=1,
                pickable=True,
                stroked=True,
            )
        )

    if show_labels:
        layers.append(
            pdk.Layer(
                "TextLayer",
                data=zone_rows,
                get_position="label_position",
                get_text="label",
                get_color=[20, 20, 20, 255],
                get_size=13,
                get_alignment_baseline="'center'",
                get_text_anchor="'middle'",
                pickable=False,
            )
        )

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=11, pitch=0),
        tooltip={
            "html": (
                "<b>{h3_zone}</b><br/>"
                "Multiplier: {price_multiplier}<br/>"
                "Demand: {demand_count}<br/>"
                "Available drivers: {available_driver_count}<br/>"
                "Pressure: {pressure}"
            )
        },
        map_style=pdk.map_styles.CARTO_LIGHT,
    )
    st.pydeck_chart(deck, use_container_width=True)


def main() -> None:
    """Run the Streamlit app."""

    st.set_page_config(page_title="Dynamic Pricing H3 Map", layout="wide")
    st.title("Dynamic Pricing H3 Map")

    ensure_state()
    zones = load_zones()

    with st.sidebar:
        event_window_seconds = st.slider("Raw event point window", 15, 300, DEFAULT_EVENT_WINDOW_SECONDS, step=15)
        refresh_interval = st.slider("Auto-refresh interval", 1, 15, 3)
        show_requests = st.checkbox("Show ride request points", value=True)
        show_drivers = st.checkbox("Show driver update points", value=True)
        show_labels = st.checkbox("Show multiplier labels", value=True)
        color_by = st.selectbox(
            "Zone coloring",
            ["Price multiplier", "Demand count", "Supply count", "Pressure"],
        )
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        if st.button("Refresh now"):
            st.rerun()

    consume_messages()
    trim_events(event_window_seconds)
    zone_rows = build_zone_rows(zones, color_by)

    total_demand = sum(row["demand_count"] for row in zone_rows)
    total_supply = sum(row["available_driver_count"] for row in zone_rows)
    avg_multiplier = sum(row["price_multiplier"] for row in zone_rows) / len(zone_rows) if zone_rows else 1.0
    request_point_count, driver_point_count = event_counts()

    metric_cols = st.columns(4)
    metric_cols[0].metric("Tracked zones", len(zone_rows))
    metric_cols[1].metric("15s demand", total_demand)
    metric_cols[2].metric("15s available drivers", total_supply)
    metric_cols[3].metric("Average multiplier", f"{avg_multiplier:.2f}x")
    st.caption(
        f"Visible raw points in {event_window_seconds}s window: "
        f"{request_point_count} requests, {driver_point_count} driver updates."
    )

    if st.session_state.consumer is None:
        st.warning(st.session_state.kafka_status)
    else:
        st.caption(f"{st.session_state.kafka_status}. Records read: {st.session_state.kafka_records_read}")

    render_map(zone_rows, show_requests, show_drivers, show_labels)

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
