"""Spark Structured Streaming job for zone-level dynamic pricing."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RIDE_REQUESTS_TOPIC = os.getenv("RIDE_REQUESTS_TOPIC", "ride_requests")
DRIVER_UPDATES_TOPIC = os.getenv("DRIVER_UPDATES_TOPIC", "driver_updates")
PRICING_TOPIC = os.getenv("PRICING_TOPIC", "zone_pricing")
PRICING_WINDOW_SECONDS = int(os.getenv("PRICING_WINDOW_SECONDS", "15"))
PRICING_SLIDE_SECONDS = int(os.getenv("PRICING_SLIDE_SECONDS", "5"))
PRICING_MIN_MULTIPLIER = float(os.getenv("PRICING_MIN_MULTIPLIER", "0.9"))
PRICING_MAX_MULTIPLIER = float(os.getenv("PRICING_MAX_MULTIPLIER", "1.5"))
PRICING_PRESSURE_WEIGHT = float(os.getenv("PRICING_PRESSURE_WEIGHT", "0.4"))
CHECKPOINT_LOCATION = os.getenv("PRICING_CHECKPOINT_LOCATION", "/tmp/dynamic_pricing/pricing_stream")


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("h3_zone", StringType()),
        StructField("status", StringType()),
    ]
)


def build_stream(spark: SparkSession):
    """Build the streaming DataFrame that writes price updates."""

    topics = ",".join([RIDE_REQUESTS_TOPIC, DRIVER_UPDATES_TOPIC])
    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topics)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_events = (
        raw_events.select(
            col("topic"),
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("topic", "event.*")
        .where(col("h3_zone").isNotNull())
        .withColumn("event_time", coalesce(to_timestamp("timestamp"), current_timestamp()))
        .withColumn("demand_increment", when(col("topic") == RIDE_REQUESTS_TOPIC, lit(1)).otherwise(lit(0)))
        .withColumn(
            "available_driver_increment",
            when((col("topic") == DRIVER_UPDATES_TOPIC) & (col("status") == "available"), lit(1)).otherwise(lit(0)),
        )
    )

    aggregated = (
        parsed_events.withWatermark("event_time", f"{PRICING_WINDOW_SECONDS} seconds")
        .groupBy(
            window(
                col("event_time"),
                f"{PRICING_WINDOW_SECONDS} seconds",
                f"{PRICING_SLIDE_SECONDS} seconds",
            ),
            col("h3_zone"),
        )
        .agg(
            spark_sum("demand_increment").cast("int").alias("demand_count"),
            spark_sum("available_driver_increment").cast("int").alias("available_driver_count"),
        )
    )

    priced = (
        aggregated.withColumn(
            "pressure",
            when((col("demand_count") <= 0) & (col("available_driver_count") <= 0), lit(0.0))
            .when(col("available_driver_count") <= 0, lit(None).cast("double"))
            .otherwise((col("demand_count").cast("double") / col("available_driver_count").cast("double")) - lit(1.0)),
        )
        .withColumn(
            "price_multiplier",
            when((col("demand_count") <= 0) & (col("available_driver_count") <= 0), lit(1.0))
            .when(col("available_driver_count") <= 0, lit(PRICING_MAX_MULTIPLIER))
            .otherwise(
                expr(
                    "least("
                    f"{PRICING_MAX_MULTIPLIER}, "
                    "greatest("
                    f"{PRICING_MIN_MULTIPLIER}, "
                    f"1.0 + {PRICING_PRESSURE_WEIGHT} * tanh(pressure)"
                    "))"
                )
            ),
        )
        .select(
            col("h3_zone").cast("string").alias("key"),
            to_json(
                struct(
                    col("h3_zone"),
                    col("window.start").cast("string").alias("window_start"),
                    col("window.end").cast("string").alias("window_end"),
                    col("demand_count"),
                    col("available_driver_count"),
                    col("pressure"),
                    col("price_multiplier"),
                    current_timestamp().cast("string").alias("calculated_at"),
                )
            ).alias("value"),
        )
    )

    return priced


def main() -> None:
    """Run the pricing stream until interrupted."""

    spark = (
        SparkSession.builder.appName("dynamic-pricing-zone-pricing")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    query = (
        build_stream(spark)
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", PRICING_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("update")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
