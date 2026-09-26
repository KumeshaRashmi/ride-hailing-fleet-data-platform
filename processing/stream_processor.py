"""Spark Structured Streaming processing for Kafka fleet telemetry.

The job has two sinks:
* cleaned raw telemetry in Parquet for the Lambda batch path; and
* five-minute, per-zone utilization metrics in Parquet for serving/dashboard use.

The metrics sink appends each micro-batch update with a batch id.  A serving
layer can select the newest record for a window/zone key, or upsert it into
PostgreSQL without losing the audit trail.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from processing.common import VALID_STATUSES


def telemetry_schema() -> Any:
    """Return the Kafka JSON schema without importing Spark during unit tests."""
    from pyspark.sql.types import (  # pylint: disable=import-outside-toplevel
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("speed", IntegerType(), False),
            StructField("status", StringType(), False),
            StructField("fare", DoubleType(), False),
            StructField("timestamp", StringType(), False),
        ]
    )


def add_zone_column(dataframe: Any) -> Any:
    """Apply the same four-zone grid as ``zone_from_coordinates`` in Spark."""
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    return dataframe.withColumn(
        "zone",
        F.when(
            (F.col("lat") < F.lit(6.85))
            | (F.col("lat") > F.lit(7.05))
            | (F.col("lon") < F.lit(79.80))
            | (F.col("lon") > F.lit(79.95)),
            F.lit("outside_service_area"),
        )
        .when(
            F.col("lat") >= F.lit(6.95),
            F.when(F.col("lon") >= F.lit(79.875), F.lit("colombo_north_east")).otherwise(
                F.lit("colombo_north_west")
            ),
        )
        .otherwise(
            F.when(F.col("lon") >= F.lit(79.875), F.lit("colombo_south_east")).otherwise(
                F.lit("colombo_south_west")
            )
        ),
    )


def clean_and_enrich(kafka_dataframe: Any) -> Any:
    """Parse, validate, timestamp and enrich the incoming Kafka value JSON."""
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    parsed = (
        kafka_dataframe.select(F.from_json(F.col("value").cast("string"), telemetry_schema()).alias("event"))
        .select("event.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    valid_events = parsed.where(
        F.col("trip_id").isNotNull()
        & F.col("driver_id").isNotNull()
        & F.col("vehicle_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("status").isin(*VALID_STATUSES)
        & F.col("speed").between(0, 180)
        & (F.col("fare") >= 0)
    )
    return add_zone_column(valid_events)


def create_utilization_metrics(clean_events: Any) -> Any:
    """Build the live five-minute fleet-utilization aggregation."""
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    metrics = (
        clean_events.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "5 minutes"), F.col("zone"))
        .agg(
            F.count("*").alias("telemetry_event_count"),
            F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("idle_event_count"),
            F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("on_trip_event_count"),
            F.sum(F.when(F.col("status") != "idle", 1).otherwise(0)).alias("active_event_count"),
            F.sum("fare").alias("observed_earnings"),
            F.avg("speed").alias("average_speed"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn(
            "idle_ratio",
            F.round(F.col("idle_event_count") / F.col("telemetry_event_count"), 4),
        )
    )
    return metrics


def write_metrics_batch(batch_dataframe: Any, batch_id: int, metrics_path: str) -> None:
    """Append one update-mode micro-batch plus traceable processing metadata."""
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    (
        batch_dataframe.withColumn("spark_batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
        .write.mode("append")
        .parquet(metrics_path)
    )


def start_queries(
    spark: Any,
    bootstrap_servers: str,
    topic: str,
    raw_events_path: str,
    metrics_path: str,
    checkpoint_root: str,
    trigger_seconds: int,
) -> tuple[Any, Any]:
    """Start raw and aggregate query branches and return their query handles."""
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    clean_events = clean_and_enrich(kafka_stream)
    metrics = create_utilization_metrics(clean_events)

    raw_query = (
        clean_events.writeStream.format("parquet")
        .queryName("fleet_clean_telemetry_to_parquet")
        .outputMode("append")
        .option("checkpointLocation", str(Path(checkpoint_root) / "raw_events"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start(raw_events_path)
    )
    metrics_query = (
        metrics.writeStream.queryName("fleet_utilization_metrics_to_parquet")
        .outputMode("update")
        .option("checkpointLocation", str(Path(checkpoint_root) / "utilization_metrics"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .foreachBatch(lambda dataframe, batch_id: write_metrics_batch(dataframe, batch_id, metrics_path))
        .start()
    )
    return raw_query, metrics_query


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process fleet telemetry from Kafka with Spark.")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="fleet-telemetry")
    parser.add_argument("--raw-events-path", default="data_lake/clean_telemetry")
    parser.add_argument("--metrics-path", default="data_lake/realtime_utilization_metrics")
    parser.add_argument("--checkpoint-root", default="data_lake/checkpoints")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.trigger_seconds <= 0:
        raise ValueError("--trigger-seconds must be positive")

    from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel

    spark = SparkSession.builder.appName("fleet-telemetry-stream-processor").getOrCreate()
    raw_query, metrics_query = start_queries(
        spark,
        args.kafka_bootstrap_servers,
        args.topic,
        args.raw_events_path,
        args.metrics_path,
        args.checkpoint_root,
        args.trigger_seconds,
    )
    print(
        json.dumps(
            {
                "event": "spark_streaming_started",
                "raw_query_id": raw_query.id,
                "metrics_query_id": metrics_query.id,
                "topic": args.topic,
            }
        )
    )
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in (raw_query, metrics_query):
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
