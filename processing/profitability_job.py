"""Spark batch reconciliation of daily expenses against processed trip revenue."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from observability.logging import get_logger, log_event


LOGGER = get_logger("fleet.profitability_job")


def build_profitability_report(expenses: Any, raw_events: Any, report_date: str) -> Any:
    """Join one logical day's cost feed to deduplicated trip revenue per vehicle."""
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    daily_events = raw_events.where(F.to_date("event_time") == F.lit(report_date))
    trip_revenue = (
        daily_events.where((F.col("status") == "on_trip") & (F.col("fare") > 0))
        # A single trip can emit several telemetry events.  Taking its maximum
        # observed fare prevents that trip being counted multiple times.
        .groupBy("vehicle_id", "trip_id")
        .agg(F.max("fare").alias("trip_fare"))
        .groupBy("vehicle_id")
        .agg(
            F.count("trip_id").alias("completed_trip_count"),
            F.round(F.sum("trip_fare"), 2).alias("trip_revenue"),
        )
    )

    report = (
        expenses.join(trip_revenue, on="vehicle_id", how="left")
        .fillna({"completed_trip_count": 0, "trip_revenue": 0.0})
        .withColumn("total_expense", F.round(F.col("fuel_cost") + F.col("maintenance_cost"), 2))
        .withColumn("profit", F.round(F.col("trip_revenue") - F.col("total_expense"), 2))
        .withColumn(
            "profit_margin_pct",
            F.when(
                F.col("trip_revenue") > 0,
                F.round((F.col("profit") / F.col("trip_revenue")) * 100, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "profitability_status",
            F.when(F.col("profit") < 0, F.lit("unprofitable")).otherwise(F.lit("profitable")),
        )
        .withColumn("report_date", F.lit(report_date).cast("date"))
        .select(
            "report_date",
            "vehicle_id",
            "completed_trip_count",
            "trip_revenue",
            "fuel_cost",
            "maintenance_cost",
            "total_expense",
            "distance_covered",
            "service_flag",
            "profit",
            "profit_margin_pct",
            "profitability_status",
        )
        .orderBy("profit", "vehicle_id")
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the daily fleet profitability report.")
    parser.add_argument("--expenses", required=True, help="Path to one daily expense CSV.")
    parser.add_argument("--raw-events-path", default="data_lake/clean_telemetry")
    parser.add_argument("--output-root", default="data_lake/profitability_reports")
    parser.add_argument("--csv-output-root", default="reports/profitability")
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("FLEET_DATABASE_URL"),
        help="Optional PostgreSQL DSN to upsert the daily serving report.",
    )
    parser.add_argument(
        "--report-date",
        type=date.fromisoformat,
        required=True,
        help="Logical date to reconcile, in YYYY-MM-DD form.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    report_date = args.report_date.isoformat()
    if args.postgres_dsn:
        from storage.postgres import initialize_schema  # pylint: disable=import-outside-toplevel

        initialize_schema(args.postgres_dsn)
    spark = SparkSession.builder.appName("fleet-daily-profitability-reconciliation").getOrCreate()
    try:
        expenses = (
            spark.read.option("header", "true")
            .csv(args.expenses)
            .withColumn("fuel_cost", F.col("fuel_cost").cast("double"))
            .withColumn("maintenance_cost", F.col("maintenance_cost").cast("double"))
            .withColumn("distance_covered", F.col("distance_covered").cast("double"))
            .where(F.col("vehicle_id").isNotNull())
        )
        raw_events = spark.read.parquet(args.raw_events_path)
        report = build_profitability_report(expenses, raw_events, report_date)

        parquet_path = str(Path(args.output_root) / f"report_date={report_date}")
        csv_path = str(Path(args.csv_output_root) / f"report_date={report_date}")
        report.write.mode("overwrite").parquet(parquet_path)
        report.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
        report_rows = list(report.toLocalIterator())
        if args.postgres_dsn:
            from storage.postgres import upsert_daily_profitability  # pylint: disable=import-outside-toplevel

            postgres_rows_upserted = upsert_daily_profitability(report_rows, args.postgres_dsn)
        else:
            postgres_rows_upserted = 0
        log_event(
            LOGGER,
            "daily_profitability_report_created",
            report_date=report_date,
            parquet_path=parquet_path,
            csv_path=csv_path,
            vehicle_count=len(report_rows),
            postgres_rows_upserted=postgres_rows_upserted,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
