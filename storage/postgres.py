"""PostgreSQL schema, upsert and query functions for the serving layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_SQL = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")


def _connect(database_url: str) -> Any:
    """Create a short-lived PostgreSQL connection only when the adapter is used."""
    try:
        import psycopg  # pylint: disable=import-outside-toplevel
    except ImportError as error:  # pragma: no cover - explained to CLI users
        raise RuntimeError(
            "PostgreSQL support requires psycopg. Install requirements.txt before "
            "using --postgres-dsn or FLEET_DATABASE_URL."
        ) from error
    return psycopg.connect(database_url, connect_timeout=5)


def initialize_schema(database_url: str) -> None:
    """Create serving and alert tables; safe to call at every service startup."""
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)


def _value(row: Mapping[str, Any] | Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def upsert_utilization_metrics(rows: Iterable[Mapping[str, Any] | Any], database_url: str) -> int:
    """Upsert the newest aggregation state for each stream window and zone."""
    payload = [
        (
            _value(row, "window_start"),
            _value(row, "window_end"),
            _value(row, "zone"),
            int(_value(row, "telemetry_event_count", 0)),
            int(_value(row, "idle_event_count", 0)),
            int(_value(row, "on_trip_event_count", 0)),
            int(_value(row, "active_event_count", 0)),
            float(_value(row, "observed_earnings", 0.0) or 0.0),
            float(_value(row, "average_speed", 0.0) or 0.0),
            float(_value(row, "idle_ratio", 0.0) or 0.0),
            int(_value(row, "spark_batch_id", 0)),
            _value(row, "processed_at"),
        )
        for row in rows
    ]
    if not payload:
        return 0

    statement = """
        INSERT INTO fleet_utilization_metrics (
            window_start, window_end, zone, telemetry_event_count, idle_event_count,
            on_trip_event_count, active_event_count, observed_earnings, average_speed,
            idle_ratio, spark_batch_id, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, zone) DO UPDATE SET
            telemetry_event_count = EXCLUDED.telemetry_event_count,
            idle_event_count = EXCLUDED.idle_event_count,
            on_trip_event_count = EXCLUDED.on_trip_event_count,
            active_event_count = EXCLUDED.active_event_count,
            observed_earnings = EXCLUDED.observed_earnings,
            average_speed = EXCLUDED.average_speed,
            idle_ratio = EXCLUDED.idle_ratio,
            spark_batch_id = EXCLUDED.spark_batch_id,
            processed_at = EXCLUDED.processed_at
        WHERE EXCLUDED.processed_at >= fleet_utilization_metrics.processed_at
    """
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(statement, payload)
    return len(payload)


def upsert_daily_profitability(rows: Iterable[Mapping[str, Any] | Any], database_url: str) -> int:
    """Upsert the batch reconciliation by logical date and vehicle."""
    columns = (
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
    payload = [tuple(_value(row, column) for column in columns) for row in rows]
    if not payload:
        return 0

    statement = """
        INSERT INTO daily_vehicle_profitability (
            report_date, vehicle_id, completed_trip_count, trip_revenue, fuel_cost,
            maintenance_cost, total_expense, distance_covered, service_flag, profit,
            profit_margin_pct, profitability_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
            completed_trip_count = EXCLUDED.completed_trip_count,
            trip_revenue = EXCLUDED.trip_revenue,
            fuel_cost = EXCLUDED.fuel_cost,
            maintenance_cost = EXCLUDED.maintenance_cost,
            total_expense = EXCLUDED.total_expense,
            distance_covered = EXCLUDED.distance_covered,
            service_flag = EXCLUDED.service_flag,
            profit = EXCLUDED.profit,
            profit_margin_pct = EXCLUDED.profit_margin_pct,
            profitability_status = EXCLUDED.profitability_status,
            updated_at = NOW()
    """
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(statement, payload)
    return len(payload)


def fetch_health_snapshot(database_url: str) -> dict[str, Any]:
    """Return data freshness and serving-table counts for the health endpoint."""
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(processed_at), COUNT(*)
                FROM fleet_utilization_metrics
                """
            )
            last_processed_at, metric_rows = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) FROM daily_vehicle_profitability")
            profitability_rows = cursor.fetchone()[0]
    return {
        "last_processed_at": last_processed_at,
        "metric_rows": metric_rows,
        "profitability_rows": profitability_rows,
    }


def fetch_latest_utilization(database_url: str, zone: str | None = None) -> list[dict[str, Any]]:
    """Return all zones in the newest completed metrics window."""
    statement = """
        WITH latest_window AS (
            SELECT MAX(window_end) AS window_end FROM fleet_utilization_metrics
        )
        SELECT window_start, window_end, zone, telemetry_event_count,
               idle_event_count, on_trip_event_count, active_event_count,
               observed_earnings, average_speed, idle_ratio, processed_at
        FROM fleet_utilization_metrics
        WHERE window_end = (SELECT window_end FROM latest_window)
          AND (%s::text IS NULL OR zone = %s)
        ORDER BY zone
    """
    return _fetch_many(database_url, statement, (zone, zone))


def fetch_profitability_report(database_url: str, report_date: str) -> list[dict[str, Any]]:
    """Return the selected daily report, lowest profit first."""
    return _fetch_many(
        database_url,
        """
        SELECT report_date, vehicle_id, completed_trip_count, trip_revenue, fuel_cost,
               maintenance_cost, total_expense, distance_covered, service_flag, profit,
               profit_margin_pct, profitability_status, updated_at
        FROM daily_vehicle_profitability
        WHERE report_date = %s
        ORDER BY profit, vehicle_id
        """,
        (report_date,),
    )


def record_alert(
    database_url: str,
    severity: str,
    rule_name: str,
    message: str,
    context: Mapping[str, Any],
) -> None:
    """Persist an alert transition for the final report and demo evidence."""
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_alerts (severity, rule_name, message, context)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (severity, rule_name, message, json.dumps(context, default=str)),
            )


def _fetch_many(database_url: str, statement: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
    try:
        from psycopg.rows import dict_row  # pylint: disable=import-outside-toplevel
    except ImportError as error:  # pragma: no cover - mirrors _connect message
        raise RuntimeError("PostgreSQL support requires psycopg.") from error
    with _connect(database_url) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(statement, parameters)
            return list(cursor.fetchall())
