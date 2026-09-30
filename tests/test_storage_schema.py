from datetime import datetime, timezone

from storage.postgres import SCHEMA_SQL, _utc_timestamp


def test_serving_schema_covers_metrics_reports_and_alerts():
    assert "CREATE TABLE IF NOT EXISTS fleet_utilization_metrics" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS daily_vehicle_profitability" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS pipeline_alerts" in SCHEMA_SQL


def test_spark_naive_timestamp_is_normalized_to_utc():
    local_timestamp = datetime(2026, 9, 28, 11, 16, 35)

    normalized = _utc_timestamp(local_timestamp)

    assert normalized.tzinfo == timezone.utc
    assert normalized == local_timestamp.astimezone(timezone.utc)
