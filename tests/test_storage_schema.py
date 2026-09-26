from storage.postgres import SCHEMA_SQL


def test_serving_schema_covers_metrics_reports_and_alerts():
    assert "CREATE TABLE IF NOT EXISTS fleet_utilization_metrics" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS daily_vehicle_profitability" in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS pipeline_alerts" in SCHEMA_SQL
