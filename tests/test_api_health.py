from datetime import datetime, timedelta, timezone

from api.health import evaluate_pipeline_health


def test_health_is_degraded_when_pipeline_has_never_processed_data():
    result = evaluate_pipeline_health(None, threshold_seconds=60)

    assert result["status"] == "degraded"
    assert result["age_seconds"] is None


def test_health_is_healthy_for_recent_data():
    now = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
    result = evaluate_pipeline_health(now - timedelta(seconds=59), threshold_seconds=60, now=now)

    assert result == {
        "status": "healthy",
        "reason": "Telemetry is current.",
        "age_seconds": 59,
    }


def test_health_is_unhealthy_after_no_data_threshold():
    now = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
    result = evaluate_pipeline_health(now - timedelta(seconds=61), threshold_seconds=60, now=now)

    assert result["status"] == "unhealthy"
    assert result["age_seconds"] == 61
