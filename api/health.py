"""Pure health-rule logic that can be tested without FastAPI or PostgreSQL."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def evaluate_pipeline_health(
    last_processed_at: datetime | None,
    threshold_seconds: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify telemetry freshness according to the no-data alert threshold."""
    evaluated_at = now or datetime.now(timezone.utc)
    if last_processed_at is None:
        return {
            "status": "degraded",
            "reason": "No telemetry metrics have been processed yet.",
            "age_seconds": None,
        }

    if last_processed_at.tzinfo is None:
        last_processed_at = last_processed_at.replace(tzinfo=timezone.utc)
    age_seconds = max(0, int((evaluated_at - last_processed_at).total_seconds()))
    if age_seconds > threshold_seconds:
        return {
            "status": "unhealthy",
            "reason": f"No telemetry processed for {age_seconds} seconds.",
            "age_seconds": age_seconds,
        }
    return {"status": "healthy", "reason": "Telemetry is current.", "age_seconds": age_seconds}
