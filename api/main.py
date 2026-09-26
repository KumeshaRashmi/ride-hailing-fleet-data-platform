"""Queryable serving API and observability endpoints for the fleet platform."""

from __future__ import annotations

import logging
import os
from collections import Counter
from contextlib import asynccontextmanager
from datetime import date
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, PlainTextResponse

from api.health import evaluate_pipeline_health
from observability.logging import get_logger, log_event
from storage.postgres import (
    fetch_health_snapshot,
    fetch_latest_utilization,
    fetch_profitability_report,
    initialize_schema,
    record_alert,
)


DATABASE_URL = os.getenv("FLEET_DATABASE_URL", "postgresql://fleet:fleet@postgres:5432/fleet")
NO_DATA_ALERT_SECONDS = int(os.getenv("NO_DATA_ALERT_SECONDS", "60"))
LOGGER = get_logger("fleet.api")
REQUESTS: Counter[str] = Counter()
REQUEST_LOCK = Lock()
LAST_ALERT_STATUS: str | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_schema(DATABASE_URL)
    log_event(LOGGER, "api_started", database_configured=True, no_data_alert_seconds=NO_DATA_ALERT_SECONDS)
    yield


app = FastAPI(
    title="Ride-Hailing Fleet Operations API",
    version="1.0.0",
    description="Live utilization, daily profitability and pipeline-health serving layer.",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_request(request: Request, call_next: Any) -> Any:
    with REQUEST_LOCK:
        REQUESTS["requests_total"] += 1
    response = await call_next(request)
    if response.status_code >= 400:
        with REQUEST_LOCK:
            REQUESTS["requests_error_total"] += 1
    return response


@app.get("/", tags=["service"])
def root() -> dict[str, str]:
    return {"service": "fleet-serving-api", "documentation": "/docs", "health": "/health"}


@app.get("/health", tags=["observability"])
def health() -> Any:
    """Health rule: mark unhealthy when Spark has not processed data for 60 seconds."""
    global LAST_ALERT_STATUS  # pylint: disable=global-statement
    try:
        snapshot = fetch_health_snapshot(DATABASE_URL)
    except Exception as error:  # database unavailability must be visible to operators
        log_event(LOGGER, "health_check_failed", logging.ERROR, error=str(error))
        return JSONResponse(
            status_code=503,
            content=jsonable_encoder({"status": "unhealthy", "reason": "Serving database is unavailable."}),
        )

    result = evaluate_pipeline_health(snapshot["last_processed_at"], NO_DATA_ALERT_SECONDS)
    result.update(
        {
            "last_processed_at": snapshot["last_processed_at"],
            "metric_rows": snapshot["metric_rows"],
            "profitability_rows": snapshot["profitability_rows"],
            "no_data_alert_seconds": NO_DATA_ALERT_SECONDS,
        }
    )
    if result["status"] != LAST_ALERT_STATUS:
        severity = "warning" if result["status"] == "degraded" else "critical"
        if result["status"] == "healthy":
            severity = "info"
        log_event(LOGGER, "pipeline_health_transition", severity=severity, **result)
        try:
            record_alert(
                DATABASE_URL,
                severity,
                "no_telemetry_processed",
                result["reason"],
                result,
            )
        except Exception as error:  # never hide a usable health response because alert persistence failed
            log_event(LOGGER, "alert_persistence_failed", logging.ERROR, error=str(error))
        LAST_ALERT_STATUS = result["status"]

    return JSONResponse(
        status_code=200 if result["status"] == "healthy" else 503,
        content=jsonable_encoder(result),
    )


@app.get("/metrics/fleet", tags=["fleet"])
def fleet_metrics(zone: str | None = Query(default=None, pattern="^colombo_|^outside_service_area$")) -> dict[str, Any]:
    """Return every zone in the newest metrics window, optionally filtered by zone."""
    rows = fetch_latest_utilization(DATABASE_URL, zone)
    if not rows:
        raise HTTPException(status_code=404, detail="No utilization metrics are available yet.")
    return {"metric_window_count": len(rows), "metrics": rows}


@app.get("/reports/profitability/{report_date}", tags=["fleet"])
def profitability_report(report_date: date) -> dict[str, Any]:
    """Return per-vehicle daily report and a concise unprofitability summary."""
    rows = fetch_profitability_report(DATABASE_URL, report_date.isoformat())
    if not rows:
        raise HTTPException(status_code=404, detail=f"No report exists for {report_date.isoformat()}.")
    unprofitable = [row for row in rows if row["profitability_status"] == "unprofitable"]
    return {
        "report_date": report_date,
        "vehicle_count": len(rows),
        "unprofitable_vehicle_count": len(unprofitable),
        "unprofitable_vehicles": [row["vehicle_id"] for row in unprofitable],
        "report": rows,
    }


@app.get("/metrics", response_class=PlainTextResponse, tags=["observability"])
def exported_metrics() -> str:
    """Expose small Prometheus-style operational counters for the demo."""
    try:
        snapshot = fetch_health_snapshot(DATABASE_URL)
        health_result = evaluate_pipeline_health(snapshot["last_processed_at"], NO_DATA_ALERT_SECONDS)
        age = health_result["age_seconds"] if health_result["age_seconds"] is not None else -1
    except Exception:
        snapshot = {"metric_rows": 0, "profitability_rows": 0}
        age = -1
    with REQUEST_LOCK:
        request_count = REQUESTS["requests_total"]
        error_count = REQUESTS["requests_error_total"]
    return "\n".join(
        [
            "# HELP fleet_api_requests_total Requests received by the serving API.",
            "# TYPE fleet_api_requests_total counter",
            f"fleet_api_requests_total {request_count}",
            "# HELP fleet_api_request_errors_total API responses with an error status.",
            "# TYPE fleet_api_request_errors_total counter",
            f"fleet_api_request_errors_total {error_count}",
            "# HELP fleet_serving_metric_rows Number of upserted utilization rows.",
            "# TYPE fleet_serving_metric_rows gauge",
            f"fleet_serving_metric_rows {snapshot['metric_rows']}",
            "# HELP fleet_last_telemetry_age_seconds Age of newest processed metric; -1 means absent.",
            "# TYPE fleet_last_telemetry_age_seconds gauge",
            f"fleet_last_telemetry_age_seconds {age}",
            "",
        ]
    )
