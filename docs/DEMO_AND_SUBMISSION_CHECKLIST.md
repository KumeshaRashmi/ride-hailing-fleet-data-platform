# Demo and Submission Checklist

This is the final verification document. Run it from a clean clone after
pulling the latest branch; do not treat a passing unit-test suite as a substitute
for the end-to-end evidence required by the assignment.

## 1. Repository hygiene

- [ ] `git status --short` is empty before the demo.
- [ ] No `.venv`, `.ivy2`, `.pip-temp`, `data_lake`, `reports`, credentials or
  database dumps are staged.
- [ ] `pytest -q` passes.
- [ ] `docker compose config --quiet` exits with code zero.
- [ ] `README.md`, `WORK_ALLOCATION.md` and `docs/FINAL_REPORT.md` match the
  final implementation and team membership.

## 2. Start the running platform

- [ ] `docker compose up -d kafka postgres api` completes.
- [ ] `docker compose ps` shows Kafka, PostgreSQL and API as running/healthy.
- [ ] Open `http://localhost:8000/docs`; capture one screenshot of the API docs.
- [ ] Start `python -m kafka_ingestion.producer`; capture JSON published-event
  logs showing topic, vehicle, partition and offset.
- [ ] Start `processing/stream_processor.py` with the documented Spark command
  and `FLEET_DATABASE_URL` value; capture its `spark_streaming_started` log.
- [ ] Wait for one completed five-minute window and capture a JSON
  `utilization_metrics_batch_written` log.

## 3. Verify the business results

- [ ] `GET /metrics/fleet` returns one or more zones with active events, idle
  ratio and observed earnings. Capture the response.
- [ ] Run the daily expense generator once using the same logical date as the
  telemetry data. Capture the 20-record CSV header and a few rows.
- [ ] Run the profitability Spark job or successful Airflow DAG. Capture the
  `daily_profitability_report_created` log and generated CSV report.
- [ ] `GET /reports/profitability/YYYY-MM-DD` returns all 20 vehicle records;
  capture the unprofitable vehicle list.

## 4. Verify orchestration and observability

- [ ] Initialise and start the Airflow profile, then trigger
  `daily_fleet_profitability_reconciliation`.
- [ ] Capture the DAG graph and both successful task instances.
- [ ] Capture `GET /metrics` showing API counters and telemetry-age gauge.
- [ ] While healthy, capture `GET /health` with its latest `processed_at` value.
- [ ] Stop the producer, wait **at least 61 seconds**, then capture `/health`
  returning `503` and `unhealthy`.
- [ ] Restart the producer, wait for a Spark metric batch, then capture healthy
  recovery. This proves the health rule is an actual alert rather than a static
  endpoint.

## 5. Final deliverables

- [ ] Replace each `[INSERT REAL SCREENSHOT]` marker in `docs/FINAL_REPORT.md`.
- [ ] Export that document as the final PDF; inspect its page count (target
  8–15 pages), tables, diagram and screenshots.
- [ ] Record a 5–10 minute video following sections 2–4 above.
- [ ] Include the repository URL, report PDF, video link and concise individual
  contributions statement in the LMS submission.
- [ ] Have each member explain their assigned modules before submission; the
  assessment permits AI assistance but requires defending the core logic.
