# Three-member delivery plan

This file records ownership so the final submission can include the required
individual-contribution statement and so two people do not implement the same
layer on different branches.

## Member 1 — streaming source and Kafka ingestion (complete)

Delivered on commits `c0b4d70` and `60272d9`:

- fleet telemetry simulator with the required IDs, coordinates, speed, status,
  fare and UTC timestamp;
- Kafka producer publishing to `fleet-telemetry`; and
- single-node Docker Kafka configuration plus generator unit tests.

## Member 2 — data processing (this branch)

Delivered by this change:

- a daily expense CSV source, with one expense record per vehicle per logical
  day and an explicit five-minute simulated-day clock;
- Spark Structured Streaming validation, enrichment and five-minute zone-level
  utilization/earnings aggregation from Kafka;
- Parquet outputs and checkpoints for queryable, replayable processing data;
- a Spark batch profitability reconciliation joining deduplicated trip revenue
  to fuel/maintenance costs; and
- tests and documentation for the source/data contracts.

## Member 3 — final reviewer, evidence owner and submission integrator

The missing functional layers have now been implemented in this branch:

- PostgreSQL schema/upsert adapter and FastAPI serving endpoints;
- Docker Compose services for PostgreSQL, FastAPI and optional Airflow;
- Airflow DAG for the five-minute simulated daily reconciliation;
- JSON logging, Prometheus-style API metrics, and a 60-second no-data health
  rule that persists alert transitions; and
- a report draft, runbook and submission checklist under `docs/`.

Member 3 should now work as the independent final reviewer rather than duplicate
the implementation. Their required deliverables are:

1. Follow `docs/DEMO_AND_SUBMISSION_CHECKLIST.md` end-to-end on a clean clone.
   Record defects as GitHub issues and fix only verified defects on a review
   branch; do not change data contracts without team agreement.
2. Capture genuine screenshots of the running producer, Spark stream, API
   `/metrics/fleet`, daily profitability endpoint, Airflow successful DAG, and
   the `/health` 60-second no-data alert/recovery. Replace each marked evidence
   placeholder in `docs/FINAL_REPORT.md`.
3. Record a 5–10 minute demo video, export `docs/FINAL_REPORT.md` to the final
   PDF, and verify the repository contains no credentials, virtual environments,
   Maven/Ivy caches, generated data or other large runtime files.
4. Check every rubric row against the final checklist, open a pull request with
   the screenshots/report only, and obtain the team’s final approval before
   submission.

## Integration acceptance checklist

Before merging the three branches, demonstrate the following in order:

1. `docker compose up -d kafka postgres api` starts Kafka, PostgreSQL and the API.
2. The producer emits events and Spark writes clean telemetry plus five-minute
   metrics to Parquet.
3. A dated expense CSV appears every five real minutes (or via the Airflow DAG).
4. The daily Spark job produces a report containing all 20 vehicles and at
   least the revenue, costs, profit and profitability status columns.
5. The API returns current utilization and the selected day's unprofitable vehicles.
6. Stopping the producer causes the documented no-data health alert within
   60 seconds; restarting it returns the service to healthy.

## Important integration rules

- Treat `data_lake/` and `reports/` as generated runtime data; do not commit
  them. Commit sample screenshots only if they are small and intentional.
- Preserve the field names in the two data contracts. If a change is necessary,
  update all three consumers and the README in the same pull request.
- Keep `report_date` in ISO `YYYY-MM-DD` form. Airflow's execution date must
  become that same value before selecting an expense file.
