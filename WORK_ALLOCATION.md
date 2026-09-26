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

## Member 3 — serving, orchestration, observability and final evidence

Create a separate branch from the latest Member 2 commit and implement the
following. These tasks are deliberately non-overlapping and together close the
remaining rubric items.

1. **Serving store and API**
   - Extend Docker Compose with PostgreSQL and a FastAPI service.
   - Create tables/upsert logic for the newest utilization row per
     `(window_start, window_end, zone)` and for daily per-vehicle profitability.
     The source columns are documented in `README.md`; read the generated
     Parquet outputs rather than changing the processor's schema.
   - Implement `GET /health`, `GET /metrics/fleet`, and
     `GET /reports/profitability/{report_date}`. The metrics endpoint must
     return window, zone, idle ratio, active-event count and observed earnings.

2. **Airflow daily orchestration**
   - Add an Airflow service and a DAG that runs in this order: create one dated
     expense CSV, wait/validate the expected file, run the profitability Spark
     job, load the report into PostgreSQL, and log the resulting record count.
   - Make the DAG date parameter explicit so the expense file and report use
     the same logical date. For a demo, schedule every five minutes or trigger
     manually.

3. **Observability**
   - Replace ad-hoc prints in the existing producer with JSON/structured logs
     (event name, timestamp, vehicle/topic, status and error fields).
   - Add a health-check/alert rule that becomes unhealthy when no telemetry is
     processed for more than 60 seconds; surface it through `/health` and log
     an alert. Add basic counters for received, rejected and processed events.
   - Document how the marker is demonstrated: stop the producer, wait 60
     seconds, call `/health`, then restart it.

4. **Submission evidence**
   - Finish Docker Compose setup/run instructions and add API/integration tests.
   - Capture screenshots of Kafka/producer activity, Spark output, the daily
     report, API responses and the no-data alert.
   - Draft the report sections: architecture diagram; Lambda-vs-Kappa argument
     and rejected Kappa alternative; per-tool justification; observability;
     limitations/production-scale improvements; and all three contributions.

## Integration acceptance checklist

Before merging the three branches, demonstrate the following in order:

1. `docker compose up` starts Kafka, PostgreSQL, Airflow and the API.
2. The producer emits events and Spark writes clean telemetry plus five-minute
   metrics to Parquet.
3. A dated expense CSV appears every five real minutes (or via the Airflow DAG).
4. The daily Spark job produces a report containing all 20 vehicles and at
   least the revenue, costs, profit and profitability status columns.
5. The API returns current utilization and the selected day's unprofitable
   vehicles.
6. Stopping the producer causes the documented no-data health alert within
   60 seconds; restarting it returns the service to healthy.

## Important integration rules

- Treat `data_lake/` and `reports/` as generated runtime data; do not commit
  them. Commit sample screenshots only if they are small and intentional.
- Preserve the field names in the two data contracts. If a change is necessary,
  update all three consumers and the README in the same pull request.
- Keep `report_date` in ISO `YYYY-MM-DD` form. Airflow's execution date must
  become that same value before selecting an expense file.
