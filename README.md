# Ride-Hailing Fleet Data Platform

An end-to-end Applied Big Data Engineering mini-project for fleet utilization
and daily vehicle profitability. The platform answers two business questions:

1. What is the fleet's utilization and observed earnings by Colombo area in the
   latest five-minute window?
2. Which vehicles became unprofitable after that simulated day's fuel and
   maintenance costs are applied?

## Architecture decision

We chose a **Lambda architecture**. Telemetry needs near-real-time processing,
whereas expenses arrive once per simulated day and must be reconciled with
historical trip data. A single Kappa stream would force daily partner files
through an artificial event stream and make the scheduled reconciliation less
clear. Lambda does mean two processing paths, but its speed/batch separation
matches the different latency and consistency needs of this use case.

```text
Telemetry generator -> Kafka topic -> Spark Structured Streaming -> Parquet: clean telemetry
                                      |                         -> Parquet: live five-minute metrics
                                      |
Expense generator (one file / simulated day) -> CSV -------------> Spark batch reconciliation
                                                                  -> Parquet + CSV profitability report
```

The default simulated clock is **one day = five real minutes**. The generator's
`--once` option lets the same feed be triggered by an Airflow DAG for a short
demo.

## Current implementation

| Layer | Status | Main artifact |
| --- | --- | --- |
| Streaming source and ingestion | Complete | `data_generators/streaming_generator.py`, `kafka_ingestion/producer.py` |
| Kafka broker | Complete | `docker-compose.yml` |
| Daily source | Complete | `data_generators/daily_expense_generator.py` |
| Spark speed and batch paths | Complete | `processing/stream_processor.py`, `processing/profitability_job.py` |
| Database, API, Airflow and end-to-end monitoring | Assigned next | See `WORK_ALLOCATION.md` |

## Technology choices

- **Kafka**: durable, partitioned event ingestion with a producer/topic model
  that suits frequent vehicle telemetry.
- **Spark Structured Streaming**: windowed event-time aggregations from Kafka
  and a single programming model for the daily Spark batch job.
- **Parquet**: columnar, queryable storage for clean events, live metric updates
  and daily reports; it is inexpensive and suitable for Spark SQL.
- **PostgreSQL + FastAPI + Airflow** (next layer): PostgreSQL is a practical
  serving store, FastAPI exposes the metrics/reports, and Airflow schedules the
  daily reconciliation. Those deliverables are intentionally separated for the
  third group member.

## Data contracts

The Kafka event schema is:

```text
trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp
```

Each daily CSV has exactly one record per fleet vehicle:

```text
report_date, vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag
```

The stream job rejects null/invalid records, assigns each simulated coordinate
to a documented four-zone Colombo grid, and produces a five-minute window per
zone. It writes raw clean events to `data_lake/clean_telemetry/` and aggregate
updates to `data_lake/realtime_utilization_metrics/`. Each aggregate includes
`spark_batch_id` and `processed_at`, so a serving layer can select the latest
row for a `(window_start, window_end, zone)` key.

The batch job de-duplicates telemetry by `(vehicle_id, trip_id)`, takes the
maximum observed fare for that trip, then joins per-vehicle revenue to the daily
expense feed. Its result includes `total_expense`, `profit`,
`profit_margin_pct`, and `profitability_status`.

## Run locally

Prerequisites: Python 3.10+, Java 17+ and Docker Desktop. Run all commands from
the repository root.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d kafka
python -m kafka_ingestion.producer
```

In a second terminal, start the Spark speed path. The Kafka connector must be
provided to Spark; the package version matches the pinned PySpark version.

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 processing/stream_processor.py
```

After telemetry has run for at least one micro-batch, create the daily feed and
reconcile the same logical date (replace the example date with today's date or
the `--start-date` supplied to the generator):

```powershell
python -m data_generators.daily_expense_generator --once --start-date 2026-09-26
spark-submit processing/profitability_job.py --expenses data_lake/daily_expenses/vehicle_expenses_2026-09-26.csv --report-date 2026-09-26
```

The first command is intentionally a continuous process. Stop it with
`Ctrl+C` after the demo. Generated data, Spark checkpoints and report output
are ignored by Git.

## Test

```powershell
pytest -q
```

The unit tests validate telemetry-data invariants, one complete daily expense
record per vehicle, atomic/replay-safe CSV publication, and the documented zone
mapping. The end-to-end demo checklist and the third-member hand-off are in
`WORK_ALLOCATION.md`.
