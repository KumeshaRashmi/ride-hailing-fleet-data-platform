# Ride-Hailing Fleet Data Platform

A complete Applied Big Data Engineering mini-project that gives a ride-hailing
operator live fleet utilization by area and a daily per-vehicle profitability
reconciliation. It uses a simulated 20-vehicle Colombo fleet.

## What the system answers

1. What are utilization, idle ratio, active-event count and observed earnings
   by zone in the latest five-minute window?
2. Which vehicles are unprofitable after the simulated day's fuel and
   maintenance costs are reconciled with trip revenue?

## Architecture decision

The project uses a **Lambda architecture**. The speed path processes Kafka
telemetry at low latency, while the batch path joins the daily partner cost feed
with retained clean telemetry. Kappa was rejected because the daily CSV is a
genuine periodic external feed and a scheduled reconciliation is clearer and
less operationally wasteful than converting every file into a perpetual event
stream.

```text
Python telemetry source -> Kafka -> Spark Structured Streaming -> clean Parquet
                                         |                    -> live metrics Parquet
                                         |                    -> PostgreSQL upsert
                                         |                                  |
                                         +----------------------------> FastAPI /metrics/fleet

Python daily expense source -> CSV -> Airflow -> Spark batch join -> Parquet + CSV report
                                                        |                  |
                                                        +----------> PostgreSQL -> FastAPI report
```

One simulated day equals **five real minutes** by default. The same generator
supports `--once` for a manual demo and is called by Airflow for scheduled runs.

## Components

| Layer | Implementation | Purpose |
| --- | --- | --- |
| Streaming ingress | Kafka + Python producer | Durable partitioned telemetry topic |
| Daily ingress | Python CSV generator | One expense record per vehicle/day |
| Speed processing | Spark Structured Streaming | Validation, zone enrichment, five-minute aggregation |
| Batch processing | Spark | Trip de-duplication, revenue/cost reconciliation |
| Storage | Parquet + PostgreSQL | Replayable lake outputs and queryable serving tables |
| Serving | FastAPI | Fleet metrics, profitability report, health, metrics export |
| Orchestration | Airflow | Scheduled daily feed and reconciliation |
| Observability | JSON logs, API metrics, health rule | Diagnosis and no-data alerting |

## Data contracts

Telemetry topic `fleet-telemetry`:

```text
trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp
```

Daily expense CSV:

```text
report_date, vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag
```

Spark rejects invalid telemetry, maps the simulated coordinate rectangle to four
Colombo zones, and writes raw clean events to `data_lake/clean_telemetry/`.
It writes five-minute zone metrics to `data_lake/realtime_utilization_metrics/`
and PostgreSQL when `FLEET_DATABASE_URL` is set. The batch report de-duplicates
by `(vehicle_id, trip_id)`, takes the maximum observed fare per trip, then
calculates revenue, total expense, profit, margin and profitability status.

## Quick start: complete local demo on Windows

Prerequisites: Docker Desktop, Java 17+ and Python 3.10+. Run from the project
root. The first PySpark installation is large; `docs/DEMO_AND_SUBMISSION_CHECKLIST.md`
includes a short-temp-folder workaround for Windows path and disk limitations.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --no-cache-dir -r requirements.txt
docker compose up -d kafka postgres api
```

Wait until the API is available, then open `http://localhost:8000/docs`.
The initial `/health` result is deliberately `degraded` until Spark processes
the first telemetry metric.

Set the host-side connection string and start the producer in terminal 1:

```powershell
$env:FLEET_DATABASE_URL = "postgresql://fleet:fleet@localhost:5432/fleet"
python -m kafka_ingestion.producer
```

In terminal 2, activate `.venv`, set the same database value, configure your
Windows Spark prerequisites, then start the speed path:

```powershell
.\.venv\Scripts\Activate.ps1
$env:FLEET_DATABASE_URL = "postgresql://fleet:fleet@localhost:5432/fleet"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:HADOOP_HOME = "F:\hadoop-3.3.5"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"

$sparkHome = python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))"
& "$sparkHome\bin\spark-submit.cmd" `
  --conf "spark.jars.ivy=$((Get-Location).Path)\.ivy2" `
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5" `
  processing\stream_processor.py
```

`HADOOP_HOME\bin\winutils.exe` is needed by this Windows local Spark setup.
Do not commit it. See the demo runbook for the safe setup note. Linux/Docker
Spark environments do not require this Windows compatibility binary.

After at least one five-minute window has been processed, use terminal 3 to
create and reconcile the same logical date. Use today’s date in a real run.

```powershell
.\.venv\Scripts\Activate.ps1
$env:FLEET_DATABASE_URL = "postgresql://fleet:fleet@localhost:5432/fleet"
python -m data_generators.daily_expense_generator --once --start-date 2026-09-27

$sparkHome = python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))"
& "$sparkHome\bin\spark-submit.cmd" processing\profitability_job.py `
  --expenses data_lake\daily_expenses\vehicle_expenses_2026-09-27.csv `
  --report-date 2026-09-27 `
  --postgres-dsn $env:FLEET_DATABASE_URL
```

Query the results:

```powershell
Invoke-RestMethod http://localhost:8000/metrics/fleet
Invoke-RestMethod http://localhost:8000/reports/profitability/2026-09-27
Invoke-WebRequest http://localhost:8000/metrics
```

## Airflow scheduled batch path

The default Compose command does not start heavy Airflow containers. Initialise
them once, then start the orchestration profile:

```powershell
docker compose --profile orchestration up airflow-init
docker compose --profile orchestration up -d
```

Open `http://localhost:8080` and sign in as `admin` / `admin` for this local
demo only. Trigger `daily_fleet_profitability_reconciliation`, or let its
five-minute schedule run after clean telemetry exists. The DAG writes the CSV,
runs the Spark batch job and upserts the report into PostgreSQL.

## Observability demonstration

Every producer, generator, Spark batch and API lifecycle event is JSON logged.
`GET /metrics` exposes Prometheus-style API/serving gauges. `GET /health`
evaluates the latest `processed_at` timestamp:

- `healthy`: data processed in the past 60 seconds;
- `degraded`: no Spark metric has ever been processed; and
- `unhealthy`: no telemetry has been processed for more than 60 seconds.

For the required alert proof, first obtain a healthy response, stop the
producer, wait 61 seconds, call `/health`, capture the 503 `unhealthy`
response, restart the producer and capture the recovery. Alert transitions are
also retained in the `pipeline_alerts` PostgreSQL table.

## Tests and submission material

```powershell
pytest -q
```

`docs/FINAL_REPORT.md` is the report draft. Replace only the explicitly marked
evidence placeholders with screenshots from the real run before exporting to
PDF. `docs/DEMO_AND_SUBMISSION_CHECKLIST.md` is the final reviewer’s exact
checklist. Runtime data, virtual environments, Ivy cache, Spark checkpoints and
reports are intentionally ignored by Git.
