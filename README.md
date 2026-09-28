# Ride-Hailing Fleet Data Platform

A complete Applied Big Data Engineering mini-project that gives a ride-hailing
operator live fleet utilization by area and a daily per-vehicle profitability
reconciliation. It uses a simulated 20-vehicle Colombo fleet.

## What the system answers

1. What are utilization, idle ratio, active-event count, and observed earnings
   by zone in the latest five-minute window?
2. Which vehicles are unprofitable after the simulated day's fuel and
   maintenance costs are reconciled with trip revenue?

## Architecture

The project uses a **Lambda architecture**. The speed path processes Kafka
telemetry at low latency, while the batch path joins the daily partner cost feed
with retained clean telemetry. Kappa was rejected because the daily CSV is a
periodic external feed and scheduled reconciliation is clearer than converting
every file into a continuous event stream.

```text
Python telemetry source -> Kafka -> Spark Structured Streaming -> clean Parquet
                                         |                    -> live metrics Parquet
                                         |                    -> PostgreSQL upsert
                                         +----------------------------> FastAPI

Python daily expense source -> CSV -> Airflow -> Spark batch join -> Parquet + CSV
                                                        +----------> PostgreSQL -> FastAPI
```

One simulated day equals **five real minutes** by default. The daily expense
generator supports `--once` for a manual run and is also called by Airflow.

## Components and data contracts

| Layer | Implementation | Purpose |
| --- | --- | --- |
| Streaming ingress | Kafka + Python producer | Durable partitioned telemetry topic |
| Daily ingress | Python CSV generator | One expense record per vehicle/day |
| Speed processing | Spark Structured Streaming | Validation, zone enrichment, five-minute aggregation |
| Batch processing | Spark SQL | Trip de-duplication and revenue/cost reconciliation |
| Storage | Parquet + PostgreSQL | Replayable lake outputs and queryable serving tables |
| Serving | FastAPI | Fleet metrics, profitability report, health, metrics export |
| Orchestration | Airflow | Scheduled daily feed and reconciliation |

Telemetry topic `fleet-telemetry`:

```text
trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp
```

Daily expense CSV:

```text
report_date, vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag
```

Spark validates telemetry, maps the Colombo coordinate grid into four zones,
and writes clean events to `data_lake/clean_telemetry/`. It writes five-minute
zone metrics to `data_lake/realtime_utilization_metrics/` and PostgreSQL when
`FLEET_DATABASE_URL` is set. The batch report de-duplicates by
`(vehicle_id, trip_id)`, takes the maximum observed fare per trip, then
calculates revenue, total expense, profit, margin, and profitability status.

## Run locally on Windows with WSL2

Use Docker Desktop for Kafka, PostgreSQL, FastAPI, and Airflow. Use WSL2/Ubuntu
for the Python producer and Spark jobs. The Windows virtualenv cannot be
activated from Linux, and native Windows Spark requires additional Hadoop
Windows support files (`winutils.exe`); this guide avoids that requirement.
Keep the repository in the Windows workspace and access it from WSL under
`/mnt/c/...`.

Prerequisites: Docker Desktop, WSL2 with Ubuntu, Python 3.10+, and Java 17 in
WSL. Docker commands below run in Windows PowerShell. Application commands run
in WSL Bash. Docker Desktop's WSL integration is only needed if you also want
to run Docker commands from inside WSL.

### 1. Start Kafka, PostgreSQL, and the API

In **Windows PowerShell**, change to the repository root and start the base
services:

```powershell
docker compose up -d kafka postgres api
docker compose ps
```

Wait for Kafka, PostgreSQL, and API to show as running/healthy. Open
`http://localhost:8000/docs` to verify the API is available. Before Spark writes
the first metric, `/health` is expected to report `degraded`.

### 2. Create the WSL Python environment once

Open a WSL/Ubuntu terminal in VS Code. These are Bash commands, not PowerShell
commands. Create the Linux virtualenv in your WSL home directory; do not reuse
the Windows `.venv`:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jdk

cd "/mnt/c/Users/A S U S/OneDrive/Documents/semester 8/Big data/ride-hailing/ride-hailing-fleet-data-platform"
mkdir -p "$HOME/.venvs"
python3 -m venv "$HOME/.venvs/ride-hailing"
source "$HOME/.venvs/ride-hailing/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The command uses this workspace's path. If you cloned elsewhere, replace the
`cd` path with the repository directory shown in WSL. Activate the environment
with `source "$HOME/.venvs/ride-hailing/bin/activate"` in each new WSL terminal.
The first PySpark installation downloads a large package set.

### 3. Start the telemetry producer

In **WSL terminal 1**, from the project root:

```bash
source "$HOME/.venvs/ride-hailing/bin/activate"
export FLEET_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python -m kafka_ingestion.producer
```

Leave this terminal running. It should emit `telemetry_event_published` JSON
logs with Kafka topic, partition, offset, and vehicle information.

### 4. Start the Spark streaming job

In **WSL terminal 2**, from the same project root, set UTC explicitly and run
Spark. The Kafka connector version is derived from installed PySpark so the two
versions stay aligned:

```bash
source "$HOME/.venvs/ride-hailing/bin/activate"
export TZ=UTC
export FLEET_DATABASE_URL="postgresql://fleet:fleet@localhost:5432/fleet"
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

pyspark_version="$(python -c 'import pyspark; print(pyspark.__version__)')"
spark_home="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
mkdir -p .ivy2

"$spark_home/bin/spark-submit" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.jars.ivy=$PWD/.ivy2" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${pyspark_version}" \
  processing/stream_processor.py
```

Leave Spark running. Wait for `spark_streaming_started`, then
`utilization_metrics_batch_written`. A complete event-time window may take up
to five minutes. Spark writes clean Parquet to `data_lake/clean_telemetry/`,
live metrics to `data_lake/realtime_utilization_metrics/`, and metrics to
PostgreSQL. Docker's Compose mounts make these workspace files visible to the
Airflow containers. Do not run `streaming/spark_streaming.py`; the documented
pipeline entry point is `processing/stream_processor.py`.

### 5. Check live API results

In WSL, use `curl`:

```bash
curl -i "http://localhost:8000/health"
curl -i "http://localhost:8000/metrics/fleet"
curl -i "http://localhost:8000/metrics"
```

`/metrics/fleet` returns 404 until the first metrics batch exists. `/health`
should become `healthy` after a recent metric write. API documentation is at
`http://localhost:8000/docs`. In Windows PowerShell, use `curl.exe` or
`Invoke-RestMethod`; PowerShell's `curl` alias does not accept the usual curl
options.

### 6. Run the daily profitability batch manually (optional)

Run this only after streaming has written clean telemetry for the report date.
In **WSL terminal 3**, use today's UTC date so the expense feed and telemetry
dates match:

```bash
source "$HOME/.venvs/ride-hailing/bin/activate"
export TZ=UTC
export FLEET_DATABASE_URL="postgresql://fleet:fleet@localhost:5432/fleet"
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
report_date="$(date -u +%F)"

python -m data_generators.daily_expense_generator \
  --once \
  --start-date "$report_date"

spark_home="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
"$spark_home/bin/spark-submit" \
  --conf "spark.sql.session.timeZone=UTC" \
  processing/profitability_job.py \
  --expenses "data_lake/daily_expenses/vehicle_expenses_${report_date}.csv" \
  --report-date "$report_date" \
  --postgres-dsn "$FLEET_DATABASE_URL"
```

The generator protects an existing date file. Add `--overwrite` only when you
intentionally want to replace that day's randomized costs. The report is saved
under `reports/profitability/report_date=<date>/` and upserted to PostgreSQL.
Do not run this manual batch at the same time as Airflow reconciliation for
the same date; both replace the same report output directory.

### 7. Run the Airflow scheduled batch (optional)

In **Windows PowerShell**, initialize Airflow once, then start the orchestration
profile:

```powershell
docker compose --profile orchestration up airflow-init
docker compose --profile orchestration up -d
docker compose --profile orchestration ps
```

Open `http://localhost:8080` and sign in with `admin` / `admin` for this local
demo only. Find `daily_fleet_profitability_reconciliation`, make sure it is
unpaused, then trigger a run. Wait for both `create_expense_feed` and
`reconcile_daily_report` to succeed before triggering another run. The DAG is
scheduled every five minutes; avoid overlapping manual and scheduled runs
because they write the same date's report outputs.

Airflow reads the clean telemetry `part-*.parquet` files directly. This avoids
WSL-only `/mnt/c/...` paths recorded in the streaming sink's `_spark_metadata`;
those paths are not valid inside the Airflow container even though the Parquet
files themselves are mounted under `/opt/airflow/project/data_lake/`.

### 8. Query and verify the profitability report

From WSL, use the date from the expense generator or the Airflow run:

```bash
report_date="$(date -u +%F)"
curl -i "http://localhost:8000/reports/profitability/${report_date}"
find "reports/profitability/report_date=${report_date}" -type f -name '*.csv'
```

The report should contain all 20 vehicles and include trip revenue, costs,
profit, margin, and profitability status. Exact values and the count of
unprofitable vehicles vary because the generator uses random data.

### 9. Demonstrate the no-data health alert

First confirm `/health` returns `200` and `healthy`. Stop **only the producer**
with `Ctrl+C`; leave Spark, Docker, PostgreSQL, and the API running. After at
least 61 seconds, request health again:

```bash
curl -i "http://localhost:8000/health"
```

It should return `503` with `unhealthy`. Capture the response, restart the
producer, wait for a new `utilization_metrics_batch_written` log, and confirm
`/health` returns `200 healthy` again. Health transitions are persisted in
PostgreSQL's `pipeline_alerts` table.

### 10. Run tests and stop the platform

In a WSL terminal at the project root:

```bash
source "$HOME/.venvs/ride-hailing/bin/activate"
python -m pytest -q
```

Stop producer and Spark with `Ctrl+C` in their WSL terminals. Stop Compose
services from **Windows PowerShell** when finished:

```powershell
docker compose --profile orchestration down
```

This preserves the PostgreSQL named volume. To also delete the database volume
and its stored serving data, use `docker compose down -v` only when you
intentionally want a clean reset.

## Tests and submission material

`docs/FINAL_REPORT.md` is the report draft. Replace only the explicitly marked
evidence placeholders with genuine screenshots before exporting to PDF.
`docs/DEMO_AND_SUBMISSION_CHECKLIST.md` lists the required demo evidence and
submission steps. Use `docs/REPORT_FINALIZATION_GUIDE.md` to complete and export
the report, and `docs/DEMO_VIDEO_SCRIPT.md` for the timed three-member narration
and recording workflow. Runtime data, virtual environments, Ivy cache, Spark
checkpoints, and generated reports are intentionally ignored by Git.