# Demo Video Script by Member

**Target duration:** 8 minutes  
**Speaking time:** 2 minutes 40 seconds per member  
**Recording style:** Slides for the opening; real project screens for the technical demonstration  
**Scenario:** Ride-Hailing Fleet Operations

Replace `[Member 1]`, `[Member 2]`, `[Member 3]`, and `[DATE]` before recording. Use a report date that exists in the database and state values visible in the live output. Do not claim an alert transition unless you have recorded evidence of it.

## Before Recording

1. Start the project services and confirm Kafka, Kafka UI, PostgreSQL, pgAdmin, API, and Airflow are available. Start the telemetry producer and Spark streaming job in separate terminals.
2. Confirm Kafka UI shows recent messages in `fleet-telemetry`, `/metrics/fleet` returns zone metrics, and `/health` is healthy for the current run.
3. Confirm the profitability report exists for `[DATE]` and the Airflow DAG has a successful run with both tasks green.
4. Prepare these windows in the order used below: title slide, architecture slide, generator source, producer terminal, Kafka UI, expense CSV, Spark terminal, stream processor source, fleet metrics API, profitability CSV, batch log, profitability API, API docs, pgAdmin, Airflow, metrics/health API, closing slide.
5. Log in to pgAdmin, open the `fleet` database and one relevant table before recording. Hide credentials and the pgAdmin master password.
6. Set browser zoom and terminal font sizes so text can be read in the final video. Close unrelated windows and silence notifications.
7. Record a short audio/video test. Have speakers practice the handoffs and keep the total recording close to eight minutes.

## Member 1: Introduction and Sources (0:00-2:40)

| Time | Screen | Speaker action |
| --- | --- | --- |
| 0:00-0:15 | PowerPoint title slide with project name, course, and all three member names. | Member 1 introduces the project. |
| 0:15-0:45 | Architecture slide or `docs/architecture.svg`, full-screen. Point to both paths while speaking. | Member 1 explains the business problem and Lambda paths. |
| 0:45-1:05 | `data_generators/streaming_generator.py`, showing only relevant event fields. | Member 1 describes the simulated telemetry. |
| 1:05-1:30 | Producer terminal with recent JSON event logs. | Member 1 points out topic, vehicle, partition, and offset. |
| 1:30-2:00 | Kafka UI at `http://localhost:8081`, cluster `fleet-local`, topic `fleet-telemetry`, recent messages/partition view. | Member 1 shows messages reaching Kafka. |
| 2:00-2:30 | Generated daily expense CSV, header and two or three rows visible. | Member 1 describes the batch input. |
| 2:30-2:40 | Leave the CSV visible, then switch to the Spark terminal. | Member 1 hands off to Member 2. |

**Member 1 speech**

**0:00-0:15, title slide:**

> Hello, we are [Member 1], [Member 2], and [Member 3]. Our project is a ride-hailing fleet data platform that turns vehicle telemetry and daily operating costs into useful fleet information.

**0:15-0:45, architecture slide:**

> The operator needs to know what is happening across the fleet now and which vehicles are profitable after daily expenses. Our Lambda design has a live path for continuous telemetry and a batch path for daily expenses. Both paths produce data that can be queried by the serving layer.

**0:45-1:05, generator source:**

> This generator simulates 20 vehicles and creates a telemetry event every three seconds. Each event includes identifiers, location, speed, trip status, fare, and a UTC timestamp.

**1:05-1:30, producer terminal:**

> The producer publishes each event to the `fleet-telemetry` Kafka topic. These log fields show the vehicle, partition, and offset, confirming the event was accepted by Kafka.

**1:30-2:00, Kafka UI:**

> Kafka UI lets us inspect the broker and the topic in a browser. Here we can see recent telemetry messages and the topic partitions. The producer connects through the host address, while Kafka UI connects to Kafka over the Docker network.

**2:00-2:30, expense CSV:**

> The batch input is a dated expense CSV with one record per vehicle. It includes fuel and maintenance costs, distance, and a service flag. Five real minutes represent one simulated day, so we can demonstrate a daily reconciliation in a short lab session.

**2:30-2:40, handoff:**

> I’ve shown how the two sources enter the platform. [Member 2] will show how Spark processes the live stream and produces the fleet metrics.

## Member 2: Spark Processing and Outputs (2:40-5:20)

| Time | Screen | Speaker action |
| --- | --- | --- |
| 2:40-3:10 | Spark terminal showing `spark_streaming_started` and the latest `utilization_metrics_batch_written` event. | Member 2 explains the live processing path. |
| 3:10-3:35 | `processing/stream_processor.py`, relevant schema/validation/window section only. | Member 2 points to validation, zoning, watermark, and window logic. |
| 3:35-4:05 | `http://localhost:8000/metrics/fleet`, latest window and four zones visible. | Member 2 interprets live metrics. |
| 4:05-4:30 | In VS Code Explorer open `reports/profitability/report_date=[DATE]/` and click its `part-*.csv` file. Show the header and first few rows. Use the same report date as the API. | Member 2 introduces the actual generated report and points to revenue, costs, profit, and status columns. |
| 4:30-4:55 | Show the successful Airflow `reconcile_daily_report` task log at the `daily_profitability_report_created` event. If that log is not available, show `processing/profitability_job.py` around `build_profitability_report` and the revenue/cost join. | Member 2 explains how per-trip observed fare is aggregated by vehicle, joined to expenses, then used to calculate profit and status. |
| 4:55-5:10 | In VS Code Explorer open `data_lake/profitability_reports/report_date=[DATE]/` and point to the `part-*.snappy.parquet` output and `_SUCCESS` marker. | Member 2 explains that Parquet preserves the analytical batch output, while PostgreSQL/API provide queryable serving results. |
| 5:10-5:20 | Switch browser to `http://localhost:8000/reports/profitability/[DATE]`; leave the response loaded with its date and summary visible. | Member 2 hands off; Member 3 begins presenting this API response at 5:20. |

**Member 2 speech**
**2:40-3:10, Spark terminal:**

> Spark Structured Streaming reads the Kafka topic, validates incoming records, and assigns each event to a Colombo reporting zone. This log shows the stream starting and a metrics batch being written.

**3:10-3:35, stream processor source:**

> The processor rejects invalid identifiers, timestamps, statuses, speeds, and fares. It uses a two-minute watermark for late data and five-minute event-time windows to calculate counts, activity, earnings, average speed, and idle ratio.

**3:35-4:05, fleet metrics API:**

> This endpoint shows the latest completed metrics window for each zone. We can compare event volume, active and idle counts, observed earnings, and idle ratio. Spark writes these results to Parquet and upserts serving rows into PostgreSQL.

**4:05-4:30, profitability CSV:**

> The batch job produces a dated profitability report. This CSV contains the calculated values per vehicle, including revenue, expenses, profit, and profitability status. We are showing the report for [DATE], the same date used by the API.

**4:30-4:55, batch log or batch source:**

> The batch job groups telemetry observations by vehicle and trip, uses the maximum observed on-trip fare as a revenue proxy, then joins those totals to the expense feed. The result calculates total expense, profit, margin, and profitability status. This is simulated observed fare, not settled payment data.

**4:55-5:10, Parquet output:**

> Spark also stores the report as Parquet in the data lake, with a success marker showing the output write completed. PostgreSQL serves the report through the API, which [Member 3] will show next.

**5:10-5:20, handoff:**

> That covers the stream and batch processing. [Member 3] will show the served profitability report, database, orchestration, and health status.

## Member 3: Serving, Orchestration, and Close (5:20-8:00)

| Time | Screen | Speaker action |
| --- | --- | --- |
| 5:20-5:55 | `http://localhost:8000/reports/profitability/[DATE]`, report date, vehicle count, and unprofitable summary visible. | Member 3 presents the actual report result. |
| 5:55-6:10 | `http://localhost:8000/docs`, endpoint list visible. | Member 3 shows the API contract. |
| 6:10-6:35 | pgAdmin at `http://localhost:5050`, `fleet` database and metrics/profitability table visible. | Member 3 confirms PostgreSQL backing without exposing credentials. |
| 6:35-7:15 | Airflow at `http://localhost:8080`, successful DAG graph and both tasks green. | Member 3 explains daily orchestration. |
| 7:15-7:40 | `http://localhost:8000/metrics`, then `/health` response. | Member 3 explains operational metrics and health threshold. |
| 7:40-8:00 | Closing slide or stable architecture/API screen. | Member 3 summarizes the platform and closes. |

**Member 3 speech**

**5:20-5:55, profitability API:**

> This endpoint serves the profitability report for [DATE]. It returns [actual vehicle count] vehicles and identifies [actual unprofitable count] as unprofitable in this run. The input values are randomized, so results can differ between runs.

**5:55-6:10, API docs:**

> FastAPI documents the service endpoints here. Other clients can query current fleet metrics, dated profitability reports, health, and exported operational metrics.

**6:10-6:35, pgAdmin:**

> These API results are stored in PostgreSQL. In pgAdmin we can inspect the serving tables directly, including utilization metrics and daily vehicle profitability. We have prepared this view before recording so no setup credentials are shown.

**6:35-7:15, Airflow:**

> Airflow coordinates the daily reconciliation. The first task creates the date-labelled expense feed, and the second runs the Spark report job. Both tasks are green for [DATE]. The schedule represents one simulated day every five real minutes.

**7:15-7:40, metrics and health:**

> The metrics endpoint exposes request counters, serving row counts, and the age of the latest telemetry. The health rule reports unhealthy when no new metric has been processed for more than 120 seconds. We only claim a healthy-to-unhealthy-to-recovered transition if we captured that test; per-vehicle idle-duration alerts are not implemented.

**7:40-8:00, close:**

> To summarize, the platform demonstrates Kafka ingestion, Spark streaming and batch processing, Parquet retention, PostgreSQL and API serving, Airflow orchestration, and pipeline freshness monitoring. Thank you for watching.

## Handoff and Recording Rules

- Each member speaks only during their assigned 2-minute-40-second section. Start the next member exactly at 2:40 and 5:20; keep transitions brief and included in those blocks.
- Change screens at the times in each member's table. Keep the output still long enough to read; do not scroll while a speaker is explaining a result.
- Have all applications running before recording. Do not wait for a Kafka message, Spark batch, Airflow run, or health timeout on camera.
- Use actual result counts. Replace `[DATE]` with a report date that exists; do not assume today's report exists.
- If you do not have captured health-transition evidence, show the current health response and describe the threshold without saying the transition was verified.
- Record a 10-second OBS test first. Confirm the screen, terminal font size, browser zoom, and all three microphones/speakers are clear.
