# Demo Video Script and Recording Guide

**Target duration:** 7-9 minutes  
**Speakers:** Three team members  
**Scenario:** Ride-Hailing Fleet Operations  
**DAG:** `daily_fleet_profitability_reconciliation`

Replace each `[bracketed item]` with the team's actual names or values. Use
screenshots and results from the final run; do not invent output or show
credentials, API secrets, or unrelated personal information.

## Before recording

1. Confirm Docker's Kafka, Kafka UI, PostgreSQL, API, and Airflow containers
   are running.
2. Confirm the WSL producer and Spark streaming job are running and producing
   recent output. `/health` should currently return `healthy`.
3. Confirm the API returns live metrics and a profitability report for the
   date you will show. Confirm the Airflow DAG has a successful run with both
   tasks green.
4. Prepare browser tabs for `http://localhost:8081`,
   `http://localhost:8000/docs`,
   `http://localhost:8000/metrics/fleet`,
   `http://localhost:8000/reports/profitability/[DATE]`,
   `http://localhost:8000/metrics`, and `http://localhost:8080`.
   Replace `[DATE]` with the actual report date.
5. Keep the WSL terminals visible or arrange them in a readable layout. Have the
   producer JSON log, Spark `utilization_metrics_batch_written` log, daily CSV,
   and successful Airflow run ready to show. Avoid waiting for a new five-minute
   window during the recording if a fresh successful batch is already available.
6. Agree on speaker handoffs and pronounce technology names consistently:
   Kafka, Spark Structured Streaming, Parquet, PostgreSQL, FastAPI, and Airflow.
7. Close unrelated windows, silence notifications, hide credentials, set the
   display to 1920x1080 if practical, and perform a short microphone test.

## Suggested speaker ownership

| Speaker | Contribution to present | Demo sections |
| --- | --- | --- |
| Member 1: `[name]` | Telemetry simulator, Kafka producer/topic setup, and source tests. | Introduction and streaming ingestion. |
| Member 2: `[name]` | Daily expense source, Spark streaming/batch transforms, and Parquet processing. | Processing and profitability calculation. |
| Member 3: `[name]` | API/PostgreSQL serving, Docker Compose/Airflow, observability, final integration. | Architecture overview, API, orchestration, health, and wrap-up. |

Change the assignments to reflect the work each person actually performed and
the contribution table in `FINAL_REPORT.md`.

## Timed narration and screen cues

### 0:00-0:50 | Opening and business problem | Member 3

**Show:** Project title, then the architecture diagram in
`docs/architecture.svg` or the architecture section of the report.

**Say:**

> Hello, we are [Member 1], [Member 2], and [Member 3]. This is our Applied Big
> Data Engineering mini-project: a ride-hailing fleet data platform. The
> operator needs two answers: what is happening across the fleet now, and which
> vehicles are unprofitable after daily fuel and maintenance costs are included.
> We chose a Lambda architecture because telemetry is continuous while partner
> expenses arrive as a daily file. The live path and the reconciliation path
> have different processing and latency needs.

### 0:50-2:00 | Sources and Kafka ingestion | Member 1

**Show:** `data_generators/streaming_generator.py` briefly, then the producer
terminal with published JSON records. Open Kafka UI at `http://localhost:8081`,
select the `fleet-local` cluster and `fleet-telemetry` topic, and show recent
messages plus the topic's partition/offset view.

**Say:**

> The continuous source simulates 20 vehicles and emits a telemetry event every
> three seconds. Events contain trip, driver, and vehicle identifiers, Colombo
> coordinates, speed, status, fare, and a UTC timestamp. The producer publishes
> JSON to the `fleet-telemetry` Kafka topic. Here the producer log shows the
> topic, vehicle, partition, and offset, so we can confirm events are being
> accepted by Kafka rather than only generated locally. Kafka UI gives us a
> browser view of the topic and its messages; our producer still connects to
> the broker through the host address, while the UI connects over Docker's
> internal network.
>
> The second source is a daily expense CSV. It contains one row per vehicle with
> fuel cost, maintenance cost, distance, and a service flag. One simulated day
> is five real minutes, which makes the daily path demonstrable in a lab session.

### 2:00-4:05 | Spark transformations and batch logic | Member 2

**Show:** Spark terminal. Point to `spark_streaming_started` and a recent
`utilization_metrics_batch_written` event. Then show
`/metrics/fleet`. Briefly show `processing/stream_processor.py` and
`processing/profitability_job.py` only if readable without scrolling through
large code blocks.

**Say:**

> Spark Structured Streaming reads Kafka using an explicit event schema. It
> filters invalid identifiers, timestamps, statuses, speeds, and fares, then
> maps coordinates into four Colombo zones. A two-minute watermark handles late
> events, and five-minute event-time windows calculate event counts, idle and
> active counts, observed earnings, average speed, and idle ratio.
>
> This API response is the latest window, grouped by zone. The live aggregate is
> upserted to PostgreSQL and retained in Parquet. For the daily report, the batch
> job filters telemetry to a report date, groups observations by vehicle and
> trip, and uses each trip's maximum observed on-trip fare as a simplified
> revenue proxy. It left-joins expenses so vehicles with costs and no qualifying
> trip observations still appear in the report. This is simulated observed fare,
> not a settlement or audited payment feed.

### 4:05-5:25 | Daily output and API | Member 3

**Show:** The generated CSV header and a few rows, then the profitability API
response for the same date. Keep the full response available, but scroll to the
vehicle count and unprofitable summary first.

**Say:**

> The daily reconciliation writes Parquet and a human-readable CSV, then
> upserts one row per report date and vehicle into PostgreSQL. The API returns
> all 20 simulated vehicles for this run and summarizes which ones are
> unprofitable. The exact costs and profitability counts can change because the
> inputs are randomized. The `/docs` page exposes the endpoints, while
> `/metrics/fleet` and the dated profitability endpoint provide the business
> outputs to other clients.

**Show:** `http://localhost:8000/docs`, then
`http://localhost:8000/reports/profitability/[DATE]`.

### 5:25-6:35 | Airflow orchestration | Member 3

**Show:** Airflow DAG graph for a successful run, with
`create_expense_feed` and `reconcile_daily_report` green. Optionally show the
Spark report-created log from the successful Airflow execution.

**Say:**

> Airflow coordinates the daily path. The first task creates the date-labelled
> expense feed; the second runs Spark reconciliation and loads the report. This
> graph shows both tasks succeeded for [DATE]. The five-minute schedule matches
> the compressed simulated day. We avoid launching overlapping manual and
> scheduled reconciliations because both write the same report-date output.

### 6:35-7:45 | Observability and conclusion | Members 2 and 3

**Show:** First `/metrics`, then a healthy `/health` response. If the team has
captured the full alert demonstration, show the stale-data 503 and subsequent
recovery captures. Do not stop the producer during the main take unless enough
time is available to wait for the 121-second threshold and recovery.

**Member 2 says:**

> Processing components emit structured JSON logs, and the API exports request
> counters, serving row counts, and the age of the latest telemetry metric.

**Member 3 says:**

> The pipeline health rule becomes unhealthy when no metric has been processed
> for more than 120 seconds. We verified the transition from healthy to a 503
> unhealthy response after stopping the producer, then restarted the producer
> and confirmed health recovered after Spark wrote a new metric batch. This is a
> pipeline freshness alert. A separate alert for one vehicle remaining idle for
> a configured duration is not implemented in this version and is a possible
> extension.
>
> In summary, our demo shows Kafka ingestion, Spark streaming and batch
> processing, queryable API outputs, Airflow orchestration, Parquet retention,
> and pipeline health monitoring. Thank you.

**If you did not capture the alert/recovery:** Do not say “we verified” in the
spoken line. Instead say, “The implemented rule reports unhealthy after 120
seconds without a processed metric; our final report records the health checks
we completed.”

## Recording with OBS Studio

1. Install and open OBS Studio on the computer running the browser and
   terminals. Create a scene named `Fleet Platform Demo`.
2. Add a **Display Capture** source for the monitor showing the demo. Add an
   **Audio Input Capture** source for the microphone. Use window capture only if
   it reliably includes browser and terminal windows.
3. In **Settings > Video**, choose 1920x1080 canvas/output if supported and 30
   FPS. In **Settings > Output > Recording**, choose MP4 (or MKV and remux to
   MP4 afterward) and a location with sufficient free disk space.
4. Set the microphone level so speech is clear and does not peak into clipping.
   Record a 10-second test, play it back, and confirm both screen and audio are
   present.
5. Start recording, pause briefly before speaking, follow the timed outline,
   and leave a short pause at each speaker handoff. Avoid typing long commands
   live; prepare the running system and evidence views first.
6. Stop recording, watch the entire file once, and check that the DAG status,
   API responses, logs, and speech are readable. Rename it using a clear pattern
   such as `Ride-Hailing-Fleet-Data-Platform-Demo.mp4`.

Windows Game Bar can be used for a quick capture with `Win+Alt+R`, but test
microphone recording and window capture before relying on it for the final video.

## Final recording checklist

- [ ] Video is 5-10 minutes and all three members speak.
- [ ] Architecture choice and reasons are explained.
- [ ] Both source types and the five-minute simulated day are shown.
- [ ] Kafka producer output and Spark processing evidence are visible.
- [ ] Kafka UI shows the `fleet-telemetry` topic and recent messages.
- [ ] Live zone metrics and the same-date profitability report are demonstrated.
- [ ] The successful Airflow DAG is shown.
- [ ] Health/metrics are shown, and alert claims match captured evidence.
- [ ] The optional vehicle-specific idle alert is described as unimplemented.
- [ ] Audio is understandable and no secrets or private data are exposed.
- [ ] The recorded file plays from beginning to end.
d