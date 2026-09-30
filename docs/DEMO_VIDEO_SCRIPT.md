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
   `http://localhost:5050`, `http://localhost:8000/docs`,
   `http://localhost:8000/metrics/fleet`,
   `http://localhost:8000/reports/profitability/[DATE]`,
   `http://localhost:8000/metrics`, and `http://localhost:8080`.
   Replace `[DATE]` with a date that has a report; do not assume today's report
   exists.
5. Keep the WSL terminals visible or arrange them in a readable layout. Have the
   producer JSON log, Spark `utilization_metrics_batch_written` log, daily CSV,
   and successful Airflow run ready to show. Avoid waiting for a new five-minute
   window during the recording if a fresh successful batch is already available.
   Log in to pgAdmin, set its master password, register the `postgres` server,
   and open the `fleet` database table before recording so no setup dialogs or
   credentials appear in the video.
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

## Timed Screen-by-Screen Recording Guide

Record for about **8 minutes**. Keep the title and architecture on slides only
for the first 50 seconds. At 0:50, switch to the live project and stay on real
terminals and browser pages for the rest of the video. Prepare the browser tabs
and position terminals before recording; do not type long commands on camera.

| Time | What must be on screen | Speaker and narration cue |
| --- | --- | --- |
| 0:00-0:15 | PowerPoint title slide: project name, course, team member names. | Member 3: Introduce the team and project in one sentence. |
| 0:15-0:50 | PowerPoint architecture slide, or the architecture diagram full-screen. Point to the live path and batch path as you explain them. | Member 3: Explain the operator's two questions: live fleet status and daily vehicle profitability. State that continuous telemetry and daily expenses use separate Lambda paths. |
| 0:50-1:10 | VS Code or editor showing `data_generators/streaming_generator.py`; keep only the relevant event fields visible. | Member 1: Say that the simulator represents 20 vehicles and emits telemetry every three seconds. Name a few fields such as vehicle, location, status, speed, and fare. |
| 1:10-1:30 | Producer terminal with recent `telemetry_event_published` JSON lines. | Member 1: Point out the topic, vehicle, partition, and offset to show that events are being published. |
| 1:30-1:55 | Kafka UI at `http://localhost:8081`: `fleet-local` cluster, then `fleet-telemetry` topic and its recent messages/partition view. | Member 1: Explain that Kafka UI lets us inspect messages accepted by the broker. |
| 1:55-2:15 | Open `data_lake/daily_expenses/vehicle_expenses_2026-09-30.csv` in VS Code. Show the header and two or three rows; use the date file from the actual run if different. | Member 1: Describe fuel cost, maintenance cost, distance, and service flag; note that five real minutes represent one simulated day. |
| 2:15-2:45 | Spark terminal showing `spark_streaming_started` and the latest `utilization_metrics_batch_written` log. | Member 2: Explain that Spark consumes Kafka events, validates them, assigns Colombo zones, and writes metric batches. |
| 2:45-3:15 | Briefly show the relevant section of `processing/stream_processor.py`, then return to the Spark log. Avoid scrolling through unrelated code. | Member 2: Mention the two-minute watermark and five-minute event-time windows. Do not pause long on source code. |
| 3:15-3:45 | `http://localhost:8000/metrics/fleet`, with all four zone rows and the latest window visible. | Member 2: Explain the event counts, idle ratio, observed earnings, and that metrics are served from PostgreSQL and retained in Parquet. |
| 3:45-4:15 | Show the generated profitability CSV header and a few rows. | Member 2: Explain that the batch job combines observed trip-fare revenue with daily expenses; clarify that observed fare is a proxy, not settled payment data. |
| 4:15-4:45 | `http://localhost:8000/reports/profitability/[DATE]`, using the date that actually exists. Keep vehicle count and unprofitable summary at the top. | Member 3: State the report date and actual counts visible on screen. Explain that randomized input means counts can vary. |
| 4:45-5:10 | pgAdmin at `http://localhost:5050`, with the `fleet` database and `fleet_utilization_metrics` or `daily_vehicle_profitability` table open. | Member 3: Show that API results are backed by PostgreSQL. Do not show or say the pgAdmin master password. |
| 5:10-6:00 | Airflow at `http://localhost:8080`, DAG graph for `daily_fleet_profitability_reconciliation`, both tasks green. | Member 3: Explain that Airflow creates the dated expense feed and runs the reconciliation task; point to the successful run date. |
| 6:00-6:35 | `http://localhost:8000/metrics`, then `/health` showing the current status. | Member 2: Describe the request counters, serving row count, and telemetry age. Member 3: Explain the configured 120-second freshness threshold. |
| 6:35-7:10 | Optional: show prepared captures of healthy, stale-data 503, and recovered health responses. Do not stop the producer and wait during the recording. | Member 3: Describe the captured transition only if you actually recorded it. Otherwise say the health rule detects stale pipeline data and do not claim a transition was demonstrated. |
| 7:10-8:00 | Return to the architecture slide or keep the live metrics page visible; finish on a stable, readable screen. | Members 2 and 3: Summarize Kafka ingestion, Spark processing, PostgreSQL/API serving, Airflow reconciliation, and the key limitation: per-vehicle idle-duration alerts are not implemented. Thank the audience. |

### Screen-Switch Checklist

Use this order so every transition is deliberate: **title slide → architecture
slide → generator source → producer terminal → Kafka UI → expense CSV → Spark
terminal → stream processor source → live fleet API → profitability CSV → dated
profitability API → pgAdmin → Airflow DAG → API metrics/health → closing
slide**. Leave each output visible long enough to read; avoid rapid scrolling.

Replace `[DATE]` in the profitability URL with a date that has a generated
report. Do not use today's date unless that report exists. Use actual values
visible in the API response rather than memorized example counts.

For the health segment, the configured threshold is 120 seconds. The full
healthy-to-unhealthy-to-recovered demonstration takes several minutes, so use
captures from a completed test rather than stopping the producer during this
recording. If no captures exist, show current health and explain the rule without
claiming the transition was verified.

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
5. Start recording, pause briefly before speaking, follow the screen-switch
   checklist in order, and leave a short pause at each speaker handoff. Avoid
   typing long commands live; prepare the running system and evidence views
   first.
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