# Final Report Completion Guide

Use this guide to finalize `FINAL_REPORT.md` against the mini-project rubric.
The report draft is already structured; finish the evidence and team-specific
fields using the final demonstrated run. Do not include invented values or
screenshots of a planned-but-unrun behavior.

## 1. Complete the cover information

In `FINAL_REPORT.md`, replace:

- `[three members' names and student numbers]` with the approved team names and IDs;
- the repository placeholder with the submitted repository URL; and
- the demonstration date with the date of the final recorded run.

Review the individual contribution table with all team members. Adjust it to
match actual ownership and speaking roles in `DEMO_VIDEO_SCRIPT.md`.

## 2. Capture and place real evidence

Create `docs/evidence/` and save clear, uncropped screenshots from the final
run. Use filenames that describe the evidence. Replace each `[INSERT REAL
SCREENSHOT: ...]` marker in `FINAL_REPORT.md` with a Markdown image link, for
example `![FastAPI docs and live fleet metrics](evidence/api-fleet-metrics.png)`.

Capture these items:

| Evidence | What must be visible | Suggested filename |
| --- | --- | --- |
| API documentation and fleet endpoint | FastAPI docs or the `/metrics/fleet` JSON response with the latest window and zones. | `api-fleet-metrics.png` |
| Successful Airflow DAG | `daily_fleet_profitability_reconciliation` with both `create_expense_feed` and `reconcile_daily_report` successful. | `airflow-success.png` |
| Pipeline alert and recovery | `/health` healthy, stale-data 503 unhealthy, then healthy recovery. Show the status and age/threshold fields. | `health-alert-recovery.png` |
| Spark processing evidence | A real `utilization_metrics_batch_written` log from the final stream run. | `spark-metrics-batch.png` |
| Daily reconciliation output | CSV rows plus API report summary for the same report date, showing 20 vehicles and the unprofitable summary. | `profitability-report.png` |

If a single screenshot cannot make both the unhealthy transition and recovery
readable, use separate files and link both in the relevant report section. Do not
paste terminal output into an image editor as a substitute for a real capture.

## 3. Finalize the results section

The report records one demonstrated run on `2026-09-28`: the profitability
endpoint returned 20 vehicles and 10 unprofitable vehicles. A later run can
produce different counts because expenses and telemetry are randomized. Check
the saved final-run API response and update the table in Section 7 if its values
differ.

For the final run, record:

- the report date used by both the expense CSV and profitability endpoint;
- vehicle count and unprofitable count from the API response;
- latest event-time window, zones, event counts, and earnings from
  `/metrics/fleet`; and
- the actual health transition statuses and recorded timestamps.

Use UTC timestamps as returned by the API. Explain any result that looks
unexpected. Do not claim exact profits, fares, or counts based on an earlier
randomized run if the final screenshots show different data.

## 4. Keep claims aligned with implementation

The project implements a pipeline-wide no-data health rule: it reports
unhealthy when no streaming metric has been processed for more than 60 seconds.
It does **not** currently alert when an individual vehicle has remained idle for
a configured duration. The report and video script disclose this gap. Do not
claim that per-vehicle idle alert behavior was demonstrated.

The batch calculation uses the maximum observed fare from `on_trip` telemetry
for each `(vehicle_id, trip_id)` as a simplified trip-revenue proxy. It is not
settled payment data or an immutable completed-trip ledger. Keep that distinction
in the results and limitations discussion.

## 5. Check the rubric coverage

Before exporting the report, confirm each rubric area has both an explanation
and evidence:

| Rubric area | Report location/evidence |
| --- | --- |
| Lambda or Kappa choice and rationale | Sections 2-3 and architecture diagram. |
| Two simulated sources and compressed clock | Sections 1 and 4; daily CSV screenshot. |
| Kafka, meaningful stream processing, batch reconciliation | Sections 3-4; producer and Spark evidence. |
| Queryable storage and consolidated output | Section 5; API responses and report file. |
| Airflow orchestration | Section 6; successful DAG screenshot. |
| Structured logs, metrics, and an alert | Section 6; Spark log and health/metrics evidence. |
| Limitations and honest scope | Sections 8-9; state that individual idle-duration alert is not implemented. |
| Contributions and reproducibility | Sections 10-11; actual member names, student numbers, repo URL, and run guide. |

The grading brief's vehicle-specific prolonged-idle alert appears under
suggested outputs. The minimum observability requirement is met by the
implemented pipeline-level freshness alert; disclose the distinction rather
than presenting the suggestion as completed work.

## 6. Export and inspect the PDF

Choose one export method and confirm image links render before submission.

**VS Code Markdown export:** Install a trusted Markdown-to-PDF extension from
the VS Code Marketplace, open `FINAL_REPORT.md`, use the extension's PDF export
command, and save the PDF as `docs/FINAL_REPORT.pdf`. Check its print settings
for A4 paper, readable margins, and images scaled to page width.

**Pandoc export:** If Pandoc and a LaTeX PDF engine such as XeLaTeX are already
installed, run this from a WSL Bash terminal at the repository root:

```bash
pandoc docs/FINAL_REPORT.md \
  --standalone \
  --toc \
  --number-sections \
  --resource-path=docs:docs/evidence \
  --pdf-engine=xelatex \
  -o docs/FINAL_REPORT.pdf
```

Open the generated PDF and inspect every page. Confirm the architecture diagram,
tables, screenshots, code identifiers, page breaks, and headings are readable.
The assignment checklist suggests a target length of 8-15 pages; prioritize
readability and complete evidence over adding filler to reach a page count.

## 7. Final submission cross-check

- [ ] Cover fields and contribution statements are complete and approved by the team.
- [ ] Every screenshot marker has been replaced with genuine final-run evidence.
- [ ] Report dates and counts match the saved API/CSV evidence.
- [ ] The idle-alert limitation and fare-proxy limitation are stated accurately.
- [ ] `README.md`, `FINAL_REPORT.md`, `DEMO_VIDEO_SCRIPT.md`, and the code agree.
- [ ] PDF opens and all figures/tables are legible.
- [ ] Demo video is 5-10 minutes, includes all three speakers, and matches the evidence.
- [ ] No secrets, local virtualenvs, generated runtime data, or database dumps are included in the submission.
