"""Airflow DAG that produces the daily feed and reconciles fleet profitability."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PROJECT_ROOT = Path("/opt/airflow/project")
DATA_LAKE = PROJECT_ROOT / "data_lake"


@dag(
    dag_id="daily_fleet_profitability_reconciliation",
    description="Generate one daily expenses file and reconcile fleet profitability.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["fleet", "lambda", "batch"],
)
def daily_fleet_profitability_reconciliation():
    @task
    def create_expense_feed() -> str:
        from data_generators.daily_expense_generator import write_daily_expense_file

        context = get_current_context()
        report_date = context["logical_date"].date()
        output = write_daily_expense_file(
            DATA_LAKE / "daily_expenses",
            report_date,
            overwrite=True,
        )
        return str(output)

    @task
    def reconcile_daily_report(expense_file: str) -> str:
        import pyspark

        report_date = Path(expense_file).stem.removeprefix("vehicle_expenses_")
        spark_submit = Path(pyspark.__file__).resolve().parent / "bin" / "spark-submit"
        command = [
            str(spark_submit),
            "--master",
            "local[*]",
            str(PROJECT_ROOT / "processing" / "profitability_job.py"),
            "--expenses",
            expense_file,
            "--raw-events-path",
            str(DATA_LAKE / "clean_telemetry"),
            "--output-root",
            str(DATA_LAKE / "profitability_reports"),
            "--csv-output-root",
            str(PROJECT_ROOT / "reports" / "profitability"),
            "--report-date",
            report_date,
            "--postgres-dsn",
            os.environ["FLEET_DATABASE_URL"],
        ]
        subprocess.run(command, cwd=PROJECT_ROOT, check=True)
        return f"Daily profitability report loaded for {report_date}"

    reconcile_daily_report(create_expense_feed())


daily_fleet_profitability_reconciliation()
