"""Create the once-per-simulated-day vehicle-expense feed.

The assignment permits time compression.  This generator treats five minutes as
one simulated day by default and produces one complete CSV file per interval.
Use ``--once`` in a demo or when Airflow triggers an individual daily run.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

from data_generators.streaming_generator import VEHICLES


EXPENSE_COLUMNS: Sequence[str] = (
    "report_date",
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
)
DEFAULT_SIMULATED_DAY_SECONDS = 300


def generate_expense_record(
    vehicle_id: str,
    report_date: date,
    random_source: random.Random | None = None,
) -> dict[str, str | float | int]:
    """Return one realistic expense record for a vehicle and logical day."""
    rng = random_source or random
    distance_covered = round(rng.uniform(15, 220), 2)
    maintenance_cost = round(rng.uniform(0, 2200), 2)
    service_flag = "required" if rng.random() < 0.12 else "not_required"

    # A service day is deliberately more expensive so the reconciliation has
    # both profitable and potentially unprofitable vehicles to identify.
    if service_flag == "required":
        maintenance_cost = round(maintenance_cost + rng.uniform(3500, 9000), 2)

    return {
        "report_date": report_date.isoformat(),
        "vehicle_id": vehicle_id,
        "fuel_cost": round(distance_covered * rng.uniform(35, 55), 2),
        "maintenance_cost": maintenance_cost,
        "distance_covered": distance_covered,
        "service_flag": service_flag,
    }


def generate_expense_records(
    report_date: date,
    random_source: random.Random | None = None,
) -> list[dict[str, str | float | int]]:
    """Generate exactly one expense record for every simulated fleet vehicle."""
    return [
        generate_expense_record(vehicle_id, report_date, random_source)
        for vehicle_id in VEHICLES
    ]


def write_daily_expense_file(
    output_dir: Path | str,
    report_date: date,
    random_source: random.Random | None = None,
    overwrite: bool = False,
) -> Path:
    """Atomically publish the daily CSV and return its final path.

    A consumer therefore never observes a partially-written source file.  By
    default an existing logical-day file is protected from accidental replay.
    """
    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    target = destination_dir / f"vehicle_expenses_{report_date.isoformat()}.csv"

    if target.exists() and not overwrite:
        raise FileExistsError(
            f"Daily expense feed already exists for {report_date}: {target}. "
            "Use --overwrite only for an intentional re-run."
        )

    temporary_file = target.with_suffix(".csv.tmp")
    with temporary_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=EXPENSE_COLUMNS)
        writer.writeheader()
        writer.writerows(generate_expense_records(report_date, random_source))
    temporary_file.replace(target)
    return target


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate the daily vehicle-expense feed.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_lake/daily_expenses"),
        help="Directory where daily CSV files are dropped.",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date.today(),
        help="First logical date to generate (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--simulated-day-seconds",
        type=int,
        default=DEFAULT_SIMULATED_DAY_SECONDS,
        help="Real seconds that represent one simulated day (default: 300).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Create only one daily file, then exit.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Permit replacement of an existing file for the same logical day.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.simulated_day_seconds <= 0:
        raise ValueError("--simulated-day-seconds must be positive")

    report_date = args.start_date
    while True:
        file_path = write_daily_expense_file(
            args.output_dir,
            report_date,
            overwrite=args.overwrite,
        )
        print(
            json.dumps(
                {
                    "event": "daily_expense_file_created",
                    "report_date": report_date.isoformat(),
                    "path": str(file_path),
                    "record_count": len(VEHICLES),
                }
            )
        )

        if args.once:
            return

        time.sleep(args.simulated_day_seconds)
        report_date += timedelta(days=1)


if __name__ == "__main__":
    main()
