import csv
import random
from datetime import date

import pytest

from data_generators.daily_expense_generator import (
    EXPENSE_COLUMNS,
    generate_expense_records,
    write_daily_expense_file,
)
from data_generators.streaming_generator import VEHICLES


def test_daily_expense_feed_has_one_valid_row_per_vehicle():
    records = generate_expense_records(date(2026, 9, 26), random.Random(7))

    assert len(records) == len(VEHICLES)
    assert {record["vehicle_id"] for record in records} == set(VEHICLES)
    assert {record["report_date"] for record in records} == {"2026-09-26"}
    assert all(record["fuel_cost"] > 0 for record in records)
    assert all(record["maintenance_cost"] >= 0 for record in records)
    assert {record["service_flag"] for record in records}.issubset({"required", "not_required"})


def test_daily_file_is_csv_and_protected_against_accidental_replay(tmp_path):
    report_date = date(2026, 9, 26)
    output_file = write_daily_expense_file(tmp_path, report_date, random.Random(11))

    with output_file.open(newline="", encoding="utf-8") as file_handle:
        rows = list(csv.DictReader(file_handle))

    assert output_file.name == "vehicle_expenses_2026-09-26.csv"
    assert len(rows) == len(VEHICLES)
    assert tuple(rows[0].keys()) == EXPENSE_COLUMNS
    with pytest.raises(FileExistsError):
        write_daily_expense_file(tmp_path, report_date)
