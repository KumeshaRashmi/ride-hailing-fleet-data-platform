"""Shared, dependency-free rules used by the Spark processing jobs."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALID_STATUSES = ("idle", "enroute", "on_trip")


def zone_from_coordinates(latitude: float, longitude: float) -> str:
    """Map the simulated Colombo coordinate rectangle into four report zones.

    This deliberately simple grid makes the streaming aggregation explainable in
    a viva.  A production implementation would use a GIS polygon lookup.
    """
    if latitude < 6.85 or latitude > 7.05 or longitude < 79.80 or longitude > 79.95:
        return "outside_service_area"

    north_south = "north" if latitude >= 6.95 else "south"
    east_west = "east" if longitude >= 79.875 else "west"
    return f"colombo_{north_south}_{east_west}"
