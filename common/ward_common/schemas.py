"""Event contracts shared by producers and consumers.

The vitals simulator produces events that follow `VITALS_FIELDS`; the Spark
job (Step 4) validates them against the same `VITAL_RANGES`, so producer and
consumer can never disagree about what "valid" means.

The ranges are *physiological plausibility* limits (what a real sensor could
report for a living patient), not clinical normal ranges. A value outside them
is treated as a sensor/transmission fault and dead-lettered; a value inside
them but abnormal (e.g. SpO2 85) is a genuine clinical signal and must reach
the alerting logic.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

VITALS_SCHEMA_VERSION = 1

# (min, max) inclusive plausibility limits.
VITAL_RANGES: dict[str, tuple[float, float]] = {
    "heart_rate": (20, 250),     # beats/min
    "spo2": (50, 100),           # %
    "systolic_bp": (50, 260),    # mmHg
    "diastolic_bp": (20, 160),   # mmHg
    "temperature": (30.0, 43.0), # °C
}

VITALS_FIELDS: tuple[str, ...] = (
    "schema_version",
    "event_id",
    "patient_id",
    "heart_rate",
    "spo2",
    "systolic_bp",
    "diastolic_bp",
    "temperature",
    "timestamp",    # simulated event time (ISO-8601, UTC) — used for windows
    "produced_at",  # real wall-clock time the monitor took the reading — for latency metrics
)

PATIENT_ID_PATTERN = re.compile(r"^P\d{3}$")


def patient_id(index: int) -> str:
    """1 -> 'P001'."""
    return f"P{index:03d}"


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def validate_vitals_event(event: Any, known_patients: set[str] | None = None) -> list[str]:
    """Return a list of error codes; an empty list means the event is valid.

    Error codes are stable strings because they are stored as the dead-letter
    `error_reason` and counted in metrics.
    """
    if not isinstance(event, dict):
        return ["not_an_object"]

    errors: list[str] = []
    for name in VITALS_FIELDS:
        if event.get(name) is None:
            errors.append(f"missing:{name}")

    pid = event.get("patient_id")
    if pid is not None:
        if not isinstance(pid, str) or not PATIENT_ID_PATTERN.match(pid):
            errors.append("bad_patient_id")
        elif known_patients is not None and pid not in known_patients:
            errors.append("unknown_patient")

    for name, (low, high) in VITAL_RANGES.items():
        value = event.get(name)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"type:{name}")
        elif not low <= value <= high:
            errors.append(f"range:{name}")

    sbp, dbp = event.get("systolic_bp"), event.get("diastolic_bp")
    if isinstance(sbp, (int, float)) and isinstance(dbp, (int, float)) and dbp >= sbp:
        errors.append("bp_inverted")

    for name in ("timestamp", "produced_at"):
        if event.get(name) is not None and _parse_ts(event[name]) is None:
            errors.append(f"bad_timestamp:{name}")

    return errors
