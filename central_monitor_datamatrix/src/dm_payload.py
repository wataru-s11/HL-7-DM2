from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


schema_version = 1


class SeqCounter:
    def __init__(self, start: int = 0) -> None:
        self._value = start

    def next(self) -> int:
        self._value += 1
        return self._value


_ALLOWED_VITAL_FIELDS = ("value", "unit", "flag", "status")


def _to_numeric(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            num = float(text)
        except ValueError:
            return None
        return int(num) if num.is_integer() else num
    return None


def _sanitize_vitals(vitals: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for vital_code, vital_raw in vitals.items():
        if not isinstance(vital_raw, dict):
            continue

        numeric = _to_numeric(vital_raw.get("value"))
        if numeric is None:
            continue

        clean_vital: dict[str, Any] = {"value": numeric}
        for field in _ALLOWED_VITAL_FIELDS[1:]:
            value = vital_raw.get(field)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            clean_vital[field] = value

        result[str(vital_code)] = clean_vital

    return result


def make_payload(monitor_cache: dict[str, Any], seq: int) -> dict[str, Any]:
    beds: dict[str, Any] = {}

    for bed_id, bed_data in (monitor_cache.get("beds") or {}).items():
        if not isinstance(bed_data, dict):
            continue
        vitals_raw = bed_data.get("vitals")
        if not isinstance(vitals_raw, dict):
            continue

        vitals = _sanitize_vitals(vitals_raw)
        if vitals:
            beds[str(bed_id)] = {"vitals": vitals}

    return {
        "v": schema_version,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": seq,
        "beds": beds,
    }
