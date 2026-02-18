from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from dm_codec import add_crc32


_ALLOWED_VITAL_KEYS = {"value", "unit", "flag"}


def _sanitize_vitals(vitals: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    clean: Dict[str, Dict[str, Any]] = {}
    for code, item in vitals.items():
        if not isinstance(item, dict):
            continue
        minimal = {k: item[k] for k in _ALLOWED_VITAL_KEYS if k in item}
        if minimal:
            clean[code] = minimal
    return clean


def make_payload(monitor_cache_dict: Dict[str, Any], seq: int, schema_version: int = 1) -> Dict[str, Any]:
    """Build PHI-free payload with vitals only and CRC32."""
    beds = monitor_cache_dict.get("beds", {}) if isinstance(monitor_cache_dict, dict) else {}
    safe_beds: Dict[str, Dict[str, Any]] = {}

    for bed_id, bed_data in beds.items():
        if not isinstance(bed_data, dict):
            continue
        safe_beds[bed_id] = {
            "ts": bed_data.get("ts"),
            "vitals": _sanitize_vitals(bed_data.get("vitals", {})),
        }

    payload_without_crc = {
        "v": schema_version,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": seq,
        "beds": safe_beds,
    }
    return add_crc32(payload_without_crc)
