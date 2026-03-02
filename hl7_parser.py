from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


def _parse_obx_segments(segments: List[str]) -> Dict[str, Dict[str, Any]]:
    vitals: Dict[str, Dict[str, Any]] = {}
    for seg in segments:
        fields = seg.split("|")
        if len(fields) < 7 or fields[0] != "OBX":
            continue

        obs_id = fields[3].split("^")[0].strip() or "UNKNOWN"
        value_raw = fields[5].strip()
        unit = fields[6].strip() if len(fields) > 6 else ""
        flag = fields[8].strip() if len(fields) > 8 else ""

        try:
            value = float(value_raw)
        except ValueError:
            value = value_raw

        vitals[obs_id] = {
            "value": value,
            "unit": unit,
            "flag": flag,
        }
    return vitals


def parse_hl7_message(raw_message: str) -> Dict[str, Any]:
    """Parse minimal HL7 v2 ORU-like message into monitor cache schema."""
    segments = [s for s in raw_message.replace("\r\n", "\r").split("\r") if s.strip()]
    bed = "UNKNOWN"
    patient = {}

    for seg in segments:
        fields = seg.split("|")
        if fields[0] == "PV1" and len(fields) > 3:
            pv1_parts = fields[3].split("^")
            bed = pv1_parts[2] if len(pv1_parts) > 2 and pv1_parts[2] else fields[3] or "UNKNOWN"
        if fields[0] == "PID":
            patient = {
                "patient_id": fields[3] if len(fields) > 3 else "",
                "name": fields[5] if len(fields) > 5 else "",
                "dob": fields[7] if len(fields) > 7 else "",
            }

    vitals = _parse_obx_segments(segments)

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "bed": bed,
        "patient": patient,
        "vitals": vitals,
    }
