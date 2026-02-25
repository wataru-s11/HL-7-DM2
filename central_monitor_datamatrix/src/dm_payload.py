from __future__ import annotations

import struct
from datetime import datetime, timezone
from typing import Any

MAGIC = b"CMDM"
VERSION = 1
BEDS_6 = ["BED01", "BED02", "BED03", "BED04", "BED05", "BED06"]
PARAMS_20 = [
    "HR",
    "ART_S",
    "ART_D",
    "ART_M",
    "CVP_M",
    "RAP_M",
    "SpO2",
    "TSKIN",
    "TRECT",
    "rRESP",
    "EtCO2",
    "RR",
    "VTe",
    "VTi",
    "Ppeak",
    "PEEP",
    "O2conc",
    "NO",
    "BSR1",
    "BSR2",
]

SCALE_MAP: dict[str, int] = {
    "TSKIN": 10,
    "TRECT": 10,
}

_HEADER_STRUCT = struct.Struct("<4sBBBBq")
_CELL_STRUCT = struct.Struct("<Bi")
_ALLOWED_VITAL_FIELDS = ("value", "unit", "flag", "status")
schema_version = VERSION


class PacketError(ValueError):
    pass


class SeqCounter:
    def __init__(self, start: int = 0) -> None:
        self._value = start

    def next(self) -> int:
        self._value += 1
        return self._value


def _to_epoch_ms(value: Any) -> int:
    if value is None:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
        try:
            return int(float(text))
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _to_numeric(value: Any) -> int | float | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(numeric) if numeric.is_integer() else numeric


def _sanitize_vitals(vitals: dict[str, Any], allowed_params: set[str] | None = None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for vital_code, vital_raw in vitals.items():
        if allowed_params is not None and str(vital_code) not in allowed_params:
            continue
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
    allowed = set(PARAMS_20)
    for bed_id, bed_data in (monitor_cache.get("beds") or {}).items():
        if not isinstance(bed_data, dict):
            continue
        vitals_raw = bed_data.get("vitals")
        if not isinstance(vitals_raw, dict):
            continue
        vitals = _sanitize_vitals(vitals_raw, allowed_params=allowed)
        if vitals:
            beds[str(bed_id)] = {"vitals": vitals}

    return {
        "v": schema_version,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": seq,
        "beds": beds,
    }


def _quantize(param: str, value: Any) -> tuple[int, int]:
    numeric = _to_float(value)
    if numeric is None:
        return 0, 0
    scale = SCALE_MAP.get(param, 1)
    return 1, int(round(numeric * scale))


def _dequantize(param: str, present: int, raw_value: int) -> dict[str, Any]:
    if present == 0:
        return {"present": 0, "value": None}
    scale = SCALE_MAP.get(param, 1)
    if scale == 1:
        return {"present": 1, "value": raw_value}
    return {"present": 1, "value": raw_value / scale}


def build_packet(monitor_cache: dict[str, Any], beds: list[str] | None = None, params: list[str] | None = None) -> bytes:
    beds = beds or BEDS_6
    params = params or PARAMS_20
    if len(beds) > 255 or len(params) > 255:
        raise PacketError("beds/params count must be <= 255")

    timestamp_ms = _to_epoch_ms(monitor_cache.get("ts") or monitor_cache.get("timestamp"))
    header = _HEADER_STRUCT.pack(MAGIC, VERSION, len(beds), len(params), 0, timestamp_ms)

    body = bytearray()
    cache_beds = monitor_cache.get("beds") if isinstance(monitor_cache.get("beds"), dict) else {}

    for bed_id in beds:
        bed_data = cache_beds.get(bed_id)
        bed_present = 1 if isinstance(bed_data, dict) else 0
        body.append(bed_present)

        vitals = bed_data.get("vitals") if isinstance(bed_data, dict) else {}
        vitals = vitals if isinstance(vitals, dict) else {}

        for param in params:
            vital = vitals.get(param)
            value = vital.get("value") if isinstance(vital, dict) else vital
            present, quantized = _quantize(param, value)
            body.extend(_CELL_STRUCT.pack(present, quantized if present else 0))

    return header + bytes(body)


def parse_packet(packet_bytes: bytes, beds: list[str] | None = None, params: list[str] | None = None) -> dict[str, Any]:
    beds = beds or BEDS_6
    params = params or PARAMS_20

    if len(packet_bytes) < _HEADER_STRUCT.size:
        raise PacketError("packet too small")

    magic, version, beds_count, params_count, _reserved, timestamp_ms = _HEADER_STRUCT.unpack_from(packet_bytes, 0)
    if magic != MAGIC:
        raise PacketError(f"invalid magic: {magic!r}")
    if version != VERSION:
        raise PacketError(f"unsupported version: {version}")
    if beds_count != len(beds):
        raise PacketError(f"beds_count mismatch: {beds_count} != {len(beds)}")
    if params_count != len(params):
        raise PacketError(f"params_count mismatch: {params_count} != {len(params)}")

    expected_size = _HEADER_STRUCT.size + beds_count * (1 + params_count * _CELL_STRUCT.size)
    if len(packet_bytes) != expected_size:
        raise PacketError(f"invalid packet size: {len(packet_bytes)} != {expected_size}")

    offset = _HEADER_STRUCT.size
    out_beds: dict[str, Any] = {}

    for bed_id in beds:
        bed_present = packet_bytes[offset]
        offset += 1
        bed_payload: dict[str, Any] = {"bed_present": int(bed_present), "params": {}}

        for param in params:
            present, raw_value = _CELL_STRUCT.unpack_from(packet_bytes, offset)
            offset += _CELL_STRUCT.size
            decoded = _dequantize(param, present, raw_value)
            bed_payload["params"][param] = decoded
            if int(decoded.get("present", 0)) == 1:
                bed_payload[param] = decoded.get("value")

        out_beds[bed_id] = bed_payload

    return {
        "magic": MAGIC.decode("ascii"),
        "version": version,
        "beds_count": beds_count,
        "params_count": params_count,
        "timestamp_ms": timestamp_ms,
        "beds": out_beds,
    }
