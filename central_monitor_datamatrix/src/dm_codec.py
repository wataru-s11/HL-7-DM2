from __future__ import annotations

import copy
import json
import zlib
from typing import Any


_JSON_DUMP_KWARGS = {"sort_keys": True, "separators": (",", ":"), "ensure_ascii": False}


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, **_JSON_DUMP_KWARGS).encode("utf-8")


def compute_crc32_bytes(data_bytes: bytes) -> str:
    crc = zlib.crc32(data_bytes) & 0xFFFFFFFF
    return f"{crc:08X}"


def add_crc32(payload_without_crc: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(payload_without_crc)
    payload.pop("crc32", None)
    payload["crc32"] = compute_crc32_bytes(_canonical_json_bytes(payload))
    return payload


def verify_crc32(payload: dict[str, Any]) -> bool:
    actual_crc = str(payload.get("crc32", "")).upper()
    without_crc = copy.deepcopy(payload)
    without_crc.pop("crc32", None)
    expected_crc = compute_crc32_bytes(_canonical_json_bytes(without_crc))
    return actual_crc == expected_crc


def encode_payload(payload_without_crc: dict[str, Any]) -> bytes:
    payload_with_crc = add_crc32(payload_without_crc)
    return zlib.compress(_canonical_json_bytes(payload_with_crc), level=9)


def decode_payload(blob: bytes) -> dict[str, Any]:
    return json.loads(zlib.decompress(blob).decode("utf-8"))
