from __future__ import annotations

import json
import zlib
from typing import Any, Dict


def _canonical_payload_bytes(payload_without_crc: Dict[str, Any]) -> bytes:
    return json.dumps(payload_without_crc, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def add_crc32(payload_without_crc: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload_without_crc)
    payload.pop("crc32", None)
    crc = zlib.crc32(_canonical_payload_bytes(payload)) & 0xFFFFFFFF
    payload["crc32"] = f"{crc:08x}"
    return payload


def verify_crc32(payload: Dict[str, Any]) -> bool:
    expected = str(payload.get("crc32", "")).lower()
    without = dict(payload)
    without.pop("crc32", None)
    actual = f"{(zlib.crc32(_canonical_payload_bytes(without)) & 0xFFFFFFFF):08x}"
    return expected == actual


def encode_payload(payload: Dict[str, Any]) -> bytes:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return zlib.compress(raw, level=9)


def decode_payload(blob: bytes) -> Dict[str, Any]:
    raw = zlib.decompress(blob)
    return json.loads(raw.decode("utf-8"))
