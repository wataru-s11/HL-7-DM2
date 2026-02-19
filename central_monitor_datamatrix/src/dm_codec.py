from __future__ import annotations

import copy
import json
import struct
import zlib
from typing import Any

_CODEC_HEADER = b"DMC1"
_FOOTER_STRUCT = struct.Struct("<I")
_JSON_DUMP_KWARGS = {"sort_keys": True, "separators": (",", ":"), "ensure_ascii": False}


class CodecError(ValueError):
    pass


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, **_JSON_DUMP_KWARGS).encode("utf-8")


def compute_crc32_bytes(data_bytes: bytes) -> str:
    return f"{_crc32(data_bytes):08X}"


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


def wrap(packet_bytes: bytes, compress_level: int = 6) -> bytes:
    if not isinstance(packet_bytes, (bytes, bytearray)):
        raise CodecError("packet_bytes must be bytes-like")
    if not 1 <= compress_level <= 9:
        raise CodecError("compress_level must be between 1 and 9")

    compressed = zlib.compress(bytes(packet_bytes), level=compress_level)
    crc = _crc32(bytes(packet_bytes))
    return _CODEC_HEADER + compressed + _FOOTER_STRUCT.pack(crc)


def unwrap(blob: bytes) -> bytes:
    if not isinstance(blob, (bytes, bytearray)):
        raise CodecError("blob must be bytes-like")
    blob = bytes(blob)

    min_size = len(_CODEC_HEADER) + _FOOTER_STRUCT.size + 1
    if len(blob) < min_size:
        raise CodecError("blob too small")

    if not blob.startswith(_CODEC_HEADER):
        raise CodecError("invalid blob header")

    crc_expected = _FOOTER_STRUCT.unpack(blob[-_FOOTER_STRUCT.size :])[0]
    compressed = blob[len(_CODEC_HEADER) : -_FOOTER_STRUCT.size]

    try:
        packet = zlib.decompress(compressed)
    except zlib.error as exc:
        raise CodecError(f"zlib decompress failed: {exc}") from exc

    crc_actual = _crc32(packet)
    if crc_actual != crc_expected:
        raise CodecError(
            f"CRC mismatch: expected={crc_expected:08X} actual={crc_actual:08X}"
        )

    return packet
