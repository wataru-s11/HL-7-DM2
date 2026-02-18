from __future__ import annotations

import numpy as np

try:
    import zxingcpp
except Exception:  # pragma: no cover
    zxingcpp = None


def _to_bytes(result: object) -> bytes | None:
    raw = getattr(result, "bytes", None)
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)

    text = getattr(result, "text", None)
    if isinstance(text, str):
        return text.encode("utf-8")

    return None


def decode_datamatrix(image_bgr: np.ndarray) -> bytes | None:
    if zxingcpp is None:
        return None

    if image_bgr is None or image_bgr.size == 0:
        return None

    results = zxingcpp.read_barcodes(image_bgr)
    if not results:
        return None

    datamatrix_format = getattr(getattr(zxingcpp, "BarcodeFormat", None), "DataMatrix", None)
    prioritized = next((r for r in results if getattr(r, "format", None) == datamatrix_format), results[0])
    return _to_bytes(prioritized)
