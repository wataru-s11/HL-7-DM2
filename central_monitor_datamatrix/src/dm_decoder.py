from __future__ import annotations

import cv2
import numpy as np


try:
    _BARCODE_DETECTOR = cv2.barcode_BarcodeDetector()
except Exception:  # pragma: no cover
    _BARCODE_DETECTOR = None


def decode_datamatrix(image_bgr: np.ndarray) -> bytes | None:
    if _BARCODE_DETECTOR is None:
        return None

    if image_bgr is None or image_bgr.size == 0:
        return None

    try:
        ok, decoded_info, decoded_types, _ = _BARCODE_DETECTOR.detectAndDecode(image_bgr)
    except Exception:
        return None

    if not ok:
        return None

    for text, code_type in zip(decoded_info, decoded_types):
        if code_type != "DATAMATRIX":
            continue
        if not text:
            continue
        try:
            return text.encode("utf-8")
        except Exception:
            return None

    return None
