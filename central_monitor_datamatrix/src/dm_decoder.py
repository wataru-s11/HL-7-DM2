from __future__ import annotations

import cv2
import numpy as np
import zxingcpp


def decode_datamatrix(image_bgr: np.ndarray) -> bytes | None:
    if image_bgr is None or image_bgr.size == 0:
        return None

    try:
        if image_bgr.ndim == 3:
            image_gray = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)
        else:
            image_gray = image_bgr

        results = zxingcpp.read_barcodes(image_gray)
    except Exception:
        return None

    if not results:
        return None

    selected = None
    for result in results:
        if result.format == zxingcpp.BarcodeFormat.DataMatrix:
            selected = result
            break

    if selected is None:
        selected = results[0]

    if getattr(selected, "bytes", None):
        return bytes(selected.bytes)

    if selected.text:
        try:
            return selected.text.encode("utf-8")
        except Exception:
            return None

    return None
