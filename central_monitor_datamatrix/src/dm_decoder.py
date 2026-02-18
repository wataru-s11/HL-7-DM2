from __future__ import annotations

from typing import Any

import cv2
import numpy as np

try:
    from pylibdmtx.pylibdmtx import decode
except Exception:  # pragma: no cover
    decode = None


def _result_area(result: Any) -> int:
    rect = getattr(result, "rect", None)
    if rect is None:
        return 0
    width = getattr(rect, "width", 0)
    height = getattr(rect, "height", 0)
    return int(width) * int(height)


def decode_datamatrix(image_bgr: np.ndarray) -> bytes | None:
    if decode is None:
        return None

    if image_bgr is None or image_bgr.size == 0:
        return None

    gray = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)
    results = decode(gray)
    if not results:
        return None

    best = max(results, key=_result_area)
    return getattr(best, "data", None)
