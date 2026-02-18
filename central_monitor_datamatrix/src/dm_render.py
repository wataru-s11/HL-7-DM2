from __future__ import annotations

import base64
import logging

from PIL import Image

logger = logging.getLogger(__name__)

try:
    from zint import Symbol, Symbology
except Exception as exc:  # pragma: no cover
    Symbol = None
    Symbology = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def render_datamatrix(data: bytes, size_px: int = 320) -> Image.Image:
    if Symbol is None or Symbology is None:
        message = f"zint-bindings import failed: {_IMPORT_ERROR}"
        logger.error(message)
        raise RuntimeError(message)

    bitmap_len: int | str = "n/a"
    width: int | str = "n/a"
    reason = "unknown"

    try:
        symbol = Symbol()
        symbol.symbology = Symbology.DATAMATRIX

        payload_text = base64.b64encode(data).decode("ascii")
        symbol.encode(payload_text)
        symbol.buffer()

        bm = symbol.bitmap
        w = int(symbol.width)

        bitmap_len = len(bm) if bm is not None else 0
        width = w

        if not bm:
            reason = "empty bitmap from zint.Symbol.bitmap"
            raise ValueError(reason)

        if w <= 0:
            reason = f"invalid symbol width: {w}"
            raise ValueError(reason)

        selected_channels = None
        h = None
        for c in (4, 3, 1):
            row_bytes = w * c
            if row_bytes > 0 and bitmap_len % row_bytes == 0:
                selected_channels = c
                h = bitmap_len // row_bytes
                break

        if selected_channels is None or h is None or h <= 0:
            reason = (
                f"failed to infer channels/height for bitmap; "
                f"candidates=(4,3,1), width={w}, len(bitmap)={bitmap_len}"
            )
            raise ValueError(reason)

        mode = {4: "RGBA", 3: "RGB", 1: "L"}[selected_channels]
        image = Image.frombytes(mode, (w, h), bm)
        return image.convert("L").resize((size_px, size_px), resample=Image.NEAREST)
    except Exception as exc:
        if reason == "unknown":
            reason = str(exc)
        logger.exception("DataMatrix render failed")
        raise RuntimeError(
            f"DataMatrix render failed: {exc} "
            f"(len(bitmap)={bitmap_len}, s.width={width}, reason={reason})"
        ) from exc
