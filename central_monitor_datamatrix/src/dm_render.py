from __future__ import annotations

import base64
import logging
import math

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
    height: int | str = "n/a"
    head_hex = "n/a"
    reason = "unknown"

    try:
        symbol = Symbol()
        symbol.symbology = Symbology.DATAMATRIX

        payload_text = base64.b64encode(data).decode("ascii")
        symbol.encode(payload_text)
        symbol.buffer()

        bm = symbol.bitmap

        bitmap_len = len(bm) if bm is not None else 0
        if bm:
            head_hex = bytes(bm[:16]).hex()

        if not bm:
            reason = "empty bitmap from zint.Symbol.bitmap"
            raise ValueError(reason)

        w = None
        h = None

        bitmap_w = getattr(symbol, "bitmap_width", None)
        bitmap_h = getattr(symbol, "bitmap_height", None)
        if bitmap_w is not None and bitmap_h is not None:
            w = int(bitmap_w)
            h = int(bitmap_h)
        else:
            symbol_w = getattr(symbol, "width", None)
            symbol_h = getattr(symbol, "rows", None)
            if symbol_h is None:
                symbol_h = getattr(symbol, "height", None)

            if symbol_w is not None and symbol_h is not None:
                w = int(symbol_w)
                h = int(symbol_h)

        width = w if w is not None else "n/a"
        height = h if h is not None else "n/a"

        if w is None or h is None or w <= 0 or h <= 0:
            reason = (
                "failed to infer bitmap dimensions from symbol attributes "
                f"(bitmap_width/bitmap_height or width/rows(height)); got w={w}, h={h}"
            )
            raise ValueError(reason)

        if bitmap_len == w * h * 4:
            image = Image.frombytes("RGBA", (w, h), bytes(bm))
        elif bitmap_len == w * h * 3:
            image = Image.frombytes("RGB", (w, h), bytes(bm))
        elif bitmap_len == w * h:
            image = Image.frombytes("L", (w, h), bytes(bm))
        elif bitmap_len == math.ceil(w / 8) * h:
            row_bytes = math.ceil(w / 8)
            unpacked = bytearray(w * h)
            src = bytes(bm)
            for row in range(h):
                row_start = row * row_bytes
                for col in range(w):
                    byte_idx = row_start + (col // 8)
                    bit_shift = 7 - (col % 8)
                    bit = (src[byte_idx] >> bit_shift) & 0x01
                    unpacked[row * w + col] = 255 if bit else 0
            image = Image.frombytes("L", (w, h), bytes(unpacked))
        else:
            reason = (
                "failed to infer bitmap format from dimensions "
                f"(w={w}, h={h}, len(bitmap)={bitmap_len})"
            )
            raise ValueError(reason)

        return image.convert("L").resize((size_px, size_px), resample=Image.NEAREST)
    except Exception as exc:
        if reason == "unknown":
            reason = str(exc)
        logger.exception(
            "DataMatrix render failed: w=%s h=%s len(bitmap)=%s head16=%s",
            width,
            height,
            bitmap_len,
            head_hex,
        )
        raise RuntimeError(
            f"DataMatrix render failed: {exc} "
            f"(w={width}, h={height}, len(bitmap)={bitmap_len}, head16={head_hex}, reason={reason})"
        ) from exc
