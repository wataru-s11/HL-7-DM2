from __future__ import annotations

import base64
import io
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

_IMAGE_MAGIC_PREFIXES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",  # JPEG
    b"BM",  # BMP
    b"GIF87a",
    b"GIF89a",
    b"II*\x00",  # TIFF (little-endian)
    b"MM\x00*",  # TIFF (big-endian)
    b"RIFF",  # WEBP starts with RIFF....WEBP
)


def _looks_like_image_bytes(blob: bytes) -> bool:
    if not blob:
        return False
    if any(blob.startswith(prefix) for prefix in _IMAGE_MAGIC_PREFIXES):
        return True
    return len(blob) >= 12 and blob.startswith(b"RIFF") and blob[8:12] == b"WEBP"


def _try_bytes_attr(symbol: Symbol, attr_name: str) -> bytes:
    value = getattr(symbol, attr_name, None)
    if value is None:
        return b""
    try:
        return bytes(value)
    except Exception:
        return b""


def _render_from_bitmap(symbol: Symbol) -> Image.Image:
    bm = getattr(symbol, "bitmap", None)
    bitmap_len = len(bm) if bm is not None else 0

    if not bm:
        raise ValueError("empty bitmap from zint.Symbol.bitmap")

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

    if w is None or h is None or w <= 0 or h <= 0:
        raise ValueError(
            "failed to infer bitmap dimensions from symbol attributes "
            f"(bitmap_width/bitmap_height or width/rows(height)); got w={w}, h={h}"
        )

    if bitmap_len == w * h * 4:
        return Image.frombytes("RGBA", (w, h), bytes(bm))
    if bitmap_len == w * h * 3:
        return Image.frombytes("RGB", (w, h), bytes(bm))
    if bitmap_len == w * h:
        return Image.frombytes("L", (w, h), bytes(bm))
    if bitmap_len == math.ceil(w / 8) * h:
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
        return Image.frombytes("L", (w, h), bytes(unpacked))

    raise ValueError(
        "failed to infer bitmap format from dimensions "
        f"(w={w}, h={h}, len(bitmap)={bitmap_len})"
    )


def render_datamatrix(data: bytes, size_px: int = 320) -> Image.Image:
    if Symbol is None or Symbology is None:
        message = f"zint-bindings import failed: {_IMPORT_ERROR}"
        logger.error(message)
        raise RuntimeError(message)

    img_bytes = b""
    bitmap_len: int | str = "n/a"
    head_hex = "n/a"
    reason = "unknown"

    try:
        symbol = Symbol()
        symbol.symbology = Symbology.DATAMATRIX

        payload_text = base64.b64encode(data).decode("ascii")
        symbol.encode(payload_text)
        symbol.buffer()

        # Preferred path: zint-bindings memfile image bytes.
        img_bytes = _try_bytes_attr(symbol, "memfile")

        # Fallback to vector/file-like buffers that may also contain image bytes.
        if not img_bytes:
            for attr_name in ("buffer_vector", "buffered", "outfile_data"):
                img_bytes = _try_bytes_attr(symbol, attr_name)
                if img_bytes:
                    break

        if img_bytes and _looks_like_image_bytes(img_bytes):
            with io.BytesIO(img_bytes) as bio:
                image = Image.open(bio)
                image.load()
            return image.convert("L").resize((size_px, size_px), resample=Image.NEAREST)

        # Final fallback: legacy bitmap path.
        bitmap = getattr(symbol, "bitmap", None)
        bitmap_len = len(bitmap) if bitmap is not None else 0
        if bitmap:
            head_hex = bytes(bitmap[:16]).hex()

        if not img_bytes:
            reason = "image bytes unavailable from memfile/buffer_vector/buffered/outfile_data"
        elif not _looks_like_image_bytes(img_bytes):
            reason = "image bytes did not match known image magic"
            head_hex = img_bytes[:16].hex() if img_bytes else head_hex

        fallback_image = _render_from_bitmap(symbol)
        return fallback_image.convert("L").resize((size_px, size_px), resample=Image.NEAREST)
    except Exception as exc:
        img_len = len(img_bytes)
        if img_bytes:
            head_hex = img_bytes[:16].hex()
        if reason == "unknown":
            reason = str(exc)
        logger.exception(
            "DataMatrix render failed: len(img_bytes)=%s head16=%s len(bitmap)=%s",
            img_len,
            head_hex,
            bitmap_len,
        )
        raise RuntimeError(
            f"DataMatrix render failed: {exc} "
            f"(len(img_bytes)={img_len}, head16={head_hex}, len(bitmap)={bitmap_len}, reason={reason})"
        ) from exc
