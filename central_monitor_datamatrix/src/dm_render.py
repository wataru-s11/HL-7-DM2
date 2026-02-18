from __future__ import annotations

import logging

from PIL import Image

logger = logging.getLogger(__name__)

try:
    from pylibdmtx.pylibdmtx import encode
except Exception as exc:  # pragma: no cover
    encode = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def render_datamatrix(data: bytes, size_px: int = 280) -> Image.Image:
    if encode is None:
        message = f"pylibdmtx import failed: {_IMPORT_ERROR}"
        logger.error(message)
        raise RuntimeError(message)

    try:
        encoded = encode(data)
        image = Image.frombytes("RGB", (encoded.width, encoded.height), encoded.pixels).convert("L")
        return image.resize((size_px, size_px), resample=Image.NEAREST)
    except Exception as exc:
        logger.exception("DataMatrix render failed")
        raise RuntimeError(f"DataMatrix render failed: {exc}") from exc
