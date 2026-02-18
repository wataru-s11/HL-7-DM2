from __future__ import annotations

import base64
import logging
import os
import tempfile

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

    temp_path: str | None = None
    try:
        symbol = Symbol()
        symbol.symbology = Symbology.DATAMATRIX

        payload_text = base64.b64encode(data).decode("ascii")

        fd, temp_path = tempfile.mkstemp(suffix=".png")
        os.close(fd)

        symbol.outfile = temp_path
        symbol.encode(payload_text)

        with Image.open(temp_path) as image:
            rendered = image.convert("L")
            return rendered.resize((size_px, size_px), resample=Image.NEAREST)
    except Exception as exc:
        logger.exception("DataMatrix render failed")
        raise RuntimeError(f"DataMatrix render failed: {exc}") from exc
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
