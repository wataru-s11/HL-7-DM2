from __future__ import annotations

import base64
import logging
import tempfile
from pathlib import Path

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


def _encode_symbol(symbol: Symbol, data: bytes) -> None:
    try:
        symbol.encode(data)
        return
    except Exception:
        pass

    fallback_text = base64.b64encode(data).decode("ascii")
    symbol.encode(fallback_text)


def render_datamatrix(data: bytes, size_px: int = 320) -> Image.Image:
    if Symbol is None or Symbology is None:
        message = f"zint-bindings import failed: {_IMPORT_ERROR}"
        logger.error(message)
        raise RuntimeError(message)

    try:
        symbol = Symbol(Symbology.DATAMATRIX)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            outfile = Path(tmp.name)

        try:
            symbol.outfile = str(outfile)
            _encode_symbol(symbol, data)
            image = Image.open(outfile).convert("L")
            return image.resize((size_px, size_px), resample=Image.NEAREST)
        finally:
            outfile.unlink(missing_ok=True)
    except Exception as exc:
        logger.exception("DataMatrix render failed")
        raise RuntimeError(f"DataMatrix render failed: {exc}") from exc
