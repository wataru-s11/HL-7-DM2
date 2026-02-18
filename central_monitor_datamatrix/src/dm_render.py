from __future__ import annotations

from PIL import Image

try:
    from pylibdmtx.pylibdmtx import encode
except Exception as exc:  # pragma: no cover
    encode = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def render_datamatrix(data: bytes, size: int = 280) -> Image.Image:
    if encode is None:
        raise RuntimeError(
            f"pylibdmtx is unavailable: {_IMPORT_ERROR}. See README for libdmtx installation hints."
        )

    encoded = encode(data)
    image = Image.frombytes("RGB", (encoded.width, encoded.height), encoded.pixels)
    image = image.convert("L")
    image = image.resize((size, size), resample=Image.NEAREST)
    return image
