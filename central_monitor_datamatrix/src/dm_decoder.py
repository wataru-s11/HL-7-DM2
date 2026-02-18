from __future__ import annotations

from typing import Optional, Union

import numpy as np
from PIL import Image

try:
    from pylibdmtx.pylibdmtx import decode
except Exception:  # pragma: no cover
    decode = None


ImageLike = Union[np.ndarray, Image.Image]


def decode_datamatrix_from_image(image: ImageLike) -> Optional[bytes]:
    if decode is None:
        return None

    if isinstance(image, np.ndarray):
        pil_img = Image.fromarray(image)
    else:
        pil_img = image

    results = decode(pil_img)
    if not results:
        return None
    return results[0].data
