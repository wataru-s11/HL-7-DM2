from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from PIL import Image

from dm_codec import decode_payload, verify_crc32
from dm_decoder import decode_datamatrix_from_image

logger = logging.getLogger(__name__)


def _iter_images(image: str | None, image_dir: str | None, latest_n: int) -> Iterable[Path]:
    if image:
        yield Path(image)
        return

    if not image_dir:
        return

    files: List[Path] = []
    for ext in ("*.png", "*.jpg", "*.jpeg", "*.bmp"):
        files.extend(Path(image_dir).glob(ext))
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[:latest_n]:
        yield p


def _right_bottom_roi(img: Image.Image, roi_size: int) -> Image.Image:
    w, h = img.size
    left = max(0, w - roi_size)
    top = max(0, h - roi_size)
    return img.crop((left, top, w, h))


def _append_jsonl(out_path: Path, payload: dict, source: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "captured_at": datetime.utcnow().isoformat() + "Z",
        "source": source,
        "payload": payload,
    }
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Decode DataMatrix from screenshot(s) and append JSONL")
    ap.add_argument("--image", help="single image file path")
    ap.add_argument("--image-dir", help="directory to scan (latest N images)")
    ap.add_argument("--latest-n", type=int, default=5)
    ap.add_argument("--roi-size", type=int, default=420)
    ap.add_argument("--output-root", default="dataset")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    day = datetime.now().strftime("%Y%m%d")
    out_path = Path(args.output_root) / day / "dm_results.jsonl"

    for path in _iter_images(args.image, args.image_dir, args.latest_n):
        if not path.exists():
            logger.warning("image not found: %s", path)
            continue
        try:
            img = Image.open(path).convert("RGB")
            roi = _right_bottom_roi(img, args.roi_size)
            blob = decode_datamatrix_from_image(roi)
            if blob is None:
                logger.warning("decode failed: %s", path)
                continue
            payload = decode_payload(blob)
            if not verify_crc32(payload):
                logger.warning("crc mismatch: %s", path)
                continue
            _append_jsonl(out_path, payload, source=str(path))
            logger.info("decoded+saved: %s", path)
        except Exception as exc:
            logger.exception("processing failed for %s: %s", path, exc)


if __name__ == "__main__":
    main()
