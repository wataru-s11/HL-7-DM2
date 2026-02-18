from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import cv2

from dm_codec import decode_payload, verify_crc32
from dm_decoder import decode_datamatrix

logger = logging.getLogger(__name__)


def parse_roi(roi_text: str) -> tuple[int, int, int, int]:
    parts = [p.strip() for p in roi_text.split(",")]
    if len(parts) != 4:
        raise ValueError("ROI must be x,y,w,h")
    x, y, w, h = map(int, parts)
    if w <= 0 or h <= 0:
        raise ValueError("ROI width/height must be > 0")
    return x, y, w, h


def auto_roi(image) -> tuple[int, int, int, int]:
    h, w = image.shape[:2]
    roi_w = max(1, int(w * 0.25))
    roi_h = max(1, int(h * 0.25))
    return w - roi_w, h - roi_h, roi_w, roi_h


def crop_roi(image, roi: tuple[int, int, int, int]):
    x, y, w, h = roi
    ih, iw = image.shape[:2]
    x = max(0, min(x, iw - 1))
    y = max(0, min(y, ih - 1))
    w = max(1, min(w, iw - x))
    h = max(1, min(h, ih - y))
    return image[y : y + h, x : x + w]


def iter_input_images(input_path: Path, last: int):
    if input_path.is_file():
        yield input_path
        return

    files = sorted(
        [p for p in input_path.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".bmp"}],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for file_path in files[:last]:
        yield file_path


def append_jsonl(out_path: Path, source: Path, payload: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "crc_ok": True,
        "payload": payload,
    }
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Decode DataMatrix from screenshots and append JSONL")
    parser.add_argument("--input", required=True, help="PNG path or folder")
    parser.add_argument("--out", required=True, help="JSONL output path")
    parser.add_argument("--roi", help="x,y,w,h")
    parser.add_argument("--last", type=int, default=10, help="latest N images when --input is folder")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    input_path = Path(args.input)
    out_path = Path(args.out)
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    explicit_roi = parse_roi(args.roi) if args.roi else None

    for image_path in iter_input_images(input_path, args.last):
        image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
        if image is None:
            logger.warning("decode失敗 (image load failed): %s", image_path)
            continue

        roi = explicit_roi or auto_roi(image)
        roi_img = crop_roi(image, roi)

        try:
            blob = decode_datamatrix(roi_img)
            if blob is None:
                logger.warning("decode失敗: %s", image_path)
                continue

            payload = decode_payload(blob)
            if not verify_crc32(payload):
                logger.warning("crc不一致: %s", image_path)
                continue

            append_jsonl(out_path, image_path, payload)
            logger.info("saved: %s", image_path)
        except Exception as exc:
            logger.exception("例外: %s (%s)", image_path, exc)


if __name__ == "__main__":
    main()
