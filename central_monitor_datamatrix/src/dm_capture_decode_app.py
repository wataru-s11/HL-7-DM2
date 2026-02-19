from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
from PIL import ImageGrab

import dm_datamatrix

logger = logging.getLogger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture ROI and decode DataMatrix on interval")
    parser.add_argument("--interval-sec", type=float, default=10.0, help="Capture interval seconds")
    parser.add_argument("--left", type=int, required=True, help="ROI left")
    parser.add_argument("--top", type=int, required=True, help="ROI top")
    parser.add_argument("--width", type=int, required=True, help="ROI width")
    parser.add_argument("--height", type=int, required=True, help="ROI height")
    parser.add_argument("--out-jsonl", default="dataset/decoded_results.jsonl", help="Output JSONL path")
    parser.add_argument("--captures-dir", default="dataset/captures", help="Captured PNG directory")
    return parser.parse_args()


def append_jsonl(out_path: Path, record: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def capture_image(left: int, top: int, width: int, height: int):
    bbox = (left, top, left + width, top + height)
    return ImageGrab.grab(bbox=bbox)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    captures_dir = Path(args.captures_dir)
    captures_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = Path(args.out_jsonl)

    logger.info("start capture loop interval=%.2fs roi=(%d,%d,%d,%d)", args.interval_sec, args.left, args.top, args.width, args.height)

    try:
        while True:
            decoded_at_ms = now_ms()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            image_path = captures_dir / f"{ts}.png"

            record = {
                "timestamp_ms": None,
                "decoded_at_ms": decoded_at_ms,
                "source_image": str(image_path),
                "decode_ok": False,
                "crc_ok": False,
                "beds": None,
            }

            try:
                capture = capture_image(args.left, args.top, args.width, args.height)
                capture.save(image_path)

                image_rgb = np.array(capture)
                image_bgr = image_rgb[:, :, ::-1]
                payload = dm_datamatrix.decode_payload_from_bgr_image(image_bgr)

                record["timestamp_ms"] = payload.get("timestamp_ms")
                record["beds"] = payload.get("beds")
                record["decode_ok"] = True
                record["crc_ok"] = True
                logger.info("decoded ok: %s", image_path)
            except Exception as exc:
                record["error"] = str(exc)
                logger.warning("decode failed: %s (%s)", image_path, exc)

            append_jsonl(out_jsonl, record)
            time.sleep(max(0.1, args.interval_sec))
    except KeyboardInterrupt:
        logger.info("stopped by keyboard interrupt")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
