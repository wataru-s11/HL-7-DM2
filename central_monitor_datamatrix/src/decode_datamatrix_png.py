from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import cv2

import dm_datamatrix

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decode DataMatrix PNG and restore JSON payload.")
    parser.add_argument("--image", required=True, help="Path to DataMatrix image file")
    parser.add_argument("--out-json", help="Optional output JSON file path")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    image_path = Path(args.image)
    image = cv2.imread(str(image_path))
    if image is None:
        logger.error("failed to read image: %s", image_path)
        return 1

    try:
        payload = dm_datamatrix.decode_payload_from_bgr_image(image)
        logger.info("CRC OK")
    except Exception as exc:
        logger.error("failed to decode payload: %s", exc)
        return 1

    output_json = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out_json:
        out = Path(args.out_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(output_json + "\n", encoding="utf-8")
    else:
        sys.stdout.write(output_json)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
