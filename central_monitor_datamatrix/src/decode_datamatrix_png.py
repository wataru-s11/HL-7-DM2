from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
from pathlib import Path

import cv2

import dm_codec
import dm_decoder


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decode DataMatrix PNG and verify payload CRC32."
    )
    parser.add_argument("--image", required=True, help="Path to DataMatrix image file")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    image_path = Path(args.image)
    image = cv2.imread(str(image_path))
    if image is None:
        logger.error("failed to read image: %s", image_path)
        return 1

    blob_text_bytes = dm_decoder.decode_datamatrix(image)
    if blob_text_bytes is None:
        logger.error("failed to decode DataMatrix blob from image: %s", image_path)
        return 1

    try:
        blob = base64.b64decode(blob_text_bytes.decode("utf-8"))
        payload = dm_codec.decode_payload(blob)
    except Exception as exc:
        logger.error("failed to decode payload: %s", exc)
        return 1

    if not dm_codec.verify_crc32(payload):
        logger.error("CRC32 verification failed")
        return 2

    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
