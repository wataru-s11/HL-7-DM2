from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import mss
import numpy as np
from PIL import Image
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
    parser.add_argument("--monitor-index", type=int, default=None, help="Monitor index for monitor-local ROI coordinates")
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


def capture_image_by_mss(left: int, top: int, width: int, height: int) -> Image.Image:
    monitor = {"left": left, "top": top, "width": width, "height": height}
    with mss.mss() as sct:
        screenshot = sct.grab(monitor)
    return Image.frombytes("RGB", screenshot.size, screenshot.bgra, "raw", "BGRX")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    captures_dir = Path(args.captures_dir)
    captures_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = Path(args.out_jsonl)

    roi_left = args.left
    roi_top = args.top

    if args.monitor_index is not None:
        with mss.mss() as sct:
            monitors = sct.monitors

        if args.monitor_index < 0 or args.monitor_index >= len(monitors):
            raise ValueError(f"invalid --monitor-index={args.monitor_index}; valid range is 0..{len(monitors) - 1}")

        mon = monitors[args.monitor_index]
        roi_left = mon["left"] + args.left
        roi_top = mon["top"] + args.top

        logger.info(
            "monitor_index=%d mon_left=%d mon_top=%d mon_width=%d mon_height=%d",
            args.monitor_index,
            mon["left"],
            mon["top"],
            mon["width"],
            mon["height"],
        )

    logger.info("roi_global=(%d, %d, %d, %d)", roi_left, roi_top, args.width, args.height)
    logger.info("start capture loop interval=%.2fs roi=(%d,%d,%d,%d)", args.interval_sec, roi_left, roi_top, args.width, args.height)

    sequence = 0

    try:
        while True:
            decoded_at_ms = now_ms()
            dt_now = datetime.now()
            # 秒単位ファイル名だと短い間隔で同名上書きが起き、
            # 監視時に「更新されていない」ように見えることがあるため
            # ミリ秒+連番で常に一意なキャプチャ名にする。
            ts = dt_now.strftime("%Y%m%d_%H%M%S")
            image_path = captures_dir / f"{ts}_{dt_now.microsecond // 1000:03d}_{sequence:06d}.png"
            sequence += 1

            record = {
                "timestamp_ms": decoded_at_ms,
                "epoch_ms": None,
                "ts": None,
                "packet_id": None,
                "decoded_at_ms": decoded_at_ms,
                "source_image": str(image_path),
                "decode_ok": False,
                "crc_ok": False,
                "beds": None,
            }

            try:
                if args.monitor_index is None:
                    capture = capture_image(roi_left, roi_top, args.width, args.height)
                else:
                    capture = capture_image_by_mss(roi_left, roi_top, args.width, args.height)
                capture.save(image_path)

                image_rgb = np.array(capture)
                image_bgr = image_rgb[:, :, ::-1]
                payload = dm_datamatrix.decode_payload_from_bgr_image(image_bgr)

                dm_epoch_ms = payload.get("epoch_ms") if payload.get("epoch_ms") is not None else payload.get("timestamp_ms")
                record["epoch_ms"] = dm_epoch_ms
                record["timestamp_ms"] = dm_epoch_ms if dm_epoch_ms is not None else decoded_at_ms
                record["ts"] = payload.get("ts")
                record["packet_id"] = payload.get("packet_id")
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
