from __future__ import annotations

import argparse
import json
import logging
import time
import tkinter as tk
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageTk
from screeninfo import get_monitors

import dm_datamatrix
import paths as run_paths

logger = logging.getLogger(__name__)

WINDOW_WIDTH = 420
WINDOW_HEIGHT = 420


class CacheUpdateMode:
    GENERATOR = "generator"
    RECEIVER = "receiver"


def _to_epoch_ms(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _ensure_cache_metadata(cache: dict[str, Any], fallback_epoch_ms: int) -> dict[str, Any]:
    cache = dict(cache)

    epoch_ms = _to_epoch_ms(cache.get("epoch_ms"))
    if epoch_ms is None:
        epoch_ms = _to_epoch_ms(cache.get("timestamp_ms"))
    if epoch_ms is None and isinstance(cache.get("ts"), str):
        ts_text = cache["ts"].strip().replace("Z", "+00:00")
        try:
            epoch_ms = int(datetime.fromisoformat(ts_text).timestamp() * 1000)
        except ValueError:
            epoch_ms = None
    cache["epoch_ms"] = epoch_ms if epoch_ms is not None else fallback_epoch_ms

    packet_id = cache.get("packet_id")
    if isinstance(packet_id, bool) or not isinstance(packet_id, int):
        cache["packet_id"] = None

    ts = cache.get("ts")
    if not isinstance(ts, str) or not ts.strip():
        cache["ts"] = datetime.fromtimestamp(cache["epoch_ms"] / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")

    source = cache.get("source")
    if not isinstance(source, str) or not source.strip():
        cache["source"] = "unknown"

    beds = cache.get("beds")
    if not isinstance(beds, dict):
        cache["beds"] = {}

    return cache


class DMDisplayApp:
    def __init__(
        self,
        out_path: Path,
        poll_ms: int,
        monitor_index: int,
        margin_right_px: int,
        margin_top_px: int,
        cache_type: str,
        debug: bool = False,
    ) -> None:
        self.out_path = out_path
        self.poll_ms = poll_ms
        self.monitor_index = monitor_index
        self.margin_right_px = margin_right_px
        self.margin_top_px = margin_top_px
        self.cache_type = cache_type
        self.debug = debug

        monitors = get_monitors()
        if not monitors:
            raise RuntimeError("no monitors detected")
        if monitor_index < 0 or monitor_index >= len(monitors):
            raise ValueError(
                f"monitor-index {monitor_index} is out of range (detected: {len(monitors)})"
            )

        monitor = monitors[monitor_index]
        left = monitor.x + monitor.width - WINDOW_WIDTH - margin_right_px
        top = monitor.y + margin_top_px

        logger.info("monitor_index=%d", monitor_index)
        logger.info(
            "monitor_geometry=x=%d, y=%d, width=%d, height=%d",
            monitor.x,
            monitor.y,
            monitor.width,
            monitor.height,
        )
        logger.info(
            "dm_window_geometry=left=%d, top=%d, width=%d, height=%d",
            left,
            top,
            WINDOW_WIDTH,
            WINDOW_HEIGHT,
        )
        logger.info("margins: right=%d, top=%d", margin_right_px, margin_top_px)

        self.root = tk.Tk()
        self.root.title("DataMatrix Display")
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}+{left}+{top}")
        self.root.resizable(False, False)
        self.root.attributes("-topmost", True)

        self.image_label = tk.Label(self.root)
        self.image_label.pack(fill=tk.BOTH, expand=True)
        self.photo = None
        self.cache_path: Path | None = None
        self.last_seen_packet_id: int | None = None
        self.warned_missing_packet_id = False
        self.no_update_count = 0
        self.read_failures = 0
        self.tick_count = 0

    def set_cache_path(self, cache_path: Path) -> None:
        self.cache_path = cache_path

    def _refresh_png_if_cache_updated(self) -> None:
        if self.cache_path is None:
            return

        try:
            self.tick_count += 1
            cache, read_attempt = dm_datamatrix.load_cache_with_retry(self.cache_path)
            self.read_failures = 0

            fallback_epoch_ms = int(time.time() * 1000)
            cache = _ensure_cache_metadata(cache, fallback_epoch_ms=fallback_epoch_ms)

            current_packet_id = cache.get("packet_id") if isinstance(cache.get("packet_id"), int) else None

            if current_packet_id is None and not self.warned_missing_packet_id:
                self.warned_missing_packet_id = True
                logger.warning(
                    "cache has no packet_id. regeneration is skipped until packet_id appears: cache=%s cache_type=%s",
                    self.cache_path,
                    self.cache_type,
                )
                return

            if self.last_seen_packet_id == current_packet_id:
                self.no_update_count += 1
                if self.tick_count % 10 == 0:
                    logger.info(
                        "heartbeat: tick=%d unchanged_count=%d last_packet_id=%s read_attempt=%d",
                        self.tick_count,
                        self.no_update_count,
                        self.last_seen_packet_id,
                        read_attempt,
                    )
                if self.debug:
                    logger.debug(
                        "packet_idが変わらなかったので再生成しなかった: packet_id=%s cache_type=%s",
                        current_packet_id,
                        self.cache_type,
                    )
                return

            sizes = dm_datamatrix.generate_datamatrix_png_from_cache_data(cache, self.out_path)
            self.last_seen_packet_id = current_packet_id
            self.no_update_count = 0

            logger.info(
                "regenerated datamatrix png from cache(read-only): %s (packet=%d, cache_packet_id=%s, blob=%d, read_attempt=%d, cache_type=%s)",
                self.out_path,
                sizes["packet_size"],
                current_packet_id,
                sizes["blob_size"],
                read_attempt,
                self.cache_type,
            )
        except FileNotFoundError:
            if self.debug:
                logger.info("debug: cache file not found yet: %s", self.cache_path)
        except (json.JSONDecodeError, OSError) as exc:
            self.read_failures += 1
            logger.warning(
                "cache read retry exhausted after dm_datamatrix.load_cache_with_retry(retries=3): cache=%s error=%s failures=%d",
                self.cache_path,
                exc,
                self.read_failures,
            )
        except Exception as exc:
            self.read_failures += 1
            logger.warning("failed to regenerate datamatrix png; keeping previous png: %s (failures=%d)", exc, self.read_failures)

    def refresh_image(self) -> None:
        self._refresh_png_if_cache_updated()
        try:
            with Image.open(self.out_path) as image:
                image.load()
                resized_image = image.resize((WINDOW_WIDTH, WINDOW_HEIGHT), Image.NEAREST)
                self.photo = ImageTk.PhotoImage(resized_image)
            self.image_label.configure(image=self.photo)
        except FileNotFoundError:
            logger.warning("output image not found: %s", self.out_path)
        except Exception as exc:
            logger.error("failed to load image %s: %s", self.out_path, exc)

        self.root.after(self.poll_ms, self.refresh_image)

    def run(self) -> None:
        self.refresh_image()
        self.root.mainloop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display latest DataMatrix image")
    parser.add_argument(
        "--cache",
        default="generator_cache.json",
        help="Path to generator_cache.json (truth cache). Avoid receiver/monitor cache for display updates.",
    )
    parser.add_argument(
        "--cache-type",
        choices=[CacheUpdateMode.GENERATOR, CacheUpdateMode.RECEIVER],
        default=CacheUpdateMode.GENERATOR,
        help="Cache update strategy: generator=packet_id+epoch_ms, receiver=mtime/epoch_ms fallback",
    )
    parser.add_argument("--run-dir", help="Output directory. Default: dataset/YYYYMMDD")
    parser.add_argument("--out", default=None, help="Output PNG path (relative path is resolved under --run-dir)")
    parser.add_argument("--poll-sec", type=float, default=1.0, help="Cache polling interval seconds")
    parser.add_argument("--interval-sec", type=float, default=None, help="Deprecated alias of --poll-sec")
    parser.add_argument("--monitor-index", type=int, default=1, help="Monitor index")
    parser.add_argument("--margin-right-px", type=int, default=40, help="Right margin in pixels")
    parser.add_argument("--margin-top-px", type=int, default=0, help="Top margin in pixels")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs for update detection details")
    parser.add_argument(
        "--list-monitors",
        action="store_true",
        help="List monitor geometries via mss and exit",
    )
    return parser.parse_args()


def list_monitors() -> None:
    import mss

    with mss.mss() as sct:
        monitors = sct.monitors
    if not monitors:
        print("no monitors detected")
        return

    for i, monitor in enumerate(monitors):
        print(
            f"monitor{i}: left={monitor['left']}, top={monitor['top']}, "
            f"width={monitor['width']}, height={monitor['height']}"
        )


def main() -> int:
    logging.basicConfig(level=logging.DEBUG if "--debug" in sys.argv else logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    # Windows troubleshooting:
    # 1) python dm_display_app.py --cache generator_cache.json --out dataset\dm_latest.png --poll-sec 1 --monitor-index 2 --debug
    # 2) In another terminal: Get-FileHash dataset\dm_latest.png (run repeatedly every few seconds)
    # 3) Confirm packet_id / epoch_ms keep increasing in logs and file hash changes over time.

    if args.list_monitors:
        list_monitors()
        return 0

    poll_sec = args.poll_sec if args.interval_sec is None else args.interval_sec
    if args.interval_sec is not None:
        logger.warning("--interval-sec is deprecated. Use --poll-sec instead.")

    run_dir = run_paths.resolve_run_dir(args.run_dir)
    logger.info("run_dir=%s", run_dir)
    out_path = run_paths.resolve_in_run_dir(args.out, run_dir) or (run_dir / "dm_latest.png")

    app = DMDisplayApp(
        out_path=out_path,
        poll_ms=max(100, int(poll_sec * 1000)),
        monitor_index=args.monitor_index,
        margin_right_px=args.margin_right_px,
        margin_top_px=args.margin_top_px,
        cache_type=args.cache_type,
        debug=args.debug,
    )
    app.set_cache_path(Path(args.cache))
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
