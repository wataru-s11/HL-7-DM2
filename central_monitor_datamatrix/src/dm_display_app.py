from __future__ import annotations

import argparse
import json
import logging
import time
import tkinter as tk
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageTk
from screeninfo import get_monitors

import dm_datamatrix

logger = logging.getLogger(__name__)

WINDOW_WIDTH = 420
WINDOW_HEIGHT = 420


def _normalize_cache_metadata(cache: dict[str, Any], fallback_epoch_ms: int, fallback_packet_id: int) -> dict[str, Any]:
    cache = dict(cache)

    epoch_ms = cache.get("epoch_ms")
    if isinstance(epoch_ms, bool) or not isinstance(epoch_ms, int):
        cache["epoch_ms"] = fallback_epoch_ms

    packet_id = cache.get("packet_id")
    if isinstance(packet_id, bool) or not isinstance(packet_id, int):
        cache["packet_id"] = fallback_packet_id

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
        interval_ms: int,
        monitor_index: int,
        margin_right_px: int,
        margin_top_px: int,
        debug: bool = False,
    ) -> None:
        self.out_path = out_path
        self.interval_ms = interval_ms
        self.monitor_index = monitor_index
        self.margin_right_px = margin_right_px
        self.margin_top_px = margin_top_px
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
        self.last_seen_packet_epoch: tuple[int, int] | None = None
        self.last_cache_mtime_ns: int | None = None
        self.no_update_count = 0
        self.read_failures = 0

    def set_cache_path(self, cache_path: Path) -> None:
        self.cache_path = cache_path

    def _refresh_png_if_cache_updated(self) -> None:
        if self.cache_path is None:
            return

        try:
            stat = self.cache_path.stat()
            current_mtime_ns = stat.st_mtime_ns
            if self.last_cache_mtime_ns is not None and current_mtime_ns == self.last_cache_mtime_ns:
                self.no_update_count += 1
                if self.debug and self.no_update_count % 20 == 0:
                    logger.info("debug: cache mtime unchanged count=%d mtime_ns=%d path=%s", self.no_update_count, current_mtime_ns, self.cache_path)
                return

            cache, read_attempt = dm_datamatrix.load_cache_with_retry(self.cache_path)
            self.last_cache_mtime_ns = current_mtime_ns
            self.no_update_count = 0
            self.read_failures = 0

            fallback_epoch_ms = int(time.time() * 1000)
            fallback_packet_id = self.last_seen_packet_epoch[0] + 1 if self.last_seen_packet_epoch else 1
            cache = _normalize_cache_metadata(cache, fallback_epoch_ms=fallback_epoch_ms, fallback_packet_id=fallback_packet_id)
            current_packet_epoch = (int(cache["packet_id"]), int(cache["epoch_ms"]))
            if self.last_seen_packet_epoch == current_packet_epoch:
                if self.debug:
                    logger.info("debug: cache metadata unchanged packet_epoch=%s", current_packet_epoch)
                return

            sizes = dm_datamatrix.generate_datamatrix_png_from_cache_data(cache, self.out_path)
            self.last_seen_packet_epoch = current_packet_epoch

            logger.info(
                "regenerated datamatrix png from cache(read-only): %s (packet=%d, cache_packet_id=%d, epoch_ms=%d, blob=%d, read_attempt=%d)",
                self.out_path,
                sizes["packet_size"],
                current_packet_epoch[0],
                current_packet_epoch[1],
                sizes["blob_size"],
                read_attempt,
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
                resized_image = image.resize((WINDOW_WIDTH, WINDOW_HEIGHT), Image.NEAREST)
            self.photo = ImageTk.PhotoImage(resized_image)
            self.image_label.configure(image=self.photo)
        except FileNotFoundError:
            logger.warning("output image not found: %s", self.out_path)
        except Exception as exc:
            logger.error("failed to load image %s: %s", self.out_path, exc)

        self.root.after(self.interval_ms, self.refresh_image)

    def run(self) -> None:
        self.refresh_image()
        self.root.mainloop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display latest DataMatrix image")
    parser.add_argument(
        "--cache",
        default="generator_cache.json",
        help="Path to generator truth cache (separate from receiver cache)",
    )
    parser.add_argument("--out", default="dataset/dm_latest.png", help="Output PNG path")
    parser.add_argument("--interval-sec", type=float, default=1.0, help="Refresh interval seconds")
    parser.add_argument("--monitor-index", type=int, default=1, help="Monitor index")
    parser.add_argument("--margin-right-px", type=int, default=40, help="Right margin in pixels")
    parser.add_argument("--margin-top-px", type=int, default=0, help="Top margin in pixels")
    parser.add_argument("--debug", action="store_true", help="Show last DM epoch_ms/packet_id log")
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
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    if args.list_monitors:
        list_monitors()
        return 0

    app = DMDisplayApp(
        out_path=Path(args.out),
        interval_ms=max(100, int(args.interval_sec * 1000)),
        monitor_index=args.monitor_index,
        margin_right_px=args.margin_right_px,
        margin_top_px=args.margin_top_px,
        debug=args.debug,
    )
    app.set_cache_path(Path(args.cache))
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
