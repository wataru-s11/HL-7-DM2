from __future__ import annotations

import argparse
import logging
import tkinter as tk
from pathlib import Path

from PIL import Image, ImageTk
from screeninfo import get_monitors

import dm_datamatrix

logger = logging.getLogger(__name__)

WINDOW_WIDTH = 420
WINDOW_HEIGHT = 420


class DMDisplayApp:
    def __init__(
        self,
        out_path: Path,
        interval_ms: int,
        monitor_index: int,
        margin_right_px: int,
        margin_top_px: int,
    ) -> None:
        self.out_path = out_path
        self.interval_ms = interval_ms
        self.monitor_index = monitor_index
        self.margin_right_px = margin_right_px
        self.margin_top_px = margin_top_px

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
        self.last_cache_mtime_ns: int | None = None

    def set_cache_path(self, cache_path: Path) -> None:
        self.cache_path = cache_path

    def _refresh_png_if_cache_updated(self) -> None:
        if self.cache_path is None:
            return
        try:
            current_mtime_ns = self.cache_path.stat().st_mtime_ns
        except FileNotFoundError:
            logger.warning("cache file not found: %s", self.cache_path)
            return
        except Exception as exc:
            logger.error("failed to inspect cache file %s: %s", self.cache_path, exc)
            return

        if self.last_cache_mtime_ns == current_mtime_ns:
            return

        self.last_cache_mtime_ns = current_mtime_ns
        try:
            sizes = dm_datamatrix.generate_datamatrix_png_from_cache(self.cache_path, self.out_path)
            logger.info(
                "regenerated datamatrix png from cache: %s (packet=%d, blob=%d)",
                self.out_path,
                sizes["packet_size"],
                sizes["blob_size"],
            )
        except Exception as exc:
            logger.error("failed to regenerate datamatrix png: %s", exc)

    def refresh_image(self) -> None:
        self._refresh_png_if_cache_updated()
        try:
            image = Image.open(self.out_path)
            image = image.resize((WINDOW_WIDTH, WINDOW_HEIGHT), Image.NEAREST)
            self.photo = ImageTk.PhotoImage(image)
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
        default="monitor_cache.json",
        help="Path to monitor_cache.json (reserved for compatibility)",
    )
    parser.add_argument("--out", default="dataset/dm_latest.png", help="Output PNG path")
    parser.add_argument("--interval-sec", type=float, default=1.0, help="Refresh interval seconds")
    parser.add_argument("--monitor-index", type=int, default=1, help="Monitor index")
    parser.add_argument("--margin-right-px", type=int, default=40, help="Right margin in pixels")
    parser.add_argument("--margin-top-px", type=int, default=0, help="Top margin in pixels")
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
    )
    app.set_cache_path(Path(args.cache))
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
