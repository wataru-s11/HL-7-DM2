from __future__ import annotations

import argparse
import logging
import tkinter as tk
from pathlib import Path

from PIL import Image, ImageTk
from screeninfo import get_monitors

logger = logging.getLogger(__name__)

WINDOW_WIDTH = 420
WINDOW_HEIGHT = 420


class DMDisplayApp:
    def __init__(self, out_path: Path, interval_ms: int, monitor_index: int) -> None:
        self.out_path = out_path
        self.interval_ms = interval_ms
        self.monitor_index = monitor_index

        monitors = get_monitors()
        if not monitors:
            raise RuntimeError("no monitors detected")
        if monitor_index < 0 or monitor_index >= len(monitors):
            raise ValueError(
                f"monitor-index {monitor_index} is out of range (detected: {len(monitors)})"
            )

        monitor = monitors[monitor_index]
        left = monitor.x + monitor.width - WINDOW_WIDTH
        top = monitor.y

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

        self.root = tk.Tk()
        self.root.title("DataMatrix Display")
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}+{left}+{top}")
        self.root.resizable(False, False)
        self.root.attributes("-topmost", True)

        self.image_label = tk.Label(self.root)
        self.image_label.pack(fill=tk.BOTH, expand=True)
        self.photo = None

    def refresh_image(self) -> None:
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
    parser.add_argument("--cache", required=True, help="Path to monitor_cache.json (reserved for compatibility)")
    parser.add_argument("--out", default="dataset/dm_latest.png", help="Output PNG path")
    parser.add_argument("--interval-sec", type=float, default=1.0, help="Refresh interval seconds")
    parser.add_argument("--monitor-index", type=int, default=1, help="Monitor index (0=primary, 1=display2)")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    app = DMDisplayApp(
        out_path=Path(args.out),
        interval_ms=max(100, int(args.interval_sec * 1000)),
        monitor_index=args.monitor_index,
    )
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
