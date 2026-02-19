from __future__ import annotations

import argparse
import logging
import tkinter as tk
from pathlib import Path

from PIL import Image, ImageTk

import dm_datamatrix

logger = logging.getLogger(__name__)


class DMDisplayApp:
    def __init__(self, cache_path: Path, out_path: Path, interval_ms: int, topmost: bool, x: int, y: int) -> None:
        self.cache_path = cache_path
        self.out_path = out_path
        self.interval_ms = interval_ms
        self.last_mtime_ns: int | None = None

        self.root = tk.Tk()
        self.root.title("DataMatrix Display")
        self.root.geometry(f"420x420+{x}+{y}")
        self.root.resizable(False, False)
        if topmost:
            self.root.attributes("-topmost", True)

        self.image_label = tk.Label(self.root)
        self.image_label.pack(fill=tk.BOTH, expand=True)
        self.photo = None

    def refresh_if_needed(self) -> None:
        try:
            mtime_ns = self.cache_path.stat().st_mtime_ns
        except FileNotFoundError:
            logger.warning("cache file not found: %s", self.cache_path)
            self.root.after(self.interval_ms, self.refresh_if_needed)
            return

        if self.last_mtime_ns != mtime_ns:
            try:
                dm_datamatrix.generate_datamatrix_png_from_cache(self.cache_path, self.out_path)
                self.last_mtime_ns = mtime_ns
                self._reload_image()
                logger.info("updated DataMatrix display: %s", self.out_path)
            except Exception as exc:
                logger.error("failed to update DataMatrix: %s", exc)

        self.root.after(self.interval_ms, self.refresh_if_needed)

    def _reload_image(self) -> None:
        image = Image.open(self.out_path)
        image = image.resize((420, 420), Image.NEAREST)
        self.photo = ImageTk.PhotoImage(image)
        self.image_label.configure(image=self.photo)

    def run(self) -> None:
        self.refresh_if_needed()
        self.root.mainloop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display latest DataMatrix generated from monitor cache")
    parser.add_argument("--cache", required=True, help="Path to monitor_cache.json")
    parser.add_argument("--out", default="dataset/dm_latest.png", help="Output PNG path")
    parser.add_argument("--interval-sec", type=float, default=1.0, help="Refresh interval seconds")
    parser.add_argument("--topmost", action="store_true", help="Set window always on top")
    parser.add_argument("--x", type=int, default=1400, help="Window X position")
    parser.add_argument("--y", type=int, default=20, help="Window Y position")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    app = DMDisplayApp(
        cache_path=Path(args.cache),
        out_path=Path(args.out),
        interval_ms=max(100, int(args.interval_sec * 1000)),
        topmost=args.topmost,
        x=args.x,
        y=args.y,
    )
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
