from __future__ import annotations

import argparse
import json
import logging
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


def _validate_cache_metadata(cache: dict[str, Any]) -> dict[str, Any]:
    epoch_ms = cache.get("epoch_ms")
    packet_id = cache.get("packet_id")
    ts = cache.get("ts")

    if isinstance(epoch_ms, bool):
        raise ValueError("cache field epoch_ms must be int, got bool")
    if not isinstance(epoch_ms, int):
        raise ValueError(f"cache field epoch_ms is required as int (got={type(epoch_ms).__name__})")

    if isinstance(packet_id, bool):
        raise ValueError("cache field packet_id must be int, got bool")
    if not isinstance(packet_id, int):
        raise ValueError(f"cache field packet_id is required as int (got={type(packet_id).__name__})")

    if not isinstance(ts, str) or not ts.strip():
        cache = dict(cache)
        cache["ts"] = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")

    source = cache.get("source")
    if not isinstance(source, str) or not source.strip():
        cache = dict(cache)
        cache["source"] = "unknown"

    beds = cache.get("beds")
    if not isinstance(beds, dict):
        cache = dict(cache)
        cache["beds"] = {}

    return cache


def _resolve_snapshot_path(out_path: Path, epoch_ms: int) -> Path:
    day = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y%m%d")
    return out_path.parent / day / "cache_snapshots.jsonl"


def _read_last_packet_id(snapshot_path: Path) -> int | None:
    if not snapshot_path.exists():
        return None
    try:
        with snapshot_path.open("r", encoding="utf-8") as f:
            last_line = ""
            for line in f:
                if line.strip():
                    last_line = line
        if not last_line:
            return None
        payload = json.loads(last_line)
        raw = payload.get("packet_id")
        return int(raw) if raw is not None else None
    except Exception:
        logger.warning("failed to read last packet_id from %s", snapshot_path)
        return None


def _append_cache_snapshot(snapshot_path: Path, cache: dict[str, Any]) -> bool:
    packet_id = cache.get("packet_id")
    epoch_ms = cache.get("epoch_ms")
    ts = cache.get("ts")

    if not isinstance(packet_id, int) or not isinstance(epoch_ms, int):
        return False

    last_packet_id = _read_last_packet_id(snapshot_path)
    if last_packet_id == packet_id:
        return False

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "epoch_ms": epoch_ms,
        "packet_id": packet_id,
        "ts": ts,
        "beds": cache.get("beds", {}),
    }
    try:
        with snapshot_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return True
    except OSError as exc:
        logger.warning("failed to append cache snapshot: %s", exc)
        return False


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

    def set_cache_path(self, cache_path: Path) -> None:
        self.cache_path = cache_path

    def _refresh_png_if_cache_updated(self) -> None:
        if self.cache_path is None:
            return

        try:
            cache, read_attempt = dm_datamatrix.load_cache_with_retry(self.cache_path)
            cache = _validate_cache_metadata(cache)
            current_packet_epoch = (int(cache["packet_id"]), int(cache["epoch_ms"]))
            if self.last_seen_packet_epoch == current_packet_epoch:
                return

            sizes = dm_datamatrix.generate_datamatrix_png_from_cache_data(cache, self.out_path)
            snapshot_path = _resolve_snapshot_path(self.out_path, int(cache["epoch_ms"]))
            appended = _append_cache_snapshot(snapshot_path, cache)
            self.last_seen_packet_epoch = current_packet_epoch

            logger.info(
                "regenerated datamatrix png from cache: %s (packet=%d, cache_packet_id=%d, epoch_ms=%d, blob=%d, read_attempt=%d, snapshot_appended=%s)",
                self.out_path,
                sizes["packet_size"],
                current_packet_epoch[0],
                current_packet_epoch[1],
                sizes["blob_size"],
                read_attempt,
                appended,
            )
            if self.debug:
                logger.info(
                    "debug: dm payload key epoch_ms=%s packet_id=%s",
                    cache.get("epoch_ms"),
                    cache.get("packet_id"),
                )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "cache read retry exhausted after dm_datamatrix.load_cache_with_retry(retries=3): cache=%s error=%s",
                self.cache_path,
                exc,
            )
        except Exception as exc:
            logger.warning("failed to regenerate datamatrix png; keeping previous png: %s", exc)

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
