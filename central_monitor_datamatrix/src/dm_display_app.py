from __future__ import annotations

import argparse
import json
import logging
import os
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


def _now_epoch_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _load_packet_id(state_path: Path) -> int:
    if not state_path.exists():
        return 0
    try:
        return int(state_path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        logger.warning("failed to read packet id state: %s", state_path)
        return 0


def _save_packet_id(state_path: Path, packet_id: int) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_name(f"{state_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        f.write(str(packet_id))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, state_path)


def _write_cache(cache_path: Path, cache: dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_name(f"{cache_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, cache_path)


def _ensure_cache_metadata(cache: dict[str, Any], cache_path: Path) -> tuple[dict[str, Any], bool]:
    changed = False

    epoch_ms = cache.get("epoch_ms")
    if not isinstance(epoch_ms, int):
        try:
            epoch_ms = int(float(epoch_ms))
        except (TypeError, ValueError):
            epoch_ms = _now_epoch_ms()
        cache["epoch_ms"] = epoch_ms
        changed = True

    ts = cache.get("ts")
    if not isinstance(ts, str) or not ts.strip():
        cache["ts"] = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")
        changed = True

    packet_id = cache.get("packet_id")
    if isinstance(packet_id, bool):
        packet_id = None
    try:
        packet_id_int = int(packet_id)
    except (TypeError, ValueError):
        packet_id_int = None

    if packet_id_int is None:
        state_path = cache_path.with_suffix(".packet_id")
        packet_id_int = _load_packet_id(state_path) + 1
        cache["packet_id"] = packet_id_int
        _save_packet_id(state_path, packet_id_int)
        changed = True
    else:
        cache["packet_id"] = packet_id_int

    source = cache.get("source")
    if not isinstance(source, str) or not source.strip():
        cache["source"] = "generator"
        changed = True

    beds = cache.get("beds")
    if not isinstance(beds, dict):
        cache["beds"] = {}
        changed = True

    return cache, changed


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
            cache, read_attempt = dm_datamatrix.load_cache_with_retry(self.cache_path)
            cache, changed = _ensure_cache_metadata(cache, self.cache_path)
            if changed:
                _write_cache(self.cache_path, cache)

            sizes = dm_datamatrix.generate_datamatrix_png_from_cache_data(cache, self.out_path)
            snapshot_path = _resolve_snapshot_path(self.out_path, int(cache["epoch_ms"]))
            appended = _append_cache_snapshot(snapshot_path, cache)

            logger.info(
                "regenerated datamatrix png from cache: %s (packet=%d, blob=%d, read_attempt=%d, snapshot_appended=%s)",
                self.out_path,
                sizes["packet_size"],
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
            logger.warning("cache read retry exhausted; skip refresh and keep previous png: %s", exc)
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
        default="monitor_cache.json",
        help="Path to monitor_cache.json (reserved for compatibility)",
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
