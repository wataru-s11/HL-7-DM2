from __future__ import annotations

import argparse
import json
import logging
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from PIL import ImageTk

from dm_codec import encode_payload
from dm_payload import make_payload
from dm_render import render_datamatrix

logger = logging.getLogger(__name__)


class MonitorApp:
    def __init__(self, root: tk.Tk, cache_path: Path, interval_ms: int = 1000) -> None:
        self.root = root
        self.cache_path = cache_path
        self.interval_ms = interval_ms
        self.seq = 0
        self.dm_photo = None

        self.root.title("Central Monitor + DataMatrix")
        self.root.geometry("1200x750")
        self.root.configure(bg="black")

        self.text = tk.Text(root, bg="black", fg="#ccffcc", font=("Consolas", 14), relief=tk.FLAT)
        self.text.pack(fill=tk.BOTH, expand=True)

        self.dm_label = ttk.Label(root)
        self.dm_label.place(relx=1.0, rely=1.0, anchor="se", x=-20, y=-20)

    def _load_cache(self) -> dict:
        if not self.cache_path.exists():
            return {"beds": {}}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("cache read failed: %s", exc)
            return {"beds": {}}

    def _update_text(self, cache: dict) -> None:
        lines = [f"Monitor cache ts: {cache.get('ts', '-')}\n", "=" * 90 + "\n"]
        for bed_id, bed in sorted(cache.get("beds", {}).items()):
            lines.append(f"[{bed_id}] ts={bed.get('ts', '-') }\n")
            for code, vital in sorted(bed.get("vitals", {}).items()):
                lines.append(
                    f"  - {code:<8} value={vital.get('value')} unit={vital.get('unit', '')} flag={vital.get('flag', '')}\n"
                )
            lines.append("\n")
        self.text.delete("1.0", tk.END)
        self.text.insert(tk.END, "".join(lines))

    def _update_datamatrix(self, cache: dict) -> None:
        self.seq += 1
        payload = make_payload(cache, seq=self.seq, schema_version=1)
        blob = encode_payload(payload)
        dm_img = render_datamatrix(blob, size=280)
        self.dm_photo = ImageTk.PhotoImage(dm_img)
        self.dm_label.configure(image=self.dm_photo)

    def refresh(self) -> None:
        cache = self._load_cache()
        self._update_text(cache)
        try:
            self._update_datamatrix(cache)
        except Exception as exc:
            logger.warning("datamatrix render failed: %s", exc)
        self.root.after(self.interval_ms, self.refresh)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="monitor_cache.json")
    ap.add_argument("--interval-ms", type=int, default=1000)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    root = tk.Tk()
    app = MonitorApp(root, Path(args.cache), interval_ms=args.interval_ms)
    app.refresh()
    root.mainloop()


if __name__ == "__main__":
    main()
