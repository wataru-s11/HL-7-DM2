from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import socket
import threading
from queue import Empty, Queue
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from hl7_parser import parse_hl7_message

SB = b"\x0b"
EB_CR = b"\x1c\x0d"
RECV_TIMEOUT_SEC = 2.0
RECV_CHUNK_SIZE = 4096
MAX_MLLP_FRAME_SIZE = 256 * 1024

logger = logging.getLogger(__name__)


@dataclass
class BedDataAggregator:
    beds: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update_from_parsed(self, parsed: Dict[str, Any]) -> None:
        bed = parsed.get("bed", "UNKNOWN")
        self.beds[bed] = {
            "ts": parsed.get("ts", datetime.now(timezone.utc).isoformat()),
            "patient": parsed.get("patient", {}),
            "vitals": parsed.get("vitals", {}),
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "beds": self.beds,
        }


def _extract_mllp_payload(data: bytes) -> str:
    start = data.find(SB)
    end = data.find(EB_CR)
    if start == -1 or end == -1 or end <= start:
        return ""
    return data[start + 1 : end].decode("utf-8", errors="ignore")


def atomic_write_json(cache_path: Path, payload: Dict[str, Any], tmp_dir: Path, *, indent: int | None = None) -> None:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    shutil.copyfile(tmp_path, cache_path)
    tmp_path.unlink(missing_ok=True)


def _background_worker(
    updates: Queue[Dict[str, Any]],
    aggregator: BedDataAggregator,
    cache_path: Path,
    tmp_dir: Path,
) -> None:
    while True:
        try:
            parsed = updates.get(timeout=0.5)
        except Empty:
            continue

        try:
            with aggregator.lock:
                aggregator.update_from_parsed(parsed)
                atomic_write_json(cache_path, aggregator.snapshot(), tmp_dir, indent=2)
        except Exception:
            logger.exception("Background cache update failed")
        finally:
            updates.task_done()


def _handle_client(conn: socket.socket, updates: Queue[Dict[str, Any]]) -> None:
    try:
        conn.settimeout(RECV_TIMEOUT_SEC)
        buf = b""
        invalid_frame = False
        while True:
            try:
                chunk = conn.recv(RECV_CHUNK_SIZE)
            except socket.timeout:
                invalid_frame = True
                break

            if not chunk:
                break

            buf += chunk
            if EB_CR in buf:
                break
            if len(buf) > MAX_MLLP_FRAME_SIZE:
                invalid_frame = True
                break

        message = _extract_mllp_payload(buf)
        if not message:
            invalid_frame = True

        if invalid_frame:
            conn.sendall(SB + b"MSA|AE|ERR" + EB_CR)
            return

        try:
            parsed = parse_hl7_message(message)
        except Exception:
            conn.sendall(SB + b"MSA|AE|PARSE_ERROR" + EB_CR)
            return

        conn.sendall(SB + b"MSA|AA|OK" + EB_CR)
        updates.put(parsed)
    except Exception:
        logger.exception("Error while handling client connection")
    finally:
        conn.close()


def serve(host: str, port: int, cache_path: Path, tmp_dir: Path) -> None:
    aggregator = BedDataAggregator()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    updates: Queue[Dict[str, Any]] = Queue()

    atomic_write_json(cache_path, aggregator.snapshot(), tmp_dir)
    threading.Thread(
        target=_background_worker,
        args=(updates, aggregator, cache_path, tmp_dir),
        daemon=True,
    ).start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        print(f"HL7 receiver listening on {host}:{port}")
        while True:
            conn, _ = s.accept()
            threading.Thread(target=_handle_client, args=(conn, updates), daemon=True).start()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--cache", default="monitor_cache.json")
    ap.add_argument("--tmp-dir", default=r"C:\temp\hl7_dm_tmp")
    args = ap.parse_args()
    serve(args.host, args.port, Path(args.cache), Path(args.tmp_dir))


if __name__ == "__main__":
    main()

# Manual test (PowerShell)
# receiver:  python hl7_receiver.py --host 127.0.0.1 --port 2575 --cache receiver_cache.json
# generator: python generator.py --host 127.0.0.1 --port 2575 --count 1
# Confirm generator prints "sent" and receiver logs no exception.
