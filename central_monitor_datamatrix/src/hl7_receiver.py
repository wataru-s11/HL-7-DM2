from __future__ import annotations

import argparse
import json
import os
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from hl7_parser import parse_hl7_message

SB = b"\x0b"
EB_CR = b"\x1c\x0d"


@dataclass
class BedDataAggregator:
    beds: Dict[str, Dict[str, Any]] = field(default_factory=dict)

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


def _write_cache_atomic(cache_path: Path, payload: Dict[str, Any], *, indent: int | None = None) -> None:
    tmp_path = cache_path.with_name(f"{cache_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, cache_path)


def _handle_client(conn: socket.socket, aggregator: BedDataAggregator, cache_path: Path) -> None:
    try:
        data = conn.recv(65535)
        message = _extract_mllp_payload(data)
        if not message:
            return
        parsed = parse_hl7_message(message)
        aggregator.update_from_parsed(parsed)
        _write_cache_atomic(cache_path, aggregator.snapshot(), indent=2)
        conn.sendall(SB + b"MSA|AA|OK" + EB_CR)
    finally:
        conn.close()


def serve(host: str, port: int, cache_path: Path) -> None:
    aggregator = BedDataAggregator()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    _write_cache_atomic(cache_path, aggregator.snapshot())

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        print(f"HL7 receiver listening on {host}:{port}")
        while True:
            conn, _ = s.accept()
            threading.Thread(target=_handle_client, args=(conn, aggregator, cache_path), daemon=True).start()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--cache", default="monitor_cache.json")
    args = ap.parse_args()
    serve(args.host, args.port, Path(args.cache))


if __name__ == "__main__":
    main()
