from __future__ import annotations

import argparse
import atexit
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import cache_io
from hl7_parser import parse_hl7_message

SB = b"\x0b"
EB_CR = b"\x1c\x0d"
logger = logging.getLogger(__name__)
WRITER_LOCK_TIMEOUT_SEC = 2.0
CACHE_WRITE_RETRIES = 20
CACHE_WRITE_RETRY_DELAY_SEC = 0.05
CACHE_WRITE_RETRY_MAX_DELAY_SEC = 1.0


@dataclass
class BedDataAggregator:
    beds: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    packet_id: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update_from_parsed(self, parsed: Dict[str, Any]) -> None:
        bed = parsed.get("bed", "UNKNOWN")
        self.beds[bed] = {
            "ts": parsed.get("ts", datetime.now(timezone.utc).isoformat()),
            "patient": parsed.get("patient", {}),
            "vitals": parsed.get("vitals", {}),
        }

    def snapshot(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        self.packet_id += 1
        return {
            "epoch_ms": int(now.timestamp() * 1000),
            "ts": now.isoformat(timespec="milliseconds"),
            "packet_id": self.packet_id,
            "source": "hl7_receiver",
            "beds": self.beds,
        }


def _extract_mllp_payload(data: bytes) -> str:
    start = data.find(SB)
    end = data.find(EB_CR)
    if start == -1 or end == -1 or end <= start:
        return ""
    return data[start + 1 : end].decode("utf-8", errors="ignore")




def _permission_hint(cache_path: Path) -> str:
    return (
        "hint: the cache file may be held by antivirus/indexer/previewer or another writer process. "
        f"Close JSON viewers and ensure only one writer targets {cache_path.name}."
    )

def _write_cache_atomic(cache_path: Path, payload: Dict[str, Any]) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, CACHE_WRITE_RETRIES + 1):
        try:
            cache_io.atomic_write_json(cache_path, payload)
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt >= CACHE_WRITE_RETRIES:
                break
            delay = min(CACHE_WRITE_RETRY_MAX_DELAY_SEC, CACHE_WRITE_RETRY_DELAY_SEC * (2 ** (attempt - 1)))
            time.sleep(delay)
        except TimeoutError as exc:
            last_exc = exc
            if attempt >= CACHE_WRITE_RETRIES:
                break
            delay = min(CACHE_WRITE_RETRY_MAX_DELAY_SEC, CACHE_WRITE_RETRY_DELAY_SEC * (2 ** (attempt - 1)))
            time.sleep(delay)

    raise RuntimeError(
        f"failed to update cache after retries: {cache_path}; {_permission_hint(cache_path)}"
    ) from last_exc


def claim_single_writer(cache_path: Path, writer_name: str) -> tuple[Path, int]:
    writer_lock_path = cache_path.with_name(f"{cache_path.name}.writer.lock")
    fd = cache_io.acquire_lock(writer_lock_path, timeout_sec=WRITER_LOCK_TIMEOUT_SEC)
    os.ftruncate(fd, 0)
    os.write(fd, f"writer={writer_name} pid={os.getpid()}\n".encode("utf-8"))
    return writer_lock_path, fd


def _release_claim(lock_path: Path | None, fd: int | None) -> None:
    if lock_path is None or fd is None:
        return
    try:
        os.close(fd)
    finally:
        lock_path.unlink(missing_ok=True)


def _handle_client(conn: socket.socket, aggregator: BedDataAggregator, cache_path: Path) -> None:
    try:
        data = conn.recv(65535)
        message = _extract_mllp_payload(data)
        if not message:
            return
        parsed = parse_hl7_message(message)
        with aggregator.lock:
            aggregator.update_from_parsed(parsed)
            try:
                _write_cache_atomic(cache_path, aggregator.snapshot())
            except Exception as exc:
                logger.warning("cache update failed (skip this tick): %s", exc)
        conn.sendall(SB + b"MSA|AA|OK" + EB_CR)
    finally:
        conn.close()


def serve(host: str, port: int, cache_path: Path) -> None:
    aggregator = BedDataAggregator()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with aggregator.lock:
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
    # NOTE: cacheは単一writer運用が前提です。
    # - シミュレーション時: generator.py のみが cache writer
    # - 実HL7時: hl7_receiver.py のみが cache writer
    # PowerShell例:
    #   generatorのみ: python generator.py --cache-out generator_cache.json -> python dm_display_app.py --cache generator_cache.json
    #   receiverのみ : python hl7_receiver.py --cache receiver_cache.json -> python dm_display_app.py --cache receiver_cache.json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--cache", default="receiver_cache.json", help="receiver用cache出力先 (generatorとは分離推奨)")
    args = ap.parse_args()
    cache_path = Path(args.cache)
    claim_lock_path: Path | None = None
    claim_fd: int | None = None
    try:
        claim_lock_path, claim_fd = claim_single_writer(cache_path, "hl7_receiver")
    except TimeoutError as exc:
        logger.warning("writer claim timed out; continuing without exclusive claim: %s", exc)
    atexit.register(_release_claim, claim_lock_path, claim_fd)
    serve(args.host, args.port, cache_path)


if __name__ == "__main__":
    main()
