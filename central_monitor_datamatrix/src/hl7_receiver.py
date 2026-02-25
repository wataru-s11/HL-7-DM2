from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from hl7_parser import parse_hl7_message

SB = b"\x0b"
EB_CR = b"\x1c\x0d"
logger = logging.getLogger(__name__)
LOCK_TIMEOUT_SEC = 2.0
LOCK_STALE_SEC = 10.0


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


def _write_cache_atomic(cache_path: Path, payload: Dict[str, Any], *, indent: int | None = None) -> None:
    _write_text_atomic_with_retry(
        cache_path,
        json.dumps(payload, ensure_ascii=False, indent=indent),
    )


def _acquire_lock(lock_path: Path, timeout_sec: float = LOCK_TIMEOUT_SEC, stale_sec: float = LOCK_STALE_SEC) -> int:
    deadline = time.time() + timeout_sec
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()} tid={threading.get_ident()} ts={time.time():.6f}\n".encode("utf-8"))
            return fd
        except FileExistsError:
            try:
                age_sec = time.time() - lock_path.stat().st_mtime
                if age_sec > stale_sec:
                    logger.warning("lock file is stale; removing and retrying: %s age=%.2fs", lock_path, age_sec)
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.time() >= deadline:
                raise TimeoutError(f"timed out waiting for lock file: {lock_path}")
            time.sleep(0.05)


def _release_lock(lock_path: Path, lock_fd: int) -> None:
    try:
        os.close(lock_fd)
    finally:
        lock_path.unlink(missing_ok=True)


def _write_text_atomic_with_retry(path: Path, text: str, retries: int = 40, base_delay_sec: float = 0.05) -> None:
    lock_path = path.with_name(f"{path.name}.lock")
    lock_fd = _acquire_lock(lock_path)
    try:
        last_error: PermissionError | None = None
        for attempt in range(1, retries + 1):
            token = secrets.token_hex(4)
            tmp_path = path.with_name(
                f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}.{token}"
            )
            try:
                with tmp_path.open("w", encoding="utf-8") as f:
                    f.write(text)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
                return
            except PermissionError as exc:
                last_error = exc
                logger.warning(
                    "atomic replace retry due to PermissionError: target=%s attempt=%d/%d error=%s",
                    path,
                    attempt,
                    retries,
                    exc,
                )
                tmp_path.unlink(missing_ok=True)
                if attempt < retries:
                    time.sleep(base_delay_sec)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

        logger.warning("atomic replace retries exhausted; falling back to direct write: target=%s", path)
        try:
            with path.open("w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            if last_error is not None:
                logger.error("direct write fallback failed after atomic retries: target=%s", path)
                raise last_error
            raise
    finally:
        _release_lock(lock_path, lock_fd)


def _handle_client(conn: socket.socket, aggregator: BedDataAggregator, cache_path: Path) -> None:
    try:
        data = conn.recv(65535)
        message = _extract_mllp_payload(data)
        if not message:
            return
        parsed = parse_hl7_message(message)
        with aggregator.lock:
            aggregator.update_from_parsed(parsed)
            _write_cache_atomic(cache_path, aggregator.snapshot(), indent=2)
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--cache", default="receiver_cache.json", help="receiver用cache出力先 (generatorとは分離推奨)")
    args = ap.parse_args()
    serve(args.host, args.port, Path(args.cache))


if __name__ == "__main__":
    main()
