from __future__ import annotations

import argparse
import atexit
import logging
import os
import socket
import subprocess
import threading
import time
import re
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


def _extract_pid_from_lock(lock_body: str) -> int | None:
    match = re.search(r"\bpid=(\d+)\b", lock_body)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _pid_exists_windows(pid: int) -> bool | None:
    if pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except Exception as exc:
        logger.warning("failed to verify writer pid=%d via tasklist: %s", pid, exc)
        return None
    if result.returncode != 0:
        logger.warning(
            "tasklist returned non-zero while checking writer pid=%d: rc=%d stderr=%s",
            pid,
            result.returncode,
            result.stderr.strip(),
        )
        return None
    return f'"{pid}"' in result.stdout


def _cleanup_stale_writer_lock(lock_path: Path) -> bool:
    if not lock_path.exists():
        return True

    try:
        lock_body = lock_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        logger.warning("failed to read existing writer lock %s: %s", lock_path, exc)
        return True

    pid = _extract_pid_from_lock(lock_body)
    if pid is None:
        logger.warning("existing writer lock has no parseable pid; keeping as-is: %s", lock_path)
        return True

    if os.name != "nt":
        logger.info("writer lock pid check is Windows-only; keeping existing lock pid=%d", pid)
        return True

    exists = _pid_exists_windows(pid)
    if exists is None:
        return True
    if exists:
        logger.info("existing writer lock owner is alive (pid=%d): %s", pid, lock_path)
        return True

    logger.warning("detected stale writer lock (pid=%d not found); removing: %s", pid, lock_path)
    try:
        lock_path.unlink()
        return True
    except Exception as exc:
        logger.warning("failed to remove stale writer lock %s: %s", lock_path, exc)
        return False


def claim_single_writer(cache_path: Path, writer_name: str) -> tuple[Path | None, int | None, bool]:
    writer_lock_path = cache_path.with_name(f"{cache_path.name}.writer.lock")
    if not _cleanup_stale_writer_lock(writer_lock_path):
        return None, None, False
    try:
        fd = cache_io.acquire_lock(writer_lock_path, timeout_sec=WRITER_LOCK_TIMEOUT_SEC)
    except TimeoutError as exc:
        logger.warning("writer claim timed out; read-only mode enabled: %s", exc)
        return None, None, False

    os.ftruncate(fd, 0)
    os.write(fd, f"writer={writer_name} pid={os.getpid()}\n".encode("utf-8"))
    return writer_lock_path, fd, True


def _release_claim(lock_path: Path | None, fd: int | None) -> None:
    if lock_path is None or fd is None:
        return
    try:
        os.close(fd)
    finally:
        lock_path.unlink(missing_ok=True)


def _handle_client(conn: socket.socket, aggregator: BedDataAggregator, cache_path: Path, write_enabled: bool) -> None:
    try:
        data = conn.recv(65535)
        message = _extract_mllp_payload(data)
        if not message:
            return
        parsed = parse_hl7_message(message)
        with aggregator.lock:
            aggregator.update_from_parsed(parsed)
            if write_enabled:
                try:
                    _write_cache_atomic(cache_path, aggregator.snapshot())
                except Exception as exc:
                    logger.warning("cache update failed (skip this tick): %s", exc)
        conn.sendall(SB + b"MSA|AA|OK" + EB_CR)
    finally:
        conn.close()


def serve(host: str, port: int, cache_path: Path, write_enabled: bool) -> None:
    aggregator = BedDataAggregator()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if write_enabled:
        with aggregator.lock:
            _write_cache_atomic(cache_path, aggregator.snapshot())
    else:
        logger.info("running in read-only mode: cache writes are suppressed")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        print(f"HL7 receiver listening on {host}:{port}")
        while True:
            conn, _ = s.accept()
            threading.Thread(
                target=_handle_client,
                args=(conn, aggregator, cache_path, write_enabled),
                daemon=True,
            ).start()


def main() -> None:
    # NOTE: cacheは単一writer運用が前提です。
    # - シミュレーション時: generator.py のみが cache writer
    # - 実HL7時: hl7_receiver.py のみが cache writer
    # PowerShell例:
    #   generatorのみ: python generator.py --cache-out generator_cache.json -> python dm_display_app.py --cache generator_cache.json
    #   receiverのみ : python hl7_receiver.py --cache receiver_cache.json -> python dm_display_app.py --cache receiver_cache.json
    # 最小動作確認(院外PC / PowerShell):
    #   1) stale lock作成: Set-Content .\receiver_cache.json.writer.lock 'writer=hl7_receiver pid=999999'
    #   2) receiver起動  : python .\hl7_receiver.py --host 127.0.0.1 --port 2575 --cache receiver_cache.json
    #   3) generator送信 : python .\generator.py --host 127.0.0.1 --port 2575 --count 1 --cache-out generator_cache.json
    #   4) ログ確認      : stale lock自動削除 or read-only mode 表示(排他失敗時でも安全継続)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--cache", default="receiver_cache.json", help="receiver用cache出力先 (generatorとは分離推奨)")
    args = ap.parse_args()
    cache_path = Path(args.cache)
    claim_lock_path: Path | None = None
    claim_fd: int | None = None
    write_enabled = True
    claim_lock_path, claim_fd, write_enabled = claim_single_writer(cache_path, "hl7_receiver")
    atexit.register(_release_claim, claim_lock_path, claim_fd)
    serve(args.host, args.port, cache_path, write_enabled)


if __name__ == "__main__":
    main()
