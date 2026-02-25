from __future__ import annotations

import json
import os
import random
import threading
import time
from pathlib import Path
from typing import Any


def acquire_lock(lock_path: Path, timeout_sec: float = 5.0, poll: float = 0.05) -> int:
    """Acquire an exclusive lock file via O_CREAT|O_EXCL and return its file descriptor."""
    lock_path = Path(lock_path)
    deadline = time.monotonic() + timeout_sec

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            payload = f"pid={os.getpid()} tid={threading.get_ident()} ts={time.time():.6f}\n"
            os.write(fd, payload.encode("utf-8"))
            return fd
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for lock file: {lock_path}")
            time.sleep(max(0.001, poll))


def _release_lock(lock_path: Path, lock_fd: int) -> None:
    try:
        os.close(lock_fd)
    finally:
        Path(lock_path).unlink(missing_ok=True)


def atomic_write_json(path: Path, obj: Any, retries: int = 20) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(obj, ensure_ascii=False, indent=2)

    lock_path = path.with_name(f"{path.name}.lock")
    lock_fd = acquire_lock(lock_path)
    try:
        last_exc: PermissionError | None = None
        for attempt in range(1, retries + 1):
            token = random.getrandbits(32)
            tmp_path = path.with_name(
                f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}.{token:08x}"
            )
            try:
                with tmp_path.open("w", encoding="utf-8") as handle:
                    handle.write(text)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, path)
                return
            except PermissionError as exc:
                last_exc = exc
                tmp_path.unlink(missing_ok=True)
                if attempt >= retries:
                    break
                delay = min(0.8, 0.01 * (2 ** (attempt - 1)))
                delay += random.uniform(0.0, 0.01)
                time.sleep(delay)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"atomic write failed: {path}")
    finally:
        _release_lock(lock_path, lock_fd)


def atomic_append_jsonl(path: Path, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False) + "\n"

    lock_path = path.with_name(f"{path.name}.lock")
    lock_fd = acquire_lock(lock_path)
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        _release_lock(lock_path, lock_fd)
