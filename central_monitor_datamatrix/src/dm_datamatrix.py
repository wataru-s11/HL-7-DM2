from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import dm_codec
import dm_payload

BARCODE_TYPE_DATAMATRIX = "71"


def resolve_zint_exe() -> Path:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    zint_exe = repo_root / "tool" / "zint.exe"
    if not zint_exe.exists():
        raise FileNotFoundError(f"zint.exe not found: {zint_exe}")
    return zint_exe


def build_blob_from_cache(cache: dict[str, Any], beds_count: int = 6) -> tuple[bytes, bytes]:
    beds = dm_payload.BEDS_6[:beds_count]
    params = dm_payload.PARAMS_20
    packet_bytes = dm_payload.build_packet(cache, beds=beds, params=params)
    blob = dm_codec.wrap(packet_bytes)
    return blob, packet_bytes


def generate_datamatrix_png(
    blob: bytes,
    out_path: Path,
    zint_exe: Path | None = None,
    timeout_sec: float = 3.0,
) -> subprocess.CompletedProcess[str]:
    zint_exe = zint_exe or resolve_zint_exe()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".bin") as tf:
        tf.write(blob)
        bin_file = Path(tf.name)

    tmp_png = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}.{time.time_ns()}.png")

    try:
        cmd = [
            str(zint_exe),
            "-b",
            BARCODE_TYPE_DATAMATRIX,
            "--binary",
            "-i",
            str(bin_file),
            "--filetype=PNG",
            "--quiet",
            "--square",
            "--quietzones",
            "--scale=4",
            "-o",
            str(tmp_png),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
        if result.returncode == 0:
            replace_attempts = 3
            for attempt in range(1, replace_attempts + 1):
                try:
                    os.replace(tmp_png, out_path)
                    break
                except PermissionError:
                    if attempt == replace_attempts:
                        raise
                    time.sleep(0.05 * attempt)
        return result
    except subprocess.TimeoutExpired as exc:
        tmp_png.unlink(missing_ok=True)
        raise RuntimeError(
            "zint.exe timed out "
            f"(timeout_sec={timeout_sec})\n"
            f"stdout:\n{exc.stdout or ''}\n"
            f"stderr:\n{exc.stderr or ''}"
        ) from exc
    finally:
        bin_file.unlink(missing_ok=True)
        tmp_png.unlink(missing_ok=True)


def load_cache_with_retry(cache_path: Path, retries: int = 3, retry_delay_sec: float = 0.05) -> tuple[dict[str, Any], int]:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                return json.load(f), attempt
        except (json.JSONDecodeError, OSError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(retry_delay_sec)
    assert last_exc is not None
    raise last_exc


def generate_datamatrix_png_from_cache(cache_path: Path, out_path: Path, beds_count: int = 6) -> tuple[dict[str, int], int]:
    cache, attempt = load_cache_with_retry(cache_path)

    return generate_datamatrix_png_from_cache_data(cache, out_path, beds_count=beds_count), attempt


def generate_datamatrix_png_from_cache_data(cache: dict[str, Any], out_path: Path, beds_count: int = 6) -> dict[str, int]:

    blob, packet_bytes = build_blob_from_cache(cache, beds_count=beds_count)
    result = generate_datamatrix_png(blob, out_path)
    if result.returncode != 0:
        raise RuntimeError(
            "zint.exe failed "
            f"(returncode={result.returncode})\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    if not out_path.exists() or out_path.stat().st_size <= 0:
        raise RuntimeError("zint.exe completed but output PNG is missing or empty")

    return {"blob_size": len(blob), "packet_size": len(packet_bytes)}


def decode_payload_from_bgr_image(image_bgr) -> dict[str, Any]:
    import dm_decoder

    blob = dm_decoder.decode_datamatrix(image_bgr)
    if blob is None:
        raise ValueError("failed to decode DataMatrix blob from image")

    packet_bytes = dm_codec.unwrap(blob)
    return dm_payload.parse_packet(packet_bytes)
