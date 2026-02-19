from __future__ import annotations

import argparse
import json
import logging
import subprocess
import tempfile
from pathlib import Path

import dm_codec
import dm_payload

logger = logging.getLogger(__name__)
BARCODE_TYPE_DATAMATRIX = "71"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a DataMatrix PNG from monitor_cache.json.")
    parser.add_argument("--cache", required=True, help="Path to monitor_cache.json")
    parser.add_argument("--out", required=True, help="Output PNG path")
    parser.add_argument("--beds", type=int, default=6, help="Number of beds (default: 6)")
    parser.add_argument(
        "--params",
        default="preset20",
        choices=["preset20"],
        help="Parameter preset name (default: preset20)",
    )
    return parser.parse_args()


def _resolve_zint_exe() -> Path:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    zint_exe = repo_root / "tool" / "zint.exe"
    if not zint_exe.exists():
        raise FileNotFoundError(f"zint.exe not found: {zint_exe}")
    return zint_exe


def _run_zint(zint_exe: Path, bin_file: Path, out_path: Path) -> subprocess.CompletedProcess[str]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
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
        str(out_path),
    ]
    logger.info("running: %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    cache_path = Path(args.cache)
    out_path = Path(args.out)

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            cache = json.load(f)

        beds = dm_payload.BEDS_6[: args.beds]
        params = dm_payload.PARAMS_20
        packet_bytes = dm_payload.build_packet(cache, beds=beds, params=params)
        blob = dm_codec.wrap(packet_bytes)
        logger.info("blob size=%d bytes", len(blob))

        zint_exe = _resolve_zint_exe()
        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".bin") as tf:
            tf.write(blob)
            blob_file = Path(tf.name)

        try:
            result = _run_zint(zint_exe, blob_file, out_path)
        finally:
            blob_file.unlink(missing_ok=True)

        if result.returncode != 0:
            raise RuntimeError(
                "zint.exe failed "
                f"(returncode={result.returncode})\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        if not out_path.exists() or out_path.stat().st_size <= 0:
            raise RuntimeError("zint.exe completed but output PNG is missing or empty")

        logger.info("saved DataMatrix PNG: %s", out_path)
        logger.info("packet size=%d bytes", len(packet_bytes))
        return 0
    except FileNotFoundError as exc:
        logger.error("file not found: %s", exc)
    except json.JSONDecodeError as exc:
        logger.error("invalid JSON in cache file '%s': %s", cache_path, exc)
    except RuntimeError as exc:
        logger.error("failed to create DataMatrix image: %s", exc)
    except Exception as exc:  # pragma: no cover
        logger.exception("unexpected error while generating DataMatrix PNG: %s", exc)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
