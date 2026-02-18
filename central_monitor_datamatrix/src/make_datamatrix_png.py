from __future__ import annotations

import argparse
import base64
import inspect
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

BARCODE_TYPE_DATAMATRIX = "71"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a DataMatrix PNG from monitor_cache.json."
    )
    parser.add_argument("--cache", required=True, help="Path to monitor_cache.json")
    parser.add_argument("--out", required=True, help="Output PNG path")
    parser.add_argument(
        "--beds-limit",
        type=int,
        default=64,
        help="Maximum beds to include when dm_payload.make_payload supports beds_limit",
    )
    return parser.parse_args()


def _resolve_zint_exe() -> Path:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    zint_exe = repo_root / "tool" / "zint.exe"
    if not zint_exe.exists():
        raise FileNotFoundError(f"zint.exe not found: {zint_exe}")
    return zint_exe


def _build_payload(dm_payload: Any, cache: dict[str, Any], beds_limit: int) -> dict[str, Any]:
    make_payload = dm_payload.make_payload
    sig = inspect.signature(make_payload)
    params = sig.parameters

    if "beds_limit" in params:
        return make_payload(cache, beds_limit)

    if "seq" in params:
        seq_counter = dm_payload.SeqCounter()
        seq = seq_counter.next()
        logger.info(
            "dm_payload.make_payload does not accept beds_limit; falling back to seq=%d",
            seq,
        )
        return make_payload(cache, seq)

    return make_payload(cache)


def _run_zint(zint_exe: Path, payload_text: str, out_path: Path) -> subprocess.CompletedProcess[str]:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", encoding="ascii", delete=False, suffix=".txt") as tf:
        tf.write(payload_text)
        input_file = Path(tf.name)

    try:
        cmd = [
            str(zint_exe),
            "-b",
            BARCODE_TYPE_DATAMATRIX,
            "--filetype=PNG",
            "-i",
            str(input_file),
            "-o",
            str(out_path),
        ]
        logger.info("running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result
    finally:
        try:
            input_file.unlink(missing_ok=True)
        except Exception:
            logger.warning("failed to remove temporary input file: %s", input_file)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    cache_path = Path(args.cache)
    out_path = Path(args.out)

    try:
        import dm_codec
        import dm_payload

        with cache_path.open("r", encoding="utf-8") as f:
            cache = json.load(f)

        payload = _build_payload(dm_payload, cache, args.beds_limit)
        blob = dm_codec.encode_payload(payload)
        payload_text = base64.b64encode(blob).decode("ascii")

        zint_exe = _resolve_zint_exe()
        result = _run_zint(zint_exe, payload_text, out_path)

        if result.returncode != 0:
            raise RuntimeError(
                "zint.exe failed "
                f"(returncode={result.returncode})\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        if not out_path.exists() or out_path.stat().st_size <= 0:
            raise RuntimeError(
                "zint.exe completed but output PNG is missing or empty\n"
                f"out={out_path}\n"
                f"returncode={result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        payload_keys = sorted(payload.keys())
        beds_count = len(payload.get("beds") or {})
        logger.info("saved DataMatrix PNG: %s", out_path)
        logger.info("payload keys=%s beds=%d", payload_keys, beds_count)
        return 0
    except FileNotFoundError as exc:
        logger.error("file not found: %s", exc)
    except json.JSONDecodeError as exc:
        logger.error("invalid JSON in cache file '%s': %s", cache_path, exc)
    except RuntimeError as exc:
        logger.error("failed to create DataMatrix image: %s", exc)
    except ModuleNotFoundError as exc:
        logger.error("missing dependency: %s", exc)
    except Exception as exc:  # pragma: no cover
        logger.exception("unexpected error while generating DataMatrix PNG: %s", exc)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
