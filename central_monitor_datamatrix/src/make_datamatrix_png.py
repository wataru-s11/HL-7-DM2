from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import dm_datamatrix

logger = logging.getLogger(__name__)


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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    cache_path = Path(args.cache)
    out_path = Path(args.out)

    try:
        sizes, read_attempt = dm_datamatrix.generate_datamatrix_png_from_cache(
            cache_path=cache_path,
            out_path=out_path,
            beds_count=args.beds,
        )
        logger.info("saved DataMatrix PNG: %s", out_path)
        logger.info("blob size=%d bytes", sizes["blob_size"])
        logger.info("packet size=%d bytes", sizes["packet_size"])
        logger.info("cache read attempt=%d", read_attempt)
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
