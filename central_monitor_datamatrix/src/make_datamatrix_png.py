from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a DataMatrix PNG from monitor_cache.json."
    )
    parser.add_argument("--cache", required=True, help="Path to monitor_cache.json")
    parser.add_argument("--out", required=True, help="Output PNG path")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    cache_path = Path(args.cache)
    out_path = Path(args.out)

    try:
        import dm_codec
        import dm_payload
        import dm_render

        with cache_path.open("r", encoding="utf-8") as f:
            cache = json.load(f)

        seq_counter = dm_payload.SeqCounter()
        seq = seq_counter.next()

        payload = dm_payload.make_payload(cache, seq)
        blob = dm_codec.encode_payload(payload)
        img = dm_render.render_datamatrix(blob, size_px=320)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(out_path)

        payload_keys = sorted(payload.keys())
        beds_count = len(payload.get("beds") or {})
        logger.info("saved DataMatrix PNG: %s", out_path)
        logger.info("payload keys=%s beds=%d", payload_keys, beds_count)
        return 0
    except FileNotFoundError:
        logger.error("cache file not found: %s", cache_path)
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
