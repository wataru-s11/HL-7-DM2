from __future__ import annotations

import argparse
import json
import math
import re
from bisect import bisect_left
from collections import Counter, deque
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable

BED_IDS = [f"BED{i:02d}" for i in range(1, 7)]
VITAL_ORDER = [
    "HR", "ART_S", "ART_D", "ART_M", "CVP_M", "RAP_M", "SpO2", "TSKIN", "TRECT", "rRESP",
    "EtCO2", "RR", "VTe", "VTi", "Ppeak", "PEEP", "O2conc", "NO", "BSR1", "BSR2",
]

DEFAULT_CONFIG = {
    "integer_preferred_fields": ["HR", "SpO2", "RR", "BSR1", "BSR2"],
    "field_epsilons": {
        "HR": 0.0, "SpO2": 0.0, "RR": 0.0, "TSKIN": 0.1, "TRECT": 0.1,
        "ART_S": 1.0, "ART_D": 1.0, "ART_M": 1.0, "CVP_M": 1.0, "RAP_M": 1.0,
        "EtCO2": 1.0, "Ppeak": 1.0, "PEEP": 1.0, "VTe": 1.0, "VTi": 1.0,
        "O2conc": 1.0, "NO": 1.0, "rRESP": 1.0,
    },
    "vital_ranges": {
        "HR": [0, 300], "SpO2": [0, 100], "RR": [0, 120], "TSKIN": [20, 45], "TRECT": [20, 45],
        "ART_S": [0, 300], "ART_D": [0, 200], "ART_M": [0, 250], "CVP_M": [-20, 80], "RAP_M": [-20, 80],
        "EtCO2": [0, 150], "Ppeak": [0, 100], "PEEP": [0, 50], "VTe": [0, 3000], "VTi": [0, 3000],
        "O2conc": [0, 100], "NO": [0, 200], "BSR1": [0, 100], "BSR2": [0, 100], "rRESP": [0, 120],
    },
}

FILENAME_TS_RE = re.compile(r"(\d{8})_(\d{6})_(\d{3})")
NUM_RE = re.compile(r"[-+]?\d*\.?\d+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate DataMatrix decode results against truth data")
    parser.add_argument("--decoded-results", required=True, help="Decoded results JSONL path")
    parser.add_argument(
        "--truth-mode",
        required=True,
        choices=["cache", "generator", "generator_jsonl", "cache_snapshot_jsonl"],
    )
    parser.add_argument("--monitor-cache-dir", help="Directory containing monitor cache snapshots (legacy)")
    parser.add_argument("--generator-results", help="Generator truth JSONL path")
    parser.add_argument("--cache-snapshots", help="cache_snapshots.jsonl path")
    parser.add_argument("--out", required=True, help="Detailed result JSONL path")
    parser.add_argument("--summary-out", required=True, help="Summary JSON path")
    parser.add_argument("--last", type=int, help="Evaluate only the last N decoded records")
    parser.add_argument("--tolerance-sec", type=float, default=2.0, help="Max timestamp delta for truth matching")
    parser.add_argument("--config", default="validator_dm_config.json", help="Config JSON path")
    parser.add_argument("--debug-one", action="store_true", help="Print BED01 truth/decoded diff for first matched record")
    return parser.parse_args()


def load_or_create_config(path: Path) -> dict[str, Any]:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        print(f"[INFO] Loaded config: {path}")
        return cfg
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Config not found. Created default config: {path}")
    return DEFAULT_CONFIG


def parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    txt = value.strip()
    if not txt:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(txt)
    except ValueError:
        return None


def infer_timestamp_from_filename(path_text: str | None) -> datetime | None:
    if not path_text:
        return None
    m = FILENAME_TS_RE.search(path_text)
    if not m:
        return None
    d, t, ms = m.groups()
    try:
        return datetime.strptime(f"{d}_{t}_{ms}", "%Y%m%d_%H%M%S_%f")
    except ValueError:
        return None


def normalize_number(value: Any) -> tuple[float | None, str]:
    if value is None:
        return None, "missing"
    if isinstance(value, bool):
        return None, "invalid"
    if isinstance(value, (int, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None, "missing"
        return f, "ok"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, "missing"
        m = NUM_RE.search(text)
        if not m:
            return None, "invalid"
        try:
            f = float(m.group(0))
            if math.isnan(f) or math.isinf(f):
                return None, "missing"
            return f, "ok"
        except ValueError:
            return None, "invalid"
    return None, "invalid"


def normalize_epoch_ms(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return int(round(f))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            f = float(text)
            if math.isnan(f) or math.isinf(f):
                return None
            return int(round(f))
        except ValueError:
            return None
    return None


def normalize_packet_id(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            text = line.strip()
            if not text:
                continue
            try:
                data = json.loads(text)
            except json.JSONDecodeError as exc:
                print(f"[WARN] JSON decode error at {path}:{line_no}: {exc}")
                continue
            if isinstance(data, dict):
                yield data


def tail_jsonl(path: Path, last_n: int | None) -> list[dict[str, Any]]:
    if not last_n or last_n <= 0:
        return list(iter_jsonl(path))
    q: deque[dict[str, Any]] = deque(maxlen=last_n)
    for row in iter_jsonl(path):
        q.append(row)
    return list(q)


def load_truth_generator_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in iter_jsonl(path):
        epoch_ms = normalize_epoch_ms(row.get("epoch_ms"))
        if epoch_ms is None:
            print("[WARN] truth(generator_jsonl) row missing valid epoch_ms, skipped")
            continue
        rows.append({
            "epoch_ms": epoch_ms,
            "packet_id": normalize_packet_id(row.get("packet_id")),
            "timestamp_text": row.get("ts"),
            "beds": row.get("beds", {}),
        })
    rows.sort(key=lambda x: x["epoch_ms"])
    return rows


def load_truth_cache_snapshot_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in iter_jsonl(path):
        epoch_ms = normalize_epoch_ms(row.get("epoch_ms"))
        if epoch_ms is None:
            print("[WARN] truth(cache_snapshot_jsonl) row missing valid epoch_ms, skipped")
            continue
        rows.append({
            "epoch_ms": epoch_ms,
            "packet_id": normalize_packet_id(row.get("packet_id")),
            "timestamp_text": row.get("ts"),
            "beds": row.get("beds", {}),
        })
    rows.sort(key=lambda x: x["epoch_ms"])
    return rows


def load_truth_cache(cache_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(cache_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"[WARN] Failed to read cache file {path}: {exc}")
            continue
        epoch_ms = normalize_epoch_ms(payload.get("epoch_ms")) if isinstance(payload, dict) else None
        if epoch_ms is None and isinstance(payload, dict):
            ts = parse_timestamp(payload.get("timestamp") or payload.get("ts"))
            if ts:
                epoch_ms = int(round(ts.timestamp() * 1000))
        if epoch_ms is None:
            ts = infer_timestamp_from_filename(path.name)
            if ts:
                epoch_ms = int(round(ts.timestamp() * 1000))
        if epoch_ms is None:
            continue
        rows.append({"epoch_ms": epoch_ms, "packet_id": normalize_packet_id(payload.get("packet_id")), "timestamp_text": payload.get("ts"), "beds": payload.get("beds", {})})
    rows.sort(key=lambda x: x["epoch_ms"])
    return rows


def extract_truth_value(record: dict[str, Any], bed: str, field: str) -> Any:
    beds = record.get("beds", {}) if isinstance(record, dict) else {}
    bed_data = beds.get(bed, {}) if isinstance(beds, dict) else {}
    if not isinstance(bed_data, dict):
        return None
    vitals = bed_data.get("vitals", {})
    if not isinstance(vitals, dict):
        return bed_data.get(field)
    fobj = vitals.get(field)
    if isinstance(fobj, dict):
        return fobj.get("value")
    return fobj


def extract_decoded_value(bed_data: dict[str, Any], field: str) -> Any:
    if not isinstance(bed_data, dict):
        return None
    if field in bed_data:
        return bed_data.get(field)
    params = bed_data.get("params")
    if isinstance(params, dict):
        pobj = params.get(field)
        if isinstance(pobj, dict):
            if "value" in pobj:
                return pobj.get("value")
        elif pobj is not None:
            return pobj
    vitals = bed_data.get("vitals")
    if isinstance(vitals, dict):
        vobj = vitals.get(field)
        if isinstance(vobj, dict):
            return vobj.get("value")
        return vobj
    return None


def pick_truth(
    decoded_packet_id: int | None,
    decoded_timestamp_ms: int | None,
    truth_rows: list[dict[str, Any]],
    tolerance_sec: float,
    decoded_time_source: str,
) -> tuple[dict[str, Any] | None, float | None, str]:
    if decoded_timestamp_ms is None or not truth_rows:
        if decoded_packet_id is not None:
            for row in truth_rows:
                if normalize_packet_id(row.get("packet_id")) == decoded_packet_id:
                    return row, None, "packet_id_fallback"
        return None, None, "none"

    truth_ts = [float(r["epoch_ms"]) for r in truth_rows]
    target = float(decoded_timestamp_ms)
    idx = bisect_left(truth_ts, target)
    candidates: list[tuple[float, int]] = []
    if idx < len(truth_rows):
        candidates.append((abs(truth_ts[idx] - target), idx))
    if idx - 1 >= 0:
        candidates.append((abs(truth_ts[idx - 1] - target), idx - 1))
    if not candidates:
        return None, None, "none"

    _, best_i = min(candidates, key=lambda x: x[0])
    delta = truth_ts[best_i] - target
    if abs(delta) > tolerance_sec * 1000.0:
        return None, None, "none"

    matched_by = "epoch_ms" if decoded_time_source in {"dm_epoch_ms", "cache_epoch_ms", "timestamp_ms"} else "fallback_time"
    return truth_rows[best_i], delta, matched_by


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    xs = sorted(values)
    rank = (len(xs) - 1) * (p / 100.0)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return xs[lo]
    frac = rank - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def safe_mean(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


def safe_median(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def print_debug_one(truth_row: dict[str, Any], decoded_beds: dict[str, Any]) -> None:
    print("[DEBUG] --debug-one BED01 truth vs decoded")
    tbed = truth_row.get("beds", {}).get("BED01", {}) if isinstance(truth_row.get("beds"), dict) else {}
    dbed = decoded_beds.get("BED01", {}) if isinstance(decoded_beds, dict) else {}
    for field in VITAL_ORDER:
        tv, _ = normalize_number(extract_truth_value({"beds": {"BED01": tbed}}, "BED01", field))
        dv, _ = normalize_number(extract_decoded_value(dbed if isinstance(dbed, dict) else {}, field))
        print(f"[DEBUG] BED01 {field:>6}: truth={tv} decoded={dv}")


def main() -> None:
    args = parse_args()

    decoded_path = Path(args.decoded_results)
    out_path = Path(args.out)
    summary_path = Path(args.summary_out)
    config_path = Path(args.config)

    if not decoded_path.exists():
        raise FileNotFoundError(f"decoded-results not found: {decoded_path}")

    if args.truth_mode == "cache_snapshot_jsonl" and not args.cache_snapshots:
        raise ValueError("--cache-snapshots is required when --truth-mode=cache_snapshot_jsonl")
    if args.truth_mode in {"generator", "generator_jsonl"} and not args.generator_results:
        raise ValueError("--generator-results is required when --truth-mode=generator/generator_jsonl")
    if args.truth_mode == "cache" and not args.monitor_cache_dir:
        raise ValueError("--monitor-cache-dir is required when --truth-mode=cache")

    config = load_or_create_config(config_path)
    eps_map = config.get("field_epsilons", {})
    integer_fields = set(config.get("integer_preferred_fields", []))
    vital_ranges = config.get("vital_ranges", {})

    if args.truth_mode == "cache_snapshot_jsonl":
        truth_rows = load_truth_cache_snapshot_jsonl(Path(args.cache_snapshots))
        print("[INFO] Using truth mode: cache_snapshot_jsonl (recommended)")
    elif args.truth_mode == "generator_jsonl":
        truth_rows = load_truth_generator_jsonl(Path(args.generator_results))
        print("[INFO] Using truth mode: generator_jsonl (compatible). Consider cache_snapshot_jsonl for strict 1:1 matching.")
    elif args.truth_mode == "cache":
        truth_rows = load_truth_cache(Path(args.monitor_cache_dir))
        print("[INFO] Using truth mode: cache (legacy). Consider cache_snapshot_jsonl.")
    else:
        truth_rows = load_truth_generator_jsonl(Path(args.generator_results))
        print("[INFO] truth-mode=generator is treated as generator_jsonl compatibility mode.")

    print(f"[INFO] Loaded truth rows: {len(truth_rows)}")
    decoded_rows = tail_jsonl(decoded_path, args.last)
    print(f"[INFO] Loaded decoded rows: {len(decoded_rows)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    total_expected = missing_count = invalid_count = matched_count = within_tol_matched_count = evaluated_count = 0
    abs_errors: list[float] = []
    decoded_record_count = len(decoded_rows)
    decode_success_record_count = crc_fail_record_count = truth_missing_record_count = 0
    delta_t_values: list[float] = []
    matched_by_counter: Counter[str] = Counter()
    decoded_time_source_counter: Counter[str] = Counter()
    evaluated_on_success = matched_on_success = 0
    abs_errors_on_success: list[float] = []
    per_field: dict[str, dict[str, Any]] = {f: {"count": 0, "evaluated": 0, "matched": 0, "within_tol_matched": 0, "abs_errors": []} for f in VITAL_ORDER}
    debug_done = False

    with out_path.open("w", encoding="utf-8") as out_f:
        for rec_idx, rec in enumerate(decoded_rows, 1):
            decode_ok = bool(rec.get("decode_ok"))
            crc_ok = bool(rec.get("crc_ok"))
            is_success_record = decode_ok and crc_ok
            if is_success_record:
                decode_success_record_count += 1
            if not crc_ok:
                crc_fail_record_count += 1

            decoded_beds = rec.get("beds") if isinstance(rec.get("beds"), dict) else {}
            decoded_packet_id = normalize_packet_id(rec.get("source_packet_id"))
            if decoded_packet_id is None:
                decoded_packet_id = normalize_packet_id(rec.get("packet_id"))

            decoded_timestamp_ms = normalize_epoch_ms(rec.get("cache_epoch_ms"))
            decoded_time_source = "cache_epoch_ms" if decoded_timestamp_ms is not None else "none"
            if decoded_timestamp_ms is None:
                decoded_timestamp_ms = normalize_epoch_ms(rec.get("epoch_ms"))
                if decoded_timestamp_ms is not None:
                    decoded_time_source = "dm_epoch_ms"
            if decoded_timestamp_ms is None:
                decoded_timestamp_ms = normalize_epoch_ms(rec.get("timestamp_ms"))
                if decoded_timestamp_ms is not None:
                    decoded_time_source = "timestamp_ms"
            if decoded_timestamp_ms is None:
                decoded_timestamp_ms = normalize_epoch_ms(rec.get("decoded_at_ms"))
                if decoded_timestamp_ms is not None:
                    decoded_time_source = "decoded_at_ms"
            if decoded_timestamp_ms is None:
                dts = parse_timestamp(rec.get("timestamp")) or infer_timestamp_from_filename(rec.get("source_image") or rec.get("image_path"))
                if dts:
                    decoded_timestamp_ms = int(round(dts.timestamp() * 1000))
                    decoded_time_source = "record_timestamp"
            decoded_time_source_counter[decoded_time_source] += 1

            nearest_truth, delta_t, matched_by = pick_truth(
                decoded_packet_id,
                decoded_timestamp_ms,
                truth_rows,
                args.tolerance_sec,
                decoded_time_source,
            )
            if nearest_truth is None:
                truth_missing_record_count += 1
            else:
                matched_by_counter[matched_by] += 1
                if delta_t is not None:
                    delta_t_values.append(delta_t)
                if args.debug_one and not debug_done:
                    print_debug_one(nearest_truth, decoded_beds)
                    debug_done = True

            for bed in BED_IDS:
                bed_decoded = decoded_beds.get(bed, {}) if isinstance(decoded_beds.get(bed), dict) else {}
                for field in VITAL_ORDER:
                    total_expected += 1
                    per_field[field]["count"] += 1

                    dec_value, dec_status = normalize_number(extract_decoded_value(bed_decoded, field))
                    truth_value = None
                    truth_status = "ok"
                    if nearest_truth is None:
                        truth_status = "truth_missing"
                    else:
                        truth_value, truth_status = normalize_number(extract_truth_value(nearest_truth, bed, field))

                    status = "ok"
                    if truth_status != "ok":
                        status = truth_status
                    elif dec_status != "ok":
                        status = dec_status

                    match = within_tol_match = False
                    abs_error = None

                    if status != "ok":
                        if status in {"missing", "truth_missing"}:
                            missing_count += 1
                        else:
                            invalid_count += 1
                    else:
                        assert dec_value is not None and truth_value is not None
                        range_cfg = vital_ranges.get(field)
                        if isinstance(range_cfg, list) and len(range_cfg) == 2 and isinstance(range_cfg[0], (int, float)) and isinstance(range_cfg[1], (int, float)):
                            if not (float(range_cfg[0]) <= dec_value <= float(range_cfg[1])):
                                status = "invalid"
                                invalid_count += 1

                    if status == "ok":
                        evaluated_count += 1
                        per_field[field]["evaluated"] += 1
                        if field in integer_fields and field not in {"TSKIN", "TRECT"}:
                            match = round(dec_value) == round(truth_value)
                        else:
                            match = dec_value == truth_value

                        eps = eps_map.get(field, 0.0)
                        try:
                            eps = float(eps)
                        except (TypeError, ValueError):
                            eps = 0.0
                        within_tol_match = abs(dec_value - truth_value) <= eps
                        abs_error = abs(dec_value - truth_value)
                        abs_errors.append(abs_error)
                        per_field[field]["abs_errors"].append(abs_error)
                        if is_success_record:
                            evaluated_on_success += 1
                            abs_errors_on_success.append(abs_error)
                        if match:
                            matched_count += 1
                            per_field[field]["matched"] += 1
                            if is_success_record:
                                matched_on_success += 1
                        if within_tol_match:
                            within_tol_matched_count += 1
                            per_field[field]["within_tol_matched"] += 1

                    row = {
                        "truth_timestamp": nearest_truth.get("timestamp_text") if nearest_truth else None,
                        "delta_t_ms": delta_t,
                        "matched_by": matched_by,
                        "bed": bed,
                        "field": field,
                        "decoded_at_ms": normalize_epoch_ms(rec.get("decoded_at_ms")),
                        "timestamp_ms": decoded_timestamp_ms,
                        "packet_id": decoded_packet_id,
                        "cache_epoch_ms": normalize_epoch_ms(rec.get("cache_epoch_ms")),
                        "source_packet_id": normalize_packet_id(rec.get("source_packet_id")),
                        "source": rec.get("source"),
                        "truth_packet_id": normalize_packet_id(nearest_truth.get("packet_id")) if nearest_truth else None,
                        "decode_ok": decode_ok,
                        "crc_ok": crc_ok,
                        "decoded_value": dec_value,
                        "truth_value": truth_value,
                        "abs_error": abs_error,
                        "match": match,
                        "within_tol_match": within_tol_match,
                        "status": status,
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

            if rec_idx % 50 == 0:
                print(f"[INFO] processed decoded records: {rec_idx}/{len(decoded_rows)}")

    per_field_summary = {
        f: {
            "count": st["count"],
            "evaluated": st["evaluated"],
            "match_rate": (st["matched"] / st["evaluated"]) if st["evaluated"] else None,
            "within_tol_match_rate": (st["within_tol_matched"] / st["evaluated"]) if st["evaluated"] else None,
            "mae": safe_mean(st["abs_errors"]),
        }
        for f, st in per_field.items()
    }

    summary = {
        "decoded_records": decoded_record_count,
        "truth_rows": len(truth_rows),
        "decode_success_records": decode_success_record_count,
        "decode_success_rate": (decode_success_record_count / decoded_record_count) if decoded_record_count else None,
        "crc_fail_records": crc_fail_record_count,
        "crc_fail_rate": (crc_fail_record_count / decoded_record_count) if decoded_record_count else None,
        "total_expected": total_expected,
        "evaluated": evaluated_count,
        "matched": matched_count,
        "within_tol_matched": within_tol_matched_count,
        "match_rate": (matched_count / evaluated_count) if evaluated_count else None,
        "match_rate_on_success": (matched_on_success / evaluated_on_success) if evaluated_on_success else None,
        "within_tol_match_rate": (within_tol_matched_count / evaluated_count) if evaluated_count else None,
        "mae": safe_mean(abs_errors),
        "mae_on_success": safe_mean(abs_errors_on_success),
        "median_abs_error": safe_median(abs_errors),
        "missing": missing_count,
        "invalid": invalid_count,
        "missing_rate": (missing_count / total_expected) if total_expected else None,
        "invalid_rate": (invalid_count / total_expected) if total_expected else None,
        "truth_missing_records": truth_missing_record_count,
        "truth_missing_rate": (truth_missing_record_count / decoded_record_count) if decoded_record_count else None,
        "matched_by": dict(matched_by_counter),
        "delta_ms": {
            "mean": safe_mean(delta_t_values),
            "median": safe_median(delta_t_values),
            "p90": percentile(delta_t_values, 90.0),
            "min": min(delta_t_values) if delta_t_values else None,
            "max": max(delta_t_values) if delta_t_values else None,
            "count": len(delta_t_values),
        },
        "delta_t_ms": {"mean": safe_mean(delta_t_values), "median": safe_median(delta_t_values), "p90": percentile(delta_t_values, 90.0), "count": len(delta_t_values)},
        "per_field": per_field_summary,
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Detailed results written: {out_path}")
    print(f"[INFO] Summary written: {summary_path}")
    print(
        "[INFO] Timestamp key usage: decoded timestamp source="
        f"{dict(decoded_time_source_counter)}, truth match key={dict(matched_by_counter)}"
    )


if __name__ == "__main__":
    main()
