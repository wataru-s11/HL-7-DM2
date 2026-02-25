from __future__ import annotations

import argparse
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hl7_sender import send_mllp_message

logger = logging.getLogger(__name__)

VITAL_SPECS = [
    ("HR", "HeartRate", "bpm", 55, 110),
    ("SpO2", "SpO2", "%", 90, 100),
    ("RR", "RespRate", "rpm", 10, 24),
    ("TEMP", "Temperature", "C", 360, 390),
    ("SBP", "SystolicBP", "mmHg", 90, 160),
    ("DBP", "DiastolicBP", "mmHg", 50, 100),
    ("MAP", "MeanArterialPressure", "mmHg", 60, 120),
    ("PR", "PulseRate", "bpm", 55, 120),
    ("etCO2", "EndTidalCO2", "mmHg", 25, 50),
    ("FiO2", "InspiredO2", "%", 210, 1000),
    ("NIBP_SYS", "NIBP_Systolic", "mmHg", 90, 160),
    ("NIBP_DIA", "NIBP_Diastolic", "mmHg", 50, 100),
    ("NIBP_MAP", "NIBP_Mean", "mmHg", 60, 120),
    ("CVP", "CentralVenousPressure", "mmHg", 20, 150),
    ("ICP", "IntracranialPressure", "mmHg", 50, 250),
    ("RESP_RATE", "RespRateMonitor", "rpm", 10, 30),
    ("PULSE", "Pulse", "bpm", 55, 120),
    ("CO2", "CO2", "mmHg", 250, 500),
    ("O2_FLOW", "O2Flow", "L/min", 0, 150),
    ("BT", "BodyTemperature", "C", 360, 390),
]


JST = timezone(timedelta(hours=9))


def _to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lowered = value.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value}")


def build_patient(bed: str) -> dict[str, str]:
    bed_number = bed[-2:]
    return {
        "patient_id": f"P{bed_number}",
        "name": f"DOE^BED{bed_number}",
        "dob": "19800101",
    }


def build_bed_payload() -> dict[str, dict[str, Any]]:
    vitals: dict[str, dict[str, str | float]] = {}
    for code, _, unit, minimum, maximum in VITAL_SPECS:
        raw_value = random.randint(minimum, maximum)
        value = (
            raw_value / 10
            if code in {"TEMP", "FiO2", "CVP", "ICP", "CO2", "O2_FLOW", "BT", "etCO2"}
            else float(raw_value)
        )
        vitals[code] = {"value": value, "unit": unit, "flag": ""}
    return {"vitals": vitals}


def build_message(bed: str, msg_id: int, patient: dict[str, str], vitals: dict[str, dict[str, str | float]]) -> str:
    now = datetime.now(JST).strftime("%Y%m%d%H%M%S")
    header = (
        f"MSH|^~\\&|GEN|ICU|MON|ICU|{now}||ORU^R01|MSG{msg_id:06d}|P|2.4\r"
        f"PID|1||{patient['patient_id']}||{patient['name']}||{patient['dob']}|M\r"
        f"PV1|1|I|WARD^A^{bed}\r"
        "OBR|1|||VITALS\r"
    )

    obx_segments: list[str] = []
    for index, (code, label, _, _, _) in enumerate(VITAL_SPECS, start=1):
        vital = vitals[code]
        value_text = str(vital["value"])
        unit = str(vital["unit"])
        obx_segments.append(f"OBX|{index}|NM|{code}^{label}||{value_text}|{unit}|||N\r")

    return header + "".join(obx_segments)


def write_truth_record(out_path: str, record: dict[str, Any], append: bool) -> bool:
    try:
        destination = Path(out_path).expanduser()
        if destination.parent != Path(""):
            destination.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        with destination.open(mode=mode, encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return True
    except Exception:
        logger.exception("failed to write truth JSONL: path=%s", out_path)
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--count", type=int, default=-1, help="送信ループ回数(-1で無限)")
    ap.add_argument("--truth-out", help="truth JSONLの出力先")
    ap.add_argument("--append-truth", action="store_true", help="truth JSONLに追記する")
    ap.add_argument("--truth-every-n", type=int, default=1, help="何回に1回truthを書き込むか")
    ap.add_argument(
        "--truth-include-hl7",
        nargs="?",
        const=True,
        default=False,
        type=_to_bool,
        help="trueなら送信したHL7全文をtruthに含める",
    )
    args = ap.parse_args()

    if args.truth_every_n < 1:
        ap.error("--truth-every-n must be >= 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    msg_id = 1
    beds = [f"BED{i:02d}" for i in range(1, 7)]
    loop = 0
    truth_append_mode = bool(args.append_truth)
    while args.count < 0 or loop < args.count:
        cycle_ts = datetime.now(JST)
        cycle_iso = cycle_ts.isoformat(timespec="milliseconds")
        cycle_epoch_ms = int(cycle_ts.timestamp() * 1000)
        cycle_beds: dict[str, dict[str, Any]] = {}
        hl7_messages: list[str] = []

        for bed in beds:
            payload = build_bed_payload()
            patient = build_patient(bed)
            message = build_message(bed, msg_id, patient, payload["vitals"])
            cycle_beds[bed] = {"patient": patient, "vitals": payload["vitals"]}
            if args.truth_include_hl7:
                hl7_messages.append(message)

            ok = send_mllp_message(args.host, args.port, message)
            if ok:
                logger.info("sent message_id=MSG%06d bed=%s", msg_id, bed)
            else:
                logger.warning(
                    "send failed message_id=MSG%06d bed=%s (receiver not reachable at %s:%d)",
                    msg_id,
                    bed,
                    args.host,
                    args.port,
                )
            msg_id += 1

        if args.truth_out and ((loop + 1) % args.truth_every_n == 0):
            truth_record: dict[str, Any] = {
                "ts": cycle_iso,
                "epoch_ms": cycle_epoch_ms,
                "beds": cycle_beds,
            }
            if args.truth_include_hl7:
                truth_record["hl7"] = "\n".join(hl7_messages)

            write_truth_record(args.truth_out, truth_record, append=truth_append_mode)
            truth_append_mode = True

        loop += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
