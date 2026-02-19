from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime

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


def build_message(bed: str, msg_id: int) -> str:
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    header = (
        f"MSH|^~\\&|GEN|ICU|MON|ICU|{now}||ORU^R01|MSG{msg_id:06d}|P|2.4\r"
        "PID|1||12345||DOE^JOHN||19800101|M\r"
        f"PV1|1|I|WARD^A^{bed}\r"
        "OBR|1|||VITALS\r"
    )

    obx_segments: list[str] = []
    for index, (code, label, unit, minimum, maximum) in enumerate(VITAL_SPECS, start=1):
        value = random.randint(minimum, maximum)
        value_text = str(value / 10) if code in {"TEMP", "FiO2", "CVP", "ICP", "CO2", "O2_FLOW", "BT", "etCO2"} else str(value)
        obx_segments.append(f"OBX|{index}|NM|{code}^{label}||{value_text}|{unit}|||N\r")

    return header + "".join(obx_segments)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--count", type=int, default=-1, help="送信ループ回数(-1で無限)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    msg_id = 1
    beds = [f"BED{i:02d}" for i in range(1, 7)]
    loop = 0
    while args.count < 0 or loop < args.count:
        for bed in beds:
            ok = send_mllp_message(args.host, args.port, build_message(bed, msg_id))
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
        loop += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
