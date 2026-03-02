from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime

from hl7_sender import send_mllp_message


logger = logging.getLogger(__name__)

VITAL_SPECS = [
    ("HR", "HeartRate", "bpm", 50.0, 180.0, 0),
    ("ART_S", "ArterialSystolic", "mmHg", 40.0, 140.0, 0),
    ("ART_D", "ArterialDiastolic", "mmHg", 20.0, 90.0, 0),
    ("ART_M", "ArterialMean", "mmHg", 30.0, 110.0, 0),
    ("CVP_M", "CentralVenousMean", "mmHg", -5.0, 25.0, 0),
    ("RAP_M", "RightAtrialMean", "mmHg", -5.0, 20.0, 0),
    ("SpO2", "SpO2", "%", 85.0, 100.0, 0),
    ("TSKIN", "SkinTemperature", "C", 30.0, 40.0, 1),
    ("TRECT", "RectalTemperature", "C", 34.0, 41.0, 1),
    ("rRESP", "RawRespirationRate", "rpm", 0.0, 60.0, 0),
    ("EtCO2", "EndTidalCO2", "mmHg", 15.0, 60.0, 0),
    ("RR", "RespiratoryRate", "rpm", 5.0, 60.0, 0),
    ("VTe", "ExpiratoryTidalVolume", "mL", 0.0, 800.0, 0),
    ("VTi", "InspiratoryTidalVolume", "mL", 0.0, 800.0, 0),
    ("Ppeak", "PeakAirwayPressure", "cmH2O", 5.0, 50.0, 0),
    ("PEEP", "PositiveEndExpiratoryPressure", "cmH2O", 0.0, 20.0, 0),
    ("O2conc", "OxygenConcentration", "%", 21.0, 100.0, 0),
    ("NO", "NitricOxide", "ppm", 0.0, 40.0, 0),
    ("BSR1", "BurstSuppressionRatio1", "%", 0.0, 100.0, 0),
    ("BSR2", "BurstSuppressionRatio2", "%", 0.0, 100.0, 0),
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
    for index, (code, label, unit, minimum, maximum, decimals) in enumerate(VITAL_SPECS, start=1):
        if decimals > 0:
            value = round(random.uniform(minimum, maximum), decimals)
        else:
            value = float(random.randint(int(minimum), int(maximum)))
        value_text = str(value)
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
