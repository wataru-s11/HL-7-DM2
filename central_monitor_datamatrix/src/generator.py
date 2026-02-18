from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime

from hl7_sender import send_mllp_message

logger = logging.getLogger(__name__)


def build_message(bed: str, msg_id: int) -> str:
    hr = random.randint(55, 110)
    spo2 = random.randint(90, 100)
    rr = random.randint(10, 24)
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    return (
        f"MSH|^~\\&|GEN|ICU|MON|ICU|{now}||ORU^R01|MSG{msg_id:06d}|P|2.4\r"
        "PID|1||12345||DOE^JOHN||19800101|M\r"
        f"PV1|1|I|WARD^A^{bed}\r"
        "OBR|1|||VITALS\r"
        f"OBX|1|NM|HR^HeartRate||{hr}|bpm|||N\r"
        f"OBX|2|NM|SPO2^SpO2||{spo2}|%|||N\r"
        f"OBX|3|NM|RESP^RespRate||{rr}|rpm|||N\r"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--count", type=int, default=-1, help="送信ループ回数(-1で無限)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    msg_id = 1
    beds = ["BED01", "BED02"]
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
