from __future__ import annotations

import socket

SB = b"\x0b"
EB_CR = b"\x1c\x0d"


def send_mllp_message(host: str, port: int, hl7_message: str, timeout: float = 3.0) -> bool:
    payload = SB + hl7_message.encode("utf-8") + EB_CR
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(payload)
            _ = s.recv(1024)
        return True
    except OSError:
        return False
