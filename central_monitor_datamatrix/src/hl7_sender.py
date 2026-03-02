from __future__ import annotations

import socket

SB = b"\x0b"
EB_CR = b"\x1c\x0d"


def send_mllp_message(host: str, port: int, hl7_message: str, timeout: float = 3.0) -> bool:
    ok, _ = send_mllp_message_with_error(host, port, hl7_message, timeout=timeout)
    return ok


def send_mllp_message_with_error(
    host: str,
    port: int,
    hl7_message: str,
    timeout: float = 3.0,
) -> tuple[bool, str | None]:
    payload = SB + hl7_message.encode("utf-8") + EB_CR
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(payload)
            # Some receivers only start ACK processing after EOF on request stream.
            # Half-close write side so ACK can be returned without waiting for a full close.
            s.shutdown(socket.SHUT_WR)
            ack_chunks: list[bytes] = []
            try:
                while True:
                    chunk = s.recv(1024)
                    if not chunk:
                        break
                    ack_chunks.append(chunk)
                    if EB_CR in chunk or b"\x1c" in chunk:
                        break
            except TimeoutError:
                return (
                    False,
                    "connection established but ACK timed out "
                    "(receiver may not return MLLP ACK quickly enough for current timeout)",
                )
        ack = b"".join(ack_chunks)
        if not ack:
            return False, "connection established but no ACK returned"
        return True, None
    except TimeoutError:
        return False, "connection timed out"
    except ConnectionRefusedError:
        return False, "connection refused"
    except OSError as exc:
        return False, str(exc)
