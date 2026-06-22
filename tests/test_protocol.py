import socket
import struct
import unittest

from src.network.protocol import (
    MAX_MESSAGE_SIZE,
    MSG_HEARTBEAT,
    ProtocolError,
    build_message,
    recv_message,
    send_message,
)


class ProtocolTests(unittest.TestCase):
    # estes testes isolam o framing antes de envolver cliente e servidor reais
    def test_round_trip(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        message = build_message(MSG_HEARTBEAT, node_id="nó-1")
        send_message(left, message)
        self.assertEqual(recv_message(right), message)

    def test_unknown_type_is_rejected(self):
        with self.assertRaises(ProtocolError):
            build_message("UNKNOWN")

    def test_invalid_json_is_rejected(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        payload = b"{invalid"
        left.sendall(struct.pack(">I", len(payload)) + payload)
        with self.assertRaises(ProtocolError):
            recv_message(right)

    def test_oversized_payload_is_rejected(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.sendall(struct.pack(">I", MAX_MESSAGE_SIZE + 1))
        with self.assertRaises(ProtocolError):
            recv_message(right)


if __name__ == "__main__":
    unittest.main()
