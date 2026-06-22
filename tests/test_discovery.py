import json
import threading
import unittest
from unittest.mock import patch

from src.network.discovery import DiscoveryManager
from src.network.protocol import MSG_HELLO, MSG_HELLO_ACK


class FakeDatagramSocket:
    def __init__(self):
        self.sent = []

    def sendto(self, payload, target):
        self.sent.append((json.loads(payload.decode("utf-8")), target))


class DiscoveryTests(unittest.TestCase):
    def test_hello_registers_peer_and_sends_ack(self):
        manager = DiscoveryManager(node_id="local", node_name="Local", tcp_port=5001, udp_port=5000)
        fake_socket = FakeDatagramSocket()
        hello = json.dumps(
            {"type": MSG_HELLO, "node_id": "remote", "name": "Remote", "tcp_port": 6001}
        ).encode("utf-8")

        manager._handle_datagram(hello, ("10.0.0.2", 40000), fake_socket)

        self.assertEqual(manager.get_active_nodes()["remote"]["ip"], "10.0.0.2")
        self.assertEqual(fake_socket.sent[0][0]["type"], MSG_HELLO_ACK)
        self.assertEqual(fake_socket.sent[0][1], ("10.0.0.2", 5000))

    def test_ack_registers_peer_without_reply_loop(self):
        manager = DiscoveryManager(node_id="local", tcp_port=5001)
        fake_socket = FakeDatagramSocket()
        ack = json.dumps(
            {"type": MSG_HELLO_ACK, "node_id": "remote", "name": "Remote", "tcp_port": 6001}
        ).encode("utf-8")

        manager._handle_datagram(ack, ("10.0.0.2", 5000), fake_socket)

        self.assertIn("remote", manager.get_active_nodes())
        self.assertEqual(fake_socket.sent, [])

    def test_own_hello_is_ignored(self):
        manager = DiscoveryManager(node_id="local", tcp_port=5001)
        hello = json.dumps(
            {"type": MSG_HELLO, "node_id": "local", "name": "Local", "tcp_port": 5001}
        ).encode("utf-8")
        manager._handle_datagram(hello, ("127.0.0.1", 5000), FakeDatagramSocket())
        self.assertEqual(manager.get_active_nodes(), {})

    def test_seed_is_loaded_and_notifies_callback(self):
        event = threading.Event()
        received = []

        def callback(node_id, info):
            received.append((node_id, info))
            event.set()

        manager = DiscoveryManager(callback, node_id="local", seed_nodes="peer:6001")
        with patch("src.network.discovery.socket.gethostbyname", return_value="10.0.0.2"):
            manager._load_seeds()

        self.assertTrue(event.wait(1))
        self.assertEqual(received[0][0], "seed:peer:6001")
        self.assertEqual(received[0][1]["tcp_port"], 6001)


if __name__ == "__main__":
    unittest.main()
