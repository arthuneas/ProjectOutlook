import hashlib
import os
import socket
import struct
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.network.protocol import (
    MSG_DELETE_NOTIFY,
    MSG_ERROR,
    MSG_FILE_REQUEST,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_INDEX_EXCHANGE,
    build_message,
    recv_message,
    send_message,
)
from src.network.tcp_client import TCPClient
from src.network.tcp_server import TCPServer
from src.sync.state_db import StateDB


class TCPIntegrationTests(unittest.TestCase):
    def setUp(self):
        # cada teste recebe pasta, banco e porta temporários para não alterar dados do projeto
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.shared = root / "shared"
        self.downloads = root / "downloads"
        self.shared.mkdir()
        self.db = StateDB(root / "state.db")
        self.server = TCPServer(
            self.db,
            host="127.0.0.1",
            port=0,
            shared_folder=self.shared,
            chunk_size=1024,
            socket_timeout=2,
        )
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()
        self.db.close()
        self.temporary.cleanup()

    def test_heartbeat(self):
        response = TCPClient.send_and_receive(
            "127.0.0.1", self.port, build_message(MSG_HEARTBEAT, node_id="client"), quiet=True
        )
        self.assertEqual(response["type"], MSG_HEARTBEAT_ACK)
        self.assertIn("node_id", response)

    def test_index_exchange_returns_local_index_and_actions(self):
        self.db.update_file_state("local.txt", "abc", 10, 3)
        response = TCPClient.send_index(
            "127.0.0.1",
            self.port,
            {"remote.txt": {"hash": "def", "timestamp": 20, "size": 4, "status": "ACTIVE"}},
            node_id="remote",
            tcp_port=6000,
        )
        self.assertEqual(response["type"], MSG_INDEX_EXCHANGE)
        self.assertIn("local.txt", response["files"])
        self.assertEqual(response["actions"]["download"], ["remote.txt"])

    def test_binary_file_transfer(self):
        # o tamanho escolhido força a passagem por vários chunks
        source = self.shared / "arquivo.bin"
        source.write_bytes(os.urandom(128 * 1024 + 17))
        destination = self.downloads / source.name
        self.assertTrue(TCPClient.request_file("127.0.0.1", self.port, source.name, destination))
        self.assertEqual(self._hash(source), self._hash(destination))

    def test_empty_file_transfer(self):
        source = self.shared / "vazio.txt"
        source.write_bytes(b"")
        destination = self.downloads / source.name
        self.assertTrue(TCPClient.request_file("127.0.0.1", self.port, source.name, destination))
        self.assertEqual(destination.read_bytes(), b"")

    def test_missing_file_does_not_create_destination(self):
        destination = self.downloads / "missing.txt"
        self.assertFalse(TCPClient.request_file("127.0.0.1", self.port, "missing.txt", destination))
        self.assertFalse(destination.exists())

    def test_path_traversal_is_rejected(self):
        with socket.create_connection(("127.0.0.1", self.port), timeout=2) as sock:
            send_message(sock, build_message(MSG_FILE_REQUEST, filename="../secret.txt"))
            response = recv_message(sock)
        self.assertEqual(response["type"], MSG_ERROR)

    def test_invalid_message_does_not_stop_listener(self):
        # um cliente inválido não pode derrubar o atendimento dos clientes seguintes
        with socket.create_connection(("127.0.0.1", self.port), timeout=2) as sock:
            payload = b"{invalid"
            sock.sendall(struct.pack(">I", len(payload)) + payload)
            self.assertEqual(recv_message(sock)["type"], MSG_ERROR)
        self.test_heartbeat()

    def test_delete_notify_creates_tombstone(self):
        source = self.shared / "old.txt"
        source.write_text("old", encoding="utf-8")
        self.db.update_file_state("old.txt", "hash", 1, 3)
        message = build_message(
            MSG_DELETE_NOTIFY,
            node_id="remote",
            filename="old.txt",
            timestamp=2,
        )
        self.assertTrue(TCPClient.send_message("127.0.0.1", self.port, message, quiet=True))
        deadline = time.time() + 1
        while source.exists() and time.time() < deadline:
            time.sleep(0.01)
        self.assertFalse(source.exists())
        self.assertEqual(self.db.get_file_state("old.txt")["status"], "DELETED")

    def test_multiple_clients(self):
        # vários heartbeats simultâneos verificam o isolamento entre workers
        def heartbeat(_):
            return TCPClient.send_and_receive(
                "127.0.0.1", self.port, build_message(MSG_HEARTBEAT, node_id="client"), quiet=True
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            responses = list(executor.map(heartbeat, range(20)))
        self.assertTrue(all(response["type"] == MSG_HEARTBEAT_ACK for response in responses))

    @staticmethod
    def _hash(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
