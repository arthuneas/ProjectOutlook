"""descoberta inicial de peers por udp e seed nodes."""

import json
import socket
import threading
import time

from .protocol import MSG_HELLO, MSG_HELLO_ACK, build_message
from ..config import (
    BROADCAST_IP,
    DISCOVERY_INTERVAL,
    NODE_ID,
    NODE_NAME,
    SEED_NODES,
    TCP_PORT,
    UDP_PORT,
)
from ..ui.cli import log_error, log_info, log_warn


class DiscoveryManager:
    """mantém os peers vistos e anuncia a presença deste nó."""

    def __init__(
        self,
        on_new_node=None,
        node_id=NODE_ID,
        node_name=NODE_NAME,
        tcp_port=TCP_PORT,
        udp_port=UDP_PORT,
        broadcast_ip=BROADCAST_IP,
        seed_nodes=SEED_NODES,
        discovery_interval=DISCOVERY_INTERVAL,
    ):
        self.on_new_node = on_new_node
        self.node_id = node_id
        self.node_name = node_name
        self.tcp_port = int(tcp_port)
        self.udp_port = int(udp_port)
        self.broadcast_ip = broadcast_ip
        self.seed_nodes = seed_nodes
        self.discovery_interval = float(discovery_interval)

        # listener, callback e main podem consultar ou atualizar o catálogo ao mesmo tempo
        self.known_nodes = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.sock = None
        self.threads = []

    def start(self):
        if any(thread.is_alive() for thread in self.threads):
            return
        self.stop_event.clear()
        self.ready_event.clear()
        listener = threading.Thread(target=self._listen_loop, name="udp-listener", daemon=True)
        broadcaster = threading.Thread(target=self._broadcast_loop, name="udp-broadcast", daemon=True)
        self.threads = [listener, broadcaster]

        # o listener sobe antes do primeiro anúncio para receber respostas imediatamente
        listener.start()
        self.ready_event.wait(timeout=2)
        self._load_seeds()
        broadcaster.start()
        log_info(f"descoberta udp iniciada na porta {self.udp_port}")

    def _load_seeds(self):
        # seeds são úteis no docker e em redes onde o broadcast não alcança os peers
        for seed in filter(None, (item.strip() for item in self.seed_nodes.split(","))):
            try:
                host, port_text = seed.rsplit(":", 1)
                port = int(port_text)
                if not 1 <= port <= 65535:
                    raise ValueError("porta fora do intervalo")
                ip = socket.gethostbyname(host)
                self.register_node(
                    f"seed:{host}:{port}",
                    {"name": host, "ip": ip, "tcp_port": port, "source": "seed"},
                )
            except (OSError, ValueError) as exc:
                log_warn(f"seed inválido {seed!r}: {exc}")

    def _listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock = sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            sock.bind(("", self.udp_port))
            sock.settimeout(0.5)
            self.ready_event.set()
        except OSError as exc:
            self.ready_event.set()
            log_error(f"não foi possível abrir a descoberta udp: {exc}")
            sock.close()
            self.sock = None
            return

        while not self.stop_event.is_set():
            try:
                payload, address = sock.recvfrom(4096)
                self._handle_datagram(payload, address, sock)
            except socket.timeout:
                continue
            except OSError:
                break
            except ValueError as exc:
                log_warn(f"datagrama de descoberta ignorado: {exc}")

    def _handle_datagram(self, payload, address, sock=None):
        try:
            message = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("json udp inválido") from exc
        if not isinstance(message, dict):
            raise ValueError("mensagem udp precisa ser um objeto")
        if message.get("type") not in {MSG_HELLO, MSG_HELLO_ACK}:
            return

        remote_id = message.get("node_id")
        remote_port = message.get("tcp_port")
        if not isinstance(remote_id, str) or not remote_id or remote_id == self.node_id:
            return
        if not isinstance(remote_port, int) or isinstance(remote_port, bool) or not 1 <= remote_port <= 65535:
            raise ValueError("tcp_port inválida")

        self.register_node(
            remote_id,
            {
                "name": message.get("name", "Node"),
                "ip": address[0],
                "tcp_port": remote_port,
                "source": "udp",
            },
        )

        # somente hello recebe resposta; hello_ack não pode criar um ciclo de respostas
        if message["type"] == MSG_HELLO and (sock or self.sock):
            ack = self._encode_udp(
                build_message(
                    MSG_HELLO_ACK,
                    node_id=self.node_id,
                    name=self.node_name,
                    tcp_port=self.tcp_port,
                    version=1,
                )
            )
            try:
                (sock or self.sock).sendto(ack, (address[0], self.udp_port))
            except OSError:
                pass

    def _broadcast_loop(self):
        hello = self._encode_udp(
            build_message(
                MSG_HELLO,
                node_id=self.node_id,
                name=self.node_name,
                tcp_port=self.tcp_port,
                version=1,
            )
        )
        while not self.stop_event.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                    sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    sender.sendto(hello, (self.broadcast_ip, self.udp_port))
            except OSError as exc:
                if not self.stop_event.is_set():
                    log_warn(f"falha no broadcast udp: {exc}")
            self.stop_event.wait(self.discovery_interval)

    @staticmethod
    def _encode_udp(message):
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(payload) > 4096:
            raise ValueError("mensagem udp excede 4096 bytes")
        return payload

    def register_node(self, node_id, info, notify=True):
        if node_id == self.node_id:
            return False
        if any(field not in info for field in ("name", "ip", "tcp_port")):
            raise ValueError("informações incompletas do peer")
        with self.lock:
            is_new = node_id not in self.known_nodes
            self.known_nodes[node_id] = {**info, "last_seen": time.time()}
            callback_info = self.known_nodes[node_id].copy()
        if is_new:
            log_info(f"peer descoberto: {info['name']} ({info['ip']}:{info['tcp_port']})")
            if notify and self.on_new_node:
                threading.Thread(
                    target=self._run_callback,
                    args=(node_id, callback_info),
                    name=f"peer-callback-{node_id[:8]}",
                    daemon=True,
                ).start()
        return is_new

    def _run_callback(self, node_id, info):
        try:
            self.on_new_node(node_id, info)
        except Exception as exc:
            log_error(f"callback do peer {node_id} falhou: {exc}")

    def get_active_nodes(self):
        with self.lock:
            return {node_id: info.copy() for node_id, info in self.known_nodes.items()}

    def remove_node(self, node_id):
        with self.lock:
            return self.known_nodes.pop(node_id, None) is not None

    def stop(self):
        self.stop_event.set()
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
        for thread in self.threads:
            thread.join(timeout=2)
        self.threads.clear()
