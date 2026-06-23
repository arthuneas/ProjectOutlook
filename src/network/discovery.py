"""
discovery.py — Módulo de Descoberta de Nós via UDP Broadcast e seed nodes.

Responsabilidades:
  1. Enviar broadcasts UDP periódicos (HELLO) anunciando presença na rede
  2. Escutar broadcasts de outros nós e registrá-los em known_nodes
  3. Manter a lista known_nodes atualizada (thread-safe com Lock)
  4. Notificar o sistema quando um novo nó é descoberto (callback)

Como funciona:
  - Ao iniciar, o nó "grita" na rede: "Eu existo, minha porta TCP é X"
  - Outros nós escutam e registram o IP e porta do novo nó
  - Broadcasts são repetidos periodicamente (heartbeat de descoberta)

TODO (Grupo):
  - Implementar DiscoveryManager com known_nodes protegido por Lock
  - Implementar _listen_for_broadcasts() — socket UDP, bind, recvfrom loop
  - Implementar _broadcast_presence() — socket UDP com SO_BROADCAST
  - Implementar _broadcast_loop() — loop periódico
  - Implementar get_active_nodes() — retorna cópia do dicionário
  - Implementar remove_node(node_id) — remove nó que falhou
  - Adicionar callback on_new_node para sincronização automática
  - IMPORTANTE: Usar threading.Lock() para proteger known_nodes
  - IMPORTANTE: Configurar SO_REUSEADDR e SO_REUSEPORT no socket
"""

import json
import socket
import threading
import time

from ..config import (
    BROADCAST_IP,
    DISCOVERY_INTERVAL,
    NODE_ID,
    NODE_NAME,
    NODE_TIMEOUT,
    SEED_NODES,
    TCP_PORT,
    UDP_PORT,
)
from ..ui.cli import log_info, log_warn


class DiscoveryManager:
    def __init__(self, on_new_node=None):
        self.on_new_node = on_new_node       # chamado quando um nó novo é encontrado
        self.known_nodes = {}                # node_id -> {name, ip, tcp_port, last_seen}
        self.lock = threading.Lock()         # protege known_nodes contra acesso simultâneo
        self.stop_event = threading.Event()  # sinaliza encerramento para todas as threads
        self.udp_sock = None                 # socket de escuta principal
        self.threads = []                    # threads internas: listen, broadcast, cleanup

    # ──────────────────────────────────────────────────────────────
    # CICLO DE VIDA
    # ──────────────────────────────────────────────────────────────

    def start(self):
        self.stop_event.clear() 

        # socket compartilhado apenas para escuta; broadcast usa socket próprio
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            # permite múltiplos processos na mesma porta 
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.udp_sock.bind(("", UDP_PORT)) #vincula a porta 5000 em todas as interfaces de rede

        for target, name in [
            (self._listen_loop,    "udp-listening"),
            (self._broadcast_loop, "udp-broadcasting"),
            (self._cleanup_loop,   "discovery-cleanup"),
        ]:
            t = threading.Thread(target=target, name=name, daemon=True)
            self.threads.append(t)
            t.start()

        if SEED_NODES:
            # seed nodes são processados em thread separada para não atrasar o primeiro broadcast
            threading.Thread(target=self._init_seed_nodes, name="seed-init", daemon=True).start()

        log_info("Gerenciador de descoberta UDP ativado.")

    def stop(self):
        self.stop_event.set()
        if self.udp_sock:  #socket principal de escuta foi criado, então podemos fechá-lo para interromper a thread de escuta
            self.udp_sock.close()
        log_info("Gerenciador de descoberta finalizado.")

    # ──────────────────────────────────────────────────────────────
    # THREADS INTERNAS
    # ──────────────────────────────────────────────────────────────

    def _broadcast_loop(self):
        """Anuncia que está na rede enviando HELLO para o endereço de broadcast."""
        payload = json.dumps({
            "type":     "HELLO",
            "node_id":  NODE_ID,
            "name":     NODE_NAME,
            "tcp_port": TCP_PORT,
        }).encode("utf-8")

        # socket separado para broadcast, pois SO_BROADCAST não é compatível com SO_REUSEPORT em alguns sistemas
        bcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            while not self.stop_event.is_set():
                try:
                    bcast_sock.sendto(payload, (BROADCAST_IP, UDP_PORT)) #envia a msg pro broadacast
                except OSError:
                    pass
                # espera em fatias de 0,5 s para reagir rápido ao stop_event
                for _ in range(int(DISCOVERY_INTERVAL * 2)):
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.5)
        finally:
            bcast_sock.close()

    def _listen_loop(self):
        """Recebe pacotes UDP e registra nós que enviaram HELLO."""
        # timeout curto para que o loop verifique stop_event com frequência
        self.udp_sock.settimeout(1.0)
        while not self.stop_event.is_set():
            try:
                data, addr = self.udp_sock.recvfrom(1024)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "HELLO":
                    self._register_or_update_node(msg, addr[0])
            except (socket.timeout, json.JSONDecodeError, UnicodeDecodeError):
                continue
            except OSError:
                break

    def _cleanup_loop(self):
        """Remove nós que ultrapassaram NODE_TIMEOUT sem enviar HELLO."""
        while not self.stop_event.is_set():
            time.sleep(10)
            now = time.time()
            expired = []

            with self.lock:
                for n_id, info in self.known_nodes.items():
                    if now - info["last_seen"] > NODE_TIMEOUT:  #tempo inativo estourou node_timeout(45s)?
                        expired.append((n_id, info["name"]))
                for n_id, _ in expired:
                    del self.known_nodes[n_id]

            for _, name in expired:
                log_warn(f"nó '{name}' removido por inatividade (>{NODE_TIMEOUT}s).")

    def _init_seed_nodes(self):
        """Registra peers estáticos definidos em SEED_NODES antes do primeiro broadcast."""
        for seed in SEED_NODES.split(","):
            seed = seed.strip()
            if not seed:
                continue
            try:
                host, port = seed.split(":")
                ip = socket.gethostbyname(host)
                pseudo_msg = {
                    "node_id":  f"seed-{host}",
                    "name":     host,
                    "tcp_port": int(port),
                }
                self._register_or_update_node(pseudo_msg, ip)
            except Exception as exc:
                log_warn(f"seed node '{seed}' inacessível: {exc}")

    # ──────────────────────────────────────────────────────────────
    # GERENCIAMENTO DA TABELA DE NÓS
    # ──────────────────────────────────────────────────────────────

    def _register_or_update_node(self, msg, remote_ip):
        remote_id = msg.get("node_id")
        if not remote_id or remote_id == NODE_ID:
            return  # descarta msgs nulas ou de si mesmo

        is_new = False
        node_info = {
            "name":      msg.get("name"),
            "tcp_port":  int(msg.get("tcp_port")),
            "ip":        remote_ip,
            "last_seen": time.time(),
        }

        with self.lock:
            if remote_id not in self.known_nodes:
                is_new = True
            self.known_nodes[remote_id] = node_info
            # node_info é salvo dentro do lock e reutilizado fora para evitar race condition:
            # sem isso, _cleanup_loop poderia deletar o nó entre o 'with' e o callback abaixo

        if is_new:
            log_info(f"nó descoberto: '{node_info['name']}' ({remote_ip}:{node_info['tcp_port']})")
            if self.on_new_node:
                self.on_new_node(remote_id, node_info, remote_ip)

    def get_active_nodes(self):
        """Retorna cópia thread-safe da tabela de nós conhecidos."""
        with self.lock:
            return dict(self.known_nodes)

    def remove_node(self, node_id):
        """Remove um nó manualmente (ex: ao receber NODE_LEAVING)."""
        with self.lock:
            self.known_nodes.pop(node_id, None)