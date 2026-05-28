import socket
import threading
import time
import json
from config import UDP_PORT, TCP_PORT, BROADCAST_IP, NODE_ID, NODE_NAME
from ui.cli import log_info

class DiscoveryManager:
    def __init__(self):
        self.known_nodes = {} # {node_id: {"ip": ip, "tcp_port": port, "name": name}}
        
    def start(self):
        # Inicia thread para escutar pacotes de descoberta
        listener = threading.Thread(target=self._listen_for_broadcasts, daemon=True)
        listener.start()
        
        # Envia broadcast inicial para avisar que entrou na rede
        self._broadcast_presence()
        
        # Mantém enviando "heartbeats" periodicamente (opcional)
        while True:
            time.sleep(30)
            self._broadcast_presence()

    def _listen_for_broadcasts(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Em Windows, o bind no broadcast pode ser diferente, mas isso é a base.
        sock.bind(('', UDP_PORT))
        
        while True:
            data, addr = sock.recvfrom(1024)
            try:
                msg = json.loads(data.decode('utf-8'))
                if msg.get('node_id') != NODE_ID:
                    if msg.get('type') == 'HELLO':
                        log_info(f"Descoberto novo nó: {msg.get('name')} em {addr[0]}")
                        self.known_nodes[msg['node_id']] = {
                            "ip": addr[0],
                            "tcp_port": msg.get('tcp_port'),
                            "name": msg.get('name')
                        }
                        # Aqui deve-se responder com um HELLO_ACK via TCP ou UDP
            except Exception as e:
                pass

    def _broadcast_presence(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        msg = json.dumps({
            "type": "HELLO",
            "node_id": NODE_ID,
            "name": NODE_NAME,
            "tcp_port": TCP_PORT
        })
        
        sock.sendto(msg.encode('utf-8'), (BROADCAST_IP, UDP_PORT))
        sock.close()
