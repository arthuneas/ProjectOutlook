"""configuração central do nó syncp2p."""

import os
import uuid

# a identidade pode ser fornecida pelo ambiente para permanecer estável em testes e containers
NODE_ID = os.environ.get("NODE_ID", str(uuid.uuid4()))
NODE_NAME = os.environ.get("NODE_NAME", f"Node-{NODE_ID[:8]}")

# todas as portas podem ser alteradas sem editar o código-fonte
UDP_PORT = int(os.environ.get("UDP_PORT", 5000))
TCP_PORT = int(os.environ.get("TCP_PORT", 5001))
BROADCAST_IP = os.environ.get("BROADCAST_IP", "255.255.255.255")
SEED_NODES = os.environ.get("SEED_NODES", "")

# caminhos configuráveis permitem executar vários nós com pastas e bancos separados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_FOLDER = os.path.abspath(os.environ.get("SHARED_FOLDER", os.path.join(BASE_DIR, "shared_folder")))
DB_PATH = os.path.abspath(os.environ.get("DB_PATH", os.path.join(BASE_DIR, "state.db")))

# estes valores controlam rede, chunks e os intervalos das próximas etapas
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 4096))
SOCKET_TIMEOUT = float(os.environ.get("SOCKET_TIMEOUT", 10))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", 15))
NODE_TIMEOUT = float(os.environ.get("NODE_TIMEOUT", 45))
DISCOVERY_INTERVAL = float(os.environ.get("DISCOVERY_INTERVAL", 30))
SYNC_INTERVAL = float(os.environ.get("SYNC_INTERVAL", 300))
DEBOUNCE_DELAY = float(os.environ.get("DEBOUNCE_DELAY", 0.5))

# a pasta precisa existir antes do scan inicial e do servidor de arquivos
os.makedirs(SHARED_FOLDER, exist_ok=True)
