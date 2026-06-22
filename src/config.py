"""
config.py — Configurações centralizadas do nó SyncP2P.

Todas as constantes de rede, caminhos e identificação ficam aqui.
Ao precisar mudar uma porta ou caminho, mude APENAS neste arquivo.

TODO (Grupo):
  - Ajustar portas se necessário
  - Adicionar suporte a SEED_NODES para Docker
  - Configurar CHUNK_SIZE ideal para transferência
"""

import os
import uuid

# ─── Identificação do Nó ────────────────────────────────────────────
NODE_ID = str(uuid.uuid4())
NODE_NAME = os.environ.get("NODE_NAME", f"Node-{NODE_ID[:8]}")

# ─── Configurações de Rede ──────────────────────────────────────────
UDP_PORT = 5000
TCP_PORT = int(os.environ.get("TCP_PORT", 5001))
BROADCAST_IP = '255.255.255.255'

# Para Docker: lista de nós sementes (formato: "host1:port1,host2:port2")
SEED_NODES = os.environ.get("SEED_NODES", "")

# ─── Caminhos ───────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_FOLDER = os.path.join(BASE_DIR, 'shared_folder')
DB_PATH = os.path.join(BASE_DIR, 'state.json')

# ─── Parâmetros de Sincronização ────────────────────────────────────
CHUNK_SIZE = 4096                # Tamanho de cada pedaço para leitura/envio
HEARTBEAT_INTERVAL = 15          # Segundos entre heartbeats
NODE_TIMEOUT = 45                # Segundos sem heartbeat → nó considerado morto
DISCOVERY_INTERVAL = 30          # Segundos entre broadcasts UDP
SYNC_INTERVAL = 300              # Segundos entre sincronizações periódicas completas
DEBOUNCE_DELAY = 0.5             # Segundos de debounce para eventos do watchdog

# ─── Garantir que a pasta compartilhada exista ──────────────────────
os.makedirs(SHARED_FOLDER, exist_ok=True)
