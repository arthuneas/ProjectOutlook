import os
import uuid

# Configurações de Rede
UDP_PORT = 5000
TCP_PORT = 5001 # Deve ser dinâmico ou configurável por nó se rodar na mesma máquina fora do docker
BROADCAST_IP = '255.255.255.255'

# Identificação do Nó
NODE_ID = str(uuid.uuid4())
NODE_NAME = os.environ.get("NODE_NAME", f"Node-{NODE_ID[:8]}")

# Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_FOLDER = os.path.join(BASE_DIR, 'shared_folder')
DB_PATH = os.path.join(BASE_DIR, 'state.json') # ou .sqlite3

# Garante que a pasta compartilhada exista
os.makedirs(SHARED_FOLDER, exist_ok=True)
