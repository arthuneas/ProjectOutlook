import time
import threading
from config import NODE_NAME
from network.discovery import DiscoveryManager
from network.tcp_server import TCPServer
from sync.watcher import DirectoryWatcher
from ui.cli import log_info

def main():
    log_info(f"Iniciando {NODE_NAME}...")

    # 1. Inicia o monitoramento de arquivos
    watcher = DirectoryWatcher()
    watcher_thread = threading.Thread(target=watcher.start, daemon=True)
    watcher_thread.start()

    # 2. Inicia o Servidor TCP para receber conexões
    tcp_server = TCPServer()
    tcp_thread = threading.Thread(target=tcp_server.start, daemon=True)
    tcp_thread.start()

    # 3. Inicia o módulo de descoberta UDP
    discovery = DiscoveryManager()
    discovery_thread = threading.Thread(target=discovery.start, daemon=True)
    discovery_thread.start()

    # Loop principal (mantém o processo vivo e pode processar a UI do CLI)
    try:
        while True:
            time.sleep(1)
            # Aqui pode ser implementada a interface do terminal para
            # mostrar status ou enviar comandos manuais.
    except KeyboardInterrupt:
        log_info("Desligando o nó...")

if __name__ == "__main__":
    main()
