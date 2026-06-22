"""

main.py — Ponto de entrada e orquestrador do nó SyncP2P.

Este é o "cola" que conecta todos os módulos:
  1. Carrega configurações
  2. Inicializa todos os componentes (state_db, file_manager, reconciler, etc.)
  3. Faz scan inicial da shared_folder e popula o state_db
  4. Inicia todas as threads (discovery, tcp_server, watcher, heartbeat, periodic_sync)
  5. Define callbacks (on_new_node → sincronizar, on_file_change → notificar peers)
  6. Mantém o processo vivo no loop principal
  7. Implementa shutdown gracioso (NODE_LEAVING para todos os peers)

THREADS:
  Thread 1: DiscoveryManager.start()     — UDP broadcast + escuta
  Thread 2: TCPServer.start()            — Escuta TCP para conexões
  Thread 3: DirectoryWatcher.start()     — Watchdog na shared_folder
  Thread 4: heartbeat_loop()             — Envia HEARTBEAT periódico para peers
  Thread 5: periodic_sync_loop()         — INDEX_EXCHANGE periódico (a cada 5 min)

COMPARTILHAMENTO DE ESTADO:
  - known_nodes → protegido com Lock dentro de DiscoveryManager
  - state_db → protegido com Lock dentro de StateDB
  - files_being_synced → set() com Lock, compartilhado entre watcher e tcp_server

TODO (Grupo):
  - Instanciar todos os componentes
  - Implementar scan_initial() — varre shared_folder e popula state_db
  - Implementar on_new_node_discovered(node_id, node_info) — inicia INDEX_EXCHANGE
  - Implementar on_file_change(filename, action) — notifica todos os peers
  - Implementar heartbeat_loop() — HEARTBEAT periódico
  - Implementar periodic_sync_loop() — INDEX_EXCHANGE periódico
  - Implementar shutdown gracioso com signal handler ou try/except KeyboardInterrupt
"""

# import time
# import threading
# import signal
# from config import NODE_NAME

# def main():
#     ...

# if __name__ == "__main__":
#     main()
