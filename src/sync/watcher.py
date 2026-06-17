"""
watcher.py — Monitoramento de alterações no sistema de arquivos com Watchdog.

Responsabilidades:
  1. Monitorar a shared_folder usando a biblioteca watchdog
  2. Detectar criação, modificação, deleção e movimentação de arquivos
  3. Notificar o sistema (via callback) quando algo muda
  4. Implementar debouncing para evitar eventos duplicados
  5. Ignorar eventos causados pela própria sincronização (flag files_being_synced)

DEBOUNCING (MUITO IMPORTANTE):
  O watchdog pode disparar MÚLTIPLOS eventos para uma única alteração.
  Ex: salvar um arquivo pode gerar 2-3 eventos on_modified seguidos.
  Solução: manter um dicionário pending_events com timer de 0.5s.
  Ao receber evento, agendar processamento. Se outro evento chegar para
  o mesmo arquivo dentro do delay, reiniciar o timer.

ANTI-LOOP (MUITO IMPORTANTE):
  Quando o nó RECEBE um arquivo de outro nó e o escreve na shared_folder,
  o watchdog detecta isso como alteração LOCAL e tenta propagar de volta.
  Isso cria um loop infinito!
  Solução: manter um set() chamado files_being_synced.
  Antes de escrever arquivo recebido, adicionar ao set.
  No handler do watchdog, ignorar arquivos que estão no set.
  Após escrever, aguardar 1s e remover do set.

TODO (Grupo):
  - Criar classe SyncEventHandler(FileSystemEventHandler)
  - Sobrescrever on_created, on_modified, on_deleted, on_moved
  - Implementar debouncing com pending_events + threading.Timer
  - Implementar files_being_synced como set() protegido com Lock
  - Criar classe DirectoryWatcher que inicializa Observer do watchdog
  - Implementar start() e stop()
"""

# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler

# class SyncEventHandler(FileSystemEventHandler):
#     def __init__(self, on_change, files_being_synced):
#         ...

# class DirectoryWatcher:
#     def __init__(self, state_db, file_manager, on_change=None):
#         ...
