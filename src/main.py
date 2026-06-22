"""ponto de entrada e ciclo de vida do nó syncp2p."""

import signal
import threading
import time

from .config import NODE_NAME, SHARED_FOLDER
from .network.tcp_server import TCPServer
from .sync.file_manager import FileManager
from .sync.reconciler import Reconciler
from .sync.state_db import StateDB
from .ui.cli import log_error, log_info, log_warn


class SyncNode:
    """núcleo executável das etapas 1 e 2.

    descoberta e watchdog serão conectados nas próximas etapas sem alterar a api tcp.
    """

    def __init__(self):
        # o evento permite que o loop principal e os sinais compartilhem o mesmo estado de parada
        self.stop_event = threading.Event()
        # banco, arquivos e reconciliador formam a camada de estado usada pelo servidor tcp
        self.state_db = StateDB()
        self.file_manager = FileManager
        self.reconciler = Reconciler
        # callbacks deixam o servidor independente da futura descoberta e sincronização automática
        self.server = TCPServer(
            self.state_db,
            self.file_manager,
            self.reconciler,
            on_index=self.on_index,
            on_notify=self.on_notify,
        )

    def scan_initial(self):
        # arquivos presentes atualizam o índice e arquivos ausentes recebem um tombstone
        current = self.file_manager().scan_directory()
        for filename, metadata in current.items():
            self.state_db.update_file_state(
                filename,
                metadata["hash"],
                metadata["timestamp"],
                metadata["size"],
            )
        for filename in self.state_db.get_active_files():
            if filename not in current:
                self.state_db.mark_deleted(filename, time.time())

    def start(self):
        # o índice é montado antes de aceitar trocas com outros nós
        self.scan_initial()
        port = self.server.start()
        log_info(f"{NODE_NAME} iniciado; pasta compartilhada: {SHARED_FOLDER}")
        log_info(f"camada tcp pronta na porta {port}")
        log_warn("descoberta e watchdog serão conectados nas etapas 3 e 4")

    def on_index(self, message, remote_ip, actions):
        # nesta etapa as ações são registradas; a execução entra com descoberta e watcher
        downloads, uploads, deletions = actions
        log_info(
            f"índice recebido de {remote_ip}: "
            f"{len(downloads)} downloads, {len(uploads)} uploads, {len(deletions)} exclusões"
        )

    def on_notify(self, message, remote_ip):
        # o callback já está preparado para receber notificações das próximas etapas
        log_info(f"notificação {message['type']} recebida de {remote_ip}")

    def stop(self):
        if self.stop_event.is_set():
            return
        # o listener e os workers são encerrados antes da conexão sqlite
        self.stop_event.set()
        self.server.stop()
        self.state_db.close()
        log_info("nó encerrado")


def main():
    # esta é a única entrada do programa para execução local e pelo docker
    node = SyncNode()
    try:
        node.start()
    except Exception as exc:
        log_error(f"não foi possível iniciar o nó: {exc}")
        node.stop()
        return 1

    def shutdown(_signum=None, _frame=None):
        # ctrl+c e sinais do container usam o mesmo encerramento gracioso
        node.stop()

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)
    try:
        while not node.stop_event.wait(0.5):
            pass
    except KeyboardInterrupt:
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
