"""orquestração dos componentes do nó syncp2p."""

import os
import signal
import sys
import threading
import time
from pathlib import Path

# -----------------------------------------------------------------------------
# compatibilidade de imports
# -----------------------------------------------------------------------------

# alguns módulos antigos importam "config" como módulo de topo, enquanto os mais
# novos usam "src.config". este alias mantém ambos funcionando sem alterar o código
# dos outros integrantes.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config as config_module

sys.modules.setdefault("config", config_module)

# -----------------------------------------------------------------------------
# configuração e componentes compartilhados
# -----------------------------------------------------------------------------

from src.config import (
    HEARTBEAT_INTERVAL,
    NODE_ID,
    NODE_NAME,
    SHARED_FOLDER,
    SYNC_INTERVAL,
)
from src.network.discovery import DiscoveryManager
from src.network.protocol import (
    MSG_DELETE_NOTIFY,
    MSG_FILE_NOTIFY,
    MSG_HEARTBEAT,
    MSG_INDEX_EXCHANGE,
    MSG_NODE_LEAVING,
    build_message,
)
from src.network.tcp_client import TCPClient
from src.network.tcp_server import TCPServer
from src.sync.file_manager import FileManager
from src.sync.reconciler import Reconciler
from src.sync.state_db import StateDB
from src.sync.watcher import DirectoryWatcher
from src.ui.cli import log_error, log_info, log_warn


class SyncNode:
    """conecta os módulos existentes e controla o ciclo de vida do processo."""

    def __init__(
        self,
        state_db=None,
        file_manager=None,
        reconciler=None,
        discovery=None,
        watcher=None,
        server=None,
    ):
        # stop_event é a fonte única de verdade para saber se o nó está encerrando
        self.stop_event = threading.Event()
        # threads guarda somente loops persistentes que precisam ser aguardados no stop
        self.threads = []

        # os parâmetros opcionais permitem testar o main com componentes falsos sem rede ou disco
        self.state_db = state_db or StateDB()
        self.file_manager = file_manager or FileManager()
        self.reconciler = reconciler or Reconciler()

        # discovery chama on_new_node sempre que encontra um endereço ainda desconhecido
        self.discovery = discovery or DiscoveryManager(on_new_node=self.on_new_node)
        # watcher chama on_file_change depois de aplicar o debounce no evento do sistema
        self.watcher = watcher or DirectoryWatcher(on_change=self.on_file_change)

        # o servidor recebe as mesmas instâncias para compartilhar banco, arquivos e peers
        self.server = server or TCPServer(
            self.state_db,
            self.file_manager,
            self.reconciler,
            self.discovery,
        )

    def scan_initial(self):
        """atualiza o banco com o conteúdo atual da pasta compartilhada."""
        current = self.file_manager.scan_directory(SHARED_FOLDER)

        for filename, metadata in current.items():
            self.state_db.update_file_state(
                filename,
                metadata["hash"],
                metadata["timestamp"],
                metadata["size"],
            )

        # arquivo que estava ativo no banco mas sumiu do disco entre execuções vira tombstone,
        # garantindo que a deleção se propague para peers na próxima troca de índice
        for filename in self.state_db.get_active_files():
            if filename not in current:
                self.state_db.mark_deleted(filename, time.time())

    def start(self):
        # o banco precisa refletir o disco antes que conexões e eventos sejam aceitos
        self.scan_initial()

        # tcp_server.start() possui um loop while True bloqueante; a thread daemon
        # permite que os outros componentes continuem inicializando em paralelo.
        tcp_thread = threading.Thread(target=self.server.start, name="tcp-server-main", daemon=True)
        tcp_thread.start()
        self.threads.append(tcp_thread)

        self.watcher.start()
        self.discovery.start()

        # os dois loops usam stop_event.wait para acordar rapidamente durante o encerramento
        self._start_background_loop(self._heartbeat_loop, "heartbeat")
        self._start_background_loop(self._periodic_sync_loop, "periodic-sync")

        log_info(f"nó iniciado: {NODE_NAME}")
        log_info(f"pasta compartilhada: {SHARED_FOLDER}")

    def _start_background_loop(self, target, name):
        # loops persistentes são armazenados para que stop consiga aguardar sua finalização
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        self.threads.append(thread)

    @staticmethod
    def _start_one_off(target, name, args=()):
        """inicia uma tarefa curta sem mantê-la na lista de loops persistentes."""
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        thread.start()
        return thread

    def on_new_node(self, node_id, node_info, remote_ip):
        """agenda a troca de índice sem bloquear a thread udp do discovery."""
        # callbacks do discovery devem retornar rápido para não atrasar novos pacotes hello
        self._start_one_off(
            target=self._exchange_index,
            args=(node_id, node_info, remote_ip),
            name=f"index-{node_id[:8]}",
        )

    def _exchange_index(self, node_id, node_info, remote_ip):
        # envia o índice local completo para que o peer calcule o que precisa baixar ou apagar
        message = build_message(
            MSG_INDEX_EXCHANGE,
            node_id=NODE_ID,
            files=self.state_db.get_full_index(),
        )
        try:
            TCPClient.send_message(remote_ip, node_info["tcp_port"], message)
        except Exception as exc:
            log_warn(f"falha ao trocar índice com {node_id}: {exc}")

    def on_file_change(self, filename, action):
        """atualiza o estado local e notifica todos os peers sobre a mudança."""
        filepath = os.path.join(SHARED_FOLDER, filename)
        timestamp = time.time()

        if action == "DELETED" or not os.path.isfile(filepath):
            # tombstone garante que peers recebam a deleção mesmo estando offline no momento
            self.state_db.mark_deleted(filename, timestamp)
            message = build_message(
                MSG_DELETE_NOTIFY,
                node_id=NODE_ID,
                filename=filename,
                timestamp=timestamp,
            )
        else:
            # hash e tamanho são calculados após o debounce, quando o arquivo já está estável
            file_hash = self.file_manager.get_file_hash(filepath)
            size = self.file_manager.get_file_size(filepath)
            timestamp = os.path.getmtime(filepath)
            self.state_db.update_file_state(filename, file_hash, timestamp, size)
            message = build_message(
                MSG_FILE_NOTIFY,
                node_id=NODE_ID,
                action=action,
                filename=filename,
                hash=file_hash,
                timestamp=timestamp,
                size=size,
            )

        # o envio roda fora da thread de debounce para que novos eventos do
        # watchdog não esperem os retries de rede.
        self._start_one_off(
            target=self._send_to_all,
            args=(message,),
            name=f"notify-{filename}",
        )

    def _send_to_all(self, message):
        # a cópia retornada pelo discovery evita iterar sobre o dicionário enquanto ele muda
        for node in self.discovery.get_active_nodes().values():
            try:
                TCPClient.send_message(node["ip"], node["tcp_port"], message)
            except Exception as exc:
                log_warn(f"falha ao notificar {node.get('name')}: {exc}")

    def _heartbeat_loop(self):
        # heartbeat verifica a disponibilidade tcp; expiração definitiva continua no discovery
        while not self.stop_event.wait(HEARTBEAT_INTERVAL):
            message = build_message(MSG_HEARTBEAT, node_id=NODE_ID, timestamp=time.time())
            for node_id, node in self.discovery.get_active_nodes().items():
                response = TCPClient.send_and_receive(node["ip"], node["tcp_port"], message)
                if response is None:
                    log_warn(f"heartbeat sem resposta: {node_id}")

    def _periodic_sync_loop(self):
        # a troca periódica recupera alterações que possam ter sido perdidas durante uma falha
        while not self.stop_event.wait(SYNC_INTERVAL):
            for node_id, node in self.discovery.get_active_nodes().items():
                self._exchange_index(node_id, node, node["ip"])

    def stop(self):
        # chamadas repetidas podem vir de ctrl+c, sigterm ou erro de inicialização
        if self.stop_event.is_set():
            return
        self.stop_event.set()

        # node_leaving permite que peers removam este nó sem aguardar o timeout do discovery
        leaving = build_message(MSG_NODE_LEAVING, node_id=NODE_ID)
        self._send_to_all(leaving)

        # produtores de eventos são parados antes do banco para impedir novos callbacks tardios
        self.watcher.stop()
        self.discovery.stop()

        # heartbeat e sincronização periódica acordam pelo stop_event e podem ser aguardados;
        # a thread tcp permanece daemon porque o servidor atual não oferece stop público.
        for thread in self.threads:
            if thread.name != "tcp-server-main":
                thread.join(timeout=2)

        # StateDB não expõe close(); acessamos conn diretamente para fechar o SQLite
        # de forma segura sem alterar o módulo implementado por outro integrante.
        connection = getattr(self.state_db, "conn", None)
        lock = getattr(self.state_db, "lock", None)
        if connection is not None:
            try:
                if lock is not None:
                    with lock:
                        connection.close()
                else:
                    connection.close()
            except Exception as exc:
                log_warn(f"falha ao fechar banco: {exc}")

        log_info("nó encerrado")


def main():
    # a função mantém a criação e o encerramento do nó em um único ponto de entrada
    node = SyncNode()
    try:
        node.start()
    except Exception as exc:
        log_error(f"falha ao iniciar o nó: {exc}")
        node.stop()
        return 1

    def shutdown(_signum=None, _frame=None):
        # sigint atende ctrl+c e sigterm atende o encerramento enviado pelo docker
        node.stop()

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    try:
        # a espera com timeout mantém o processo vivo sem consumir cpu em um loop ocupado
        while not node.stop_event.wait(0.5):
            pass
    except KeyboardInterrupt:
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
