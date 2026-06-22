"""servidor tcp concorrente do syncp2p."""

import base64
import os
import socket
import threading
from pathlib import Path

from .protocol import (
    MSG_DELETE_NOTIFY,
    MSG_ERROR,
    MSG_FILE_CHUNK,
    MSG_FILE_NOTIFY,
    MSG_FILE_REQUEST,
    MSG_FILE_TRANSFER_COMPLETE,
    MSG_FILE_TRANSFER_START,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_INDEX_EXCHANGE,
    MSG_NODE_LEAVING,
    ProtocolError,
    build_message,
    recv_message,
    send_message,
)
from ..config import CHUNK_SIZE, NODE_ID, SHARED_FOLDER, SOCKET_TIMEOUT, TCP_PORT
from ..sync.file_manager import FileManager
from ..sync.reconciler import Reconciler
from ..ui.cli import log_error, log_info, log_warn


class TCPServer:
    def __init__(
        self,
        state_db,
        file_manager=FileManager,
        reconciler=Reconciler,
        discovery=None,
        on_index=None,
        on_notify=None,
        host="0.0.0.0",
        port=TCP_PORT,
        shared_folder=SHARED_FOLDER,
        chunk_size=CHUNK_SIZE,
        socket_timeout=SOCKET_TIMEOUT,
    ):
        # dependências são recebidas de fora para manter rede e sincronização desacopladas
        self.state_db = state_db
        self.file_manager = file_manager
        self.reconciler = reconciler
        self.discovery = discovery
        self.on_index = on_index
        self.on_notify = on_notify
        self.host = host
        self.port = int(port)
        self.shared_folder = Path(shared_folder).resolve()
        self.chunk_size = int(chunk_size)
        self.socket_timeout = float(socket_timeout)

        # estas estruturas permitem controlar listener, clientes e workers no encerramento
        self.stop_event = threading.Event()
        self.sock = None
        self.accept_thread = None
        self.clients = set()
        self.workers = set()
        self.lock = threading.Lock()

    @property
    def bound_port(self):
        return self.sock.getsockname()[1] if self.sock else self.port

    def start(self):
        # o accept roda em background para que o main possa iniciar os próximos componentes
        if self.accept_thread and self.accept_thread.is_alive():
            return self.bound_port
        self.shared_folder.mkdir(parents=True, exist_ok=True)
        self.stop_event.clear()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((self.host, self.port))
            server.listen(20)
            server.settimeout(0.5)
        except Exception:
            server.close()
            raise
        self.sock = server
        self.accept_thread = threading.Thread(target=self._accept_loop, name="tcp-server", daemon=True)
        self.accept_thread.start()
        log_info(f"servidor tcp escutando na porta {self.bound_port}")
        return self.bound_port

    def _accept_loop(self):
        server = self.sock
        while not self.stop_event.is_set():
            try:
                client, address = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            # cada conexão recebe um worker para que clientes sejam atendidos em paralelo
            client.settimeout(self.socket_timeout)
            worker = threading.Thread(
                target=self._client_worker,
                args=(client, address),
                name=f"tcp-client-{address[0]}:{address[1]}",
                daemon=True,
            )
            with self.lock:
                self.clients.add(client)
                self.workers.add(worker)
            worker.start()

    def _client_worker(self, sock, address):
        # o finally remove referências mesmo quando cliente ou protocolo falham
        try:
            self._handle_client(sock, address)
        finally:
            with self.lock:
                self.clients.discard(sock)
                self.workers.discard(threading.current_thread())
            sock.close()

    def _handle_client(self, sock, address):
        # a conexão permanece aberta porque uma transferência usa várias mensagens
        while not self.stop_event.is_set():
            try:
                message = recv_message(sock)
            except (EOFError, socket.timeout, OSError):
                return
            except ProtocolError as exc:
                # entrada inválida encerra somente este cliente, não o listener principal
                self._send_error(sock, "INVALID_MESSAGE", str(exc))
                return
            try:
                if not self._route_message(message, sock, address[0]):
                    return
            except (KeyError, TypeError, ValueError, ProtocolError) as exc:
                self._send_error(sock, "INVALID_FIELDS", str(exc))
                return
            except Exception as exc:
                log_error(f"erro ao atender {address[0]}: {exc}")
                self._send_error(sock, "INTERNAL_ERROR", "falha interna")
                return

    def _route_message(self, message, sock, remote_ip):
        # o roteador valida o tipo e encaminha para o handler correspondente
        msg_type = message["type"]
        if msg_type == MSG_FILE_REQUEST:
            self._send_file(sock, self._required_string(message, "filename"))
            return True
        if msg_type == MSG_INDEX_EXCHANGE:
            self._handle_index_exchange(message, sock, remote_ip)
            return True
        if msg_type == MSG_FILE_NOTIFY:
            self._required_string(message, "filename")
            if self.on_notify:
                self.on_notify(message, remote_ip)
            return True
        if msg_type == MSG_DELETE_NOTIFY:
            self._handle_delete_notify(message, remote_ip)
            return True
        if msg_type == MSG_HEARTBEAT:
            send_message(sock, build_message(MSG_HEARTBEAT_ACK, node_id=NODE_ID))
            return True
        if msg_type == MSG_NODE_LEAVING:
            node_id = self._required_string(message, "node_id")
            if self.discovery:
                self.discovery.remove_node(node_id)
            return True
        self._send_error(sock, "UNSUPPORTED_MESSAGE", f"mensagem não aceita: {msg_type}")
        return False

    def _handle_index_exchange(self, message, sock, remote_ip):
        # o reconciliador compara índices e devolve as ações necessárias no nó local
        remote_id = self._required_string(message, "node_id")
        remote_index = message.get("files")
        if not isinstance(remote_index, dict):
            raise ValueError("index_exchange exige o campo files")
        local_index = self.state_db.get_full_index()
        downloads, uploads, deletions = self.reconciler.compare_indices(
            local_index, remote_index, NODE_ID, remote_id
        )
        if self.on_index:
            self.on_index(message, remote_ip, (downloads, uploads, deletions))
        send_message(
            sock,
            build_message(
                MSG_INDEX_EXCHANGE,
                node_id=NODE_ID,
                tcp_port=self.bound_port,
                files=local_index,
                actions={"download": downloads, "upload": uploads, "delete": deletions},
                reply=True,
            ),
        )

    def _handle_delete_notify(self, message, remote_ip):
        # uma exclusão mais recente remove o arquivo e mantém um tombstone no banco
        filename = self._required_string(message, "filename")
        timestamp = message.get("timestamp")
        if not isinstance(timestamp, (int, float)):
            raise ValueError("delete_notify exige timestamp")
        local_state = self.state_db.get_file_state(filename)
        if local_state is None or timestamp >= local_state["timestamp"]:
            path = self._resolve_shared_file(filename)
            if path and path.is_file():
                self.file_manager.delete_file(path)
            self.state_db.mark_deleted(filename, timestamp)
        if self.on_notify:
            self.on_notify(message, remote_ip)

    def _send_file(self, sock, filename):
        path = self._resolve_shared_file(filename)
        if path is None or not path.is_file():
            self._send_error(sock, "FILE_NOT_FOUND", "arquivo não encontrado")
            return
        size = self.file_manager.get_file_size(path)
        file_hash = self.file_manager.get_file_hash(path)
        # arquivo vazio → 1 chunk vazio; arquivo não-vazio → ceil(size / chunk_size) chunks
        total_chunks = max(1, (size + self.chunk_size - 1) // self.chunk_size)
        send_message(
            sock,
            build_message(
                MSG_FILE_TRANSFER_START,
                filename=filename,
                size=size,
                hash=file_hash,
                total_chunks=total_chunks,
            ),
        )
        if size == 0:
            send_message(
                sock,
                build_message(MSG_FILE_CHUNK, filename=filename, chunk_index=0, data="", is_last=True),
            )
        else:
            for index, chunk in enumerate(self.file_manager.read_file_chunks(path, self.chunk_size)):
                send_message(
                    sock,
                    build_message(
                        MSG_FILE_CHUNK,
                        filename=filename,
                        chunk_index=index,
                        data=base64.b64encode(chunk).decode("ascii"),
                        is_last=index == total_chunks - 1,
                    ),
                )
        confirmation = recv_message(sock)
        if confirmation.get("type") != MSG_FILE_TRANSFER_COMPLETE:
            raise ProtocolError("confirmação de transferência inválida")
        if confirmation.get("hash") != file_hash:
            raise ProtocolError("cliente confirmou outro hash")

    def _resolve_shared_file(self, filename):
        # basename e relative_to impedem caminhos que escapem da pasta compartilhada
        if filename != os.path.basename(filename) or filename in {".", ".."}:
            return None
        path = (self.shared_folder / filename).resolve()
        try:
            path.relative_to(self.shared_folder)
        except ValueError:
            return None
        return path

    @staticmethod
    def _required_string(message, field):
        value = message.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"campo obrigatório inválido: {field}")
        return value

    @staticmethod
    def _send_error(sock, code, detail):
        try:
            send_message(sock, build_message(MSG_ERROR, code=code, error=detail))
        except OSError:
            pass

    def stop(self):
        # fechar listener e clientes acorda threads bloqueadas em accept ou recv
        self.stop_event.set()
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
        with self.lock:
            clients = list(self.clients)
            workers = list(self.workers)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.close()
        if self.accept_thread:
            self.accept_thread.join(timeout=2)
        for worker in workers:
            worker.join(timeout=2)
