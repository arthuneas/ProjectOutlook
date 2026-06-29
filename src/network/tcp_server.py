"""
tcp_server.py — Servidor TCP para receber conexões de outros nós.

Responsabilidades:
  1. Escutar na porta TCP por conexões de outros nós
  2. Aceitar conexões e criar thread separada para cada cliente
  3. Receber mensagens usando framing do protocol.py (recv_message)
  4. Rotear cada mensagem para o handler correto baseado no 'type'
  5. Responder adequadamente (ex: enviar arquivo quando receber FILE_REQUEST)

"""

import socket
import threading
from config import TCP_PORT, NODE_ID, SHARED_FOLDER
import time

from ..ui.cli import log_info, log_error, log_sync, log_warn

from .protocol import (
    build_message,
    send_message,
    recv_message,
    MSG_INDEX_EXCHANGE,
    MSG_FILE_REQUEST,
    MSG_FILE_TRANSFER_START,
    MSG_FILE_CHUNK,
    MSG_FILE_TRANSFER_COMPLETE,
    MSG_FILE_NOTIFY,
    MSG_DELETE_NOTIFY,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_NODE_LEAVING,
)

from ..sync.file_manager import FileManager
from ..sync.reconciler import Reconciler
from ..sync.state_db import StateDB
import os


class TCPServer:
    # dessa vez, diferente do tcp_client, é necessário um construtor pois o servidor é stateful! É necessário guardar informações
    def __init__(
        self, state_db, file_manager, reconciler, discovery, on_index_reconciled=None
    ):  # construtor
        self.state_db = state_db
        self.file_manager = file_manager
        self.reconciler = reconciler
        self.discovery = discovery
        self.porta = TCP_PORT
        self.on_index_reconciled = on_index_reconciled  # guarda a referência da main
        self.watcher = None

    def start(self):
        try:
            # criando o socketTCP do servidor
            server_socketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socketTCP.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # ^ essa linha serve para evitar o erro "Address already in use", principalmente quando o socket é fechado. Demora um pouco
            # até que o TCP libere a porta pois o servidor está em TIME WAIT esperando uma possível nova requisição do cliente, por segu-
            # rança, por causa do protocolo FIN. Essa config da linha ignora o TIME_WAIT, permitindo que o cliente conecte de novo imediatamente

            server_socketTCP.bind(
                ("0.0.0.0", self.porta)
            )  # escutando de 0.0.0.0 = escutar de qualquer fonte
            server_socketTCP.listen(
                5
            )  # fila de espera para receber o accept vai só até 5 "lugares"

            # feedback cli
            log_info("Servidor TCP ligado e aguardando conexões...")

            while True:
                sock_cliente, endereco_cliente = server_socketTCP.accept()
                thread_nova = threading.Thread(
                    target=self._handle_client,
                    args=(
                        sock_cliente,
                        endereco_cliente,
                    ),
                    daemon=True,
                )
                thread_nova.start()

        except Exception as e:
            log_error(f"Falha ao iniciar o servidor TCP na porta {self.porta}: {e}")
            # aceitando clientes

    # ─────────────────────────────── Implementando o _handle_client ───────────────────────────────
    def _handle_client(self, sock_cliente, endereco_cliente):
        sock_cliente.settimeout(15)

        try:
            # loop pra virar keepalive ("NÃO fechar conexão após 1 mensagem se houver conversa multi-mensagem")
            while True:
                try:
                    # lendo msg
                    msg = recv_message(sock_cliente)

                except EOFError:
                    log_info(
                        f"Conexão finalizada corretamente pelo nó remoto ({endereco_cliente[0]})."
                    )
                    break  # cliente encerrou conexão

                if msg["type"] == MSG_INDEX_EXCHANGE:
                    self._handle_index_exchange(msg, sock_cliente)

                elif msg["type"] == MSG_FILE_REQUEST:
                    self._handle_file_request(msg, sock_cliente)

                elif msg["type"] == MSG_FILE_NOTIFY:
                    self._handle_file_notify(msg, sock_cliente)

                elif msg["type"] == MSG_DELETE_NOTIFY:
                    self._handle_delete_notify(msg, sock_cliente)

                elif (
                    msg["type"] == MSG_HEARTBEAT
                ):  # cliente perguntando se tô online. Se eu recebi, estou, ent envio resposta imediata
                    msg_ack = build_message(MSG_HEARTBEAT_ACK)
                    send_message(sock_cliente, msg_ack)  # mando pro cliente o ack

        except socket.timeout:
            log_warn(
                f"Timeout de 15s atingido para o cliente ({endereco_cliente[0]}). Fechando conexão..."
            )

        except Exception as e:
            log_warn(
                "Conexão interrompida com o cliente: "
                + endereco_cliente[0]
                + f". Aviso: {e}"
            )

        finally:  # sempre é executado
            sock_cliente.close()  # libera recurso do SO

    # ─────────────────────────────── Implementando o _handle_index_exchange ───────────────────────────────
    def _handle_index_exchange(self, msg, sock_cliente):
        try:
            indice_remoto = msg["files"]
            node_id_remoto = msg["node_id"]

            # pegando informacoes do bd
            indice_local = self.state_db.get_full_index()

            # vamos comparar o que o reconciliador Last Write Wins faz com os index (ou seja, quando alguém entrar na rede, eu vou comparar
            # a minha pasta com a dele. A depender do que for >>mais recente<< :
            # 1. se nem existir no meu ou for uma versão desatualizada, eu preciso baixar
            # 2. se existir no dele, mas for uma versão antiga, ele precisa atualizar (preciso enviar a versão atualizada)
            # 3. se tiver sido apagado e tiver timestamp mais recente, precisa apagar)
            download_list, upload_list, delete_list = self.reconciler.compare_indices(
                indice_local, indice_remoto, NODE_ID, node_id_remoto
            )

            if self.on_index_reconciled:
                ip_remoto = sock_cliente.getpeername()[0]
                self.on_index_reconciled(
                    node_id_remoto, ip_remoto, download_list, upload_list, delete_list
                )

        except Exception as e:
            log_error(f"Erro ao processar reconciliação dos índices: {e}")

        # TODO: gerenciar essas listas (download_list, upload_list, delete_list) no main

    # ─────────────────────────────── Implementando o _handle_file_request ───────────────────────────────
    def _handle_file_request(self, msg, sock_cliente):
        nome_arquivo = msg["filename"]
        caminho_completo = os.path.join(SHARED_FOLDER, nome_arquivo)

        if not os.path.exists(caminho_completo):
            log_warn(
                f"Nó remoto solicitou arquivo que não existe localmente: {nome_arquivo}"
            )
            return

        try:
            tamanho = self.file_manager.get_file_size(caminho_completo)
            hash_arquivo = self.file_manager.get_file_hash(caminho_completo)

            msg_start = build_message(
                MSG_FILE_TRANSFER_START,
                filename=nome_arquivo,
                size=tamanho,
                hash=hash_arquivo,
            )
            send_message(sock_cliente, msg_start)

            import base64  # Certifique-se de importar o base64 se já não estiver no topo

            # Agora vamos enviar em chunks o arquivo através do yield do FileManager
            for bloco_binario in self.file_manager.read_file_chunks(caminho_completo):
                # ─── CORREÇÃO AQUI: Converte os bytes crus para uma string de texto Base64 segura para JSON ───
                bloco_base64 = base64.b64encode(bloco_binario).decode("utf-8")

                msg_chunk = build_message(
                    MSG_FILE_CHUNK,
                    filename=nome_arquivo,
                    data=bloco_base64,
                    is_last=False,
                )
                send_message(sock_cliente, msg_chunk)

            # Informando o chunk final
            msg_final = build_message(
                MSG_FILE_CHUNK, filename=nome_arquivo, data="", is_last=True
            )
            send_message(sock_cliente, msg_final)
            log_info(f"Arquivo {nome_arquivo} enviado com sucesso.")

        except Exception as e:
            log_error(
                f"Falha crítica ao transmitir chunks do arquivo {nome_arquivo}: {e}"
            )

    # ─────────────────────────────── Implementando o _handle_file_notify ───────────────────────────────
    def _handle_file_notify(self, msg, sock_cliente):
        nome_arquivo = msg["filename"]
        timestamp_remoto = float(msg["timestamp"])
        node_id_remoto = msg.get("node_id")

        estado_local = self.state_db.get_file_state(nome_arquivo)

        # Se o arquivo for novo ou tiver timestamp mais recente, vamos baixar
        if estado_local is None or timestamp_remoto > float(estado_local["timestamp"]):
            log_sync(
                f"Notificação: Mudança detectada no remoto para {nome_arquivo}. Agendando download..."
            )

            ip_remoto = sock_cliente.getpeername()[0]
            porta_remota = self.porta
            with self.discovery.lock:
                if node_id_remoto in self.discovery.known_nodes:
                    porta_remota = self.discovery.known_nodes[node_id_remoto][
                        "tcp_port"
                    ]

            caminho_salvar = os.path.join(SHARED_FOLDER, nome_arquivo)

            def rodar_download_background():
                from .tcp_client import TCPClient

                time.sleep(0.1)
                try:
                    # ─── ATIVA A BLINDAGEM CONTRA LOOP (Evita o efeito eco) ───
                    if hasattr(self, "watcher") and self.watcher:
                        self.watcher.mark_syncing(nome_arquivo)

                    log_sync(
                        f"Iniciando download seguro via P2P para o arquivo {nome_arquivo}"
                    )
                    TCPClient.request_file(
                        ip_remoto, porta_remota, nome_arquivo, caminho_salvar
                    )

                    hash_local = FileManager.get_file_hash(caminho_salvar)
                    tamanho_local = FileManager.get_file_size(caminho_salvar)

                    # Salva no SQLite
                    self.state_db.update_file_state(
                        nome_arquivo, hash_local, timestamp_remoto, tamanho_local
                    )
                    log_sync(f"Arquivo {nome_arquivo} integrado e gravado com sucesso!")
                except Exception as ex:
                    log_error(
                        f"Erro ao processar download em background para {nome_arquivo}: {ex}"
                    )
                finally:
                    # ─── LIBERA A BLINDAGEM DEPOIS QUE O ARQUIVO ESTABILIZOU NO DISCO ───
                    if hasattr(self, "watcher") and self.watcher:
                        self.watcher.release_syncing(nome_arquivo, delay=1.5)

            threading.Thread(target=rodar_download_background, daemon=True).start()

    # ─────────────────────────────── Implementando o _handle_delete_notify ───────────────────────────────
    def _handle_delete_notify(self, msg, sock_cliente):
        nome_arquivo = msg["filename"]
        timestamp_delecao = msg["timestamp"]

        estado_local = self.state_db.get_file_state(nome_arquivo)

        # se o arquivo existir/ativo e a delecao não é mais antiga que o arquivo
        if estado_local and estado_local["status"] == "ACTIVE":
            if timestamp_delecao > estado_local["timestamp"]:
                # marcando como deletado
                self.state_db.mark_deleted(nome_arquivo, timestamp_delecao)

                # deletando o arquivo em si
                caminho_completo = os.path.join(SHARED_FOLDER, nome_arquivo)
                self.file_manager.delete_file(caminho_completo)

                log_sync(
                    "Arquivo excluído com sucesso para sincronizar as pastas remotas"
                )
