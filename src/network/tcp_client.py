"""cliente tcp do syncp2p."""

import base64
import binascii
import hashlib
import os
import socket
import tempfile
import time
from pathlib import Path

from .protocol import (
    MSG_ERROR,
    MSG_FILE_CHUNK,
    MSG_FILE_REQUEST,
    MSG_FILE_TRANSFER_COMPLETE,
    MSG_FILE_TRANSFER_START,
    MSG_INDEX_EXCHANGE,
    ProtocolError,
    build_message,
    recv_message,
    send_message as protocol_send,
)
from ..config import NODE_ID, SOCKET_TIMEOUT, TCP_PORT
from ..ui.cli import log_error, log_info, log_sync, log_warn


class TCPClient:
    @staticmethod
    def connect(ip, port, retries=3, timeout=SOCKET_TIMEOUT, initial_backoff=0.5, quiet=False):
        # cada falha fecha o socket e aumenta o intervalo antes da próxima tentativa
        if retries < 1:
            raise ValueError("retries precisa ser pelo menos 1")
        delay = initial_backoff
        last_error = None
        for attempt in range(1, retries + 1):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            try:
                sock.connect((ip, int(port)))
                return sock
            except OSError as exc:
                last_error = exc
                sock.close()
                if attempt < retries:
                    if not quiet:
                        log_warn(f"conexão {attempt}/{retries} falhou; nova tentativa em {delay:.1f}s")
                    time.sleep(delay)
                    delay *= 2
        raise ConnectionError(f"servidor {ip}:{port} inacessível") from last_error

    @staticmethod
    def send_message(ip, port, message, quiet=False):
        # usado para operações que não precisam esperar uma resposta
        try:
            with TCPClient.connect(ip, port, quiet=quiet) as sock:
                protocol_send(sock, message)
            if not quiet:
                log_info(f"mensagem {message['type']} enviada para {ip}:{port}")
            return True
        except Exception as exc:
            if not quiet:
                log_error(f"falha ao enviar para {ip}:{port}: {exc}")
            return False

    @staticmethod
    def send_and_receive(ip, port, message, quiet=False):
        # usado por heartbeat e troca de índices, que possuem resposta imediata
        try:
            with TCPClient.connect(ip, port, quiet=quiet) as sock:
                protocol_send(sock, message)
                return recv_message(sock)
        except Exception as exc:
            if not quiet:
                log_error(f"falha na comunicação com {ip}:{port}: {exc}")
            return None

    @staticmethod
    def send_index(ip, port, local_index, node_id=NODE_ID, tcp_port=TCP_PORT):
        # identidade e porta permitem que o receptor reconheça o remetente
        return TCPClient.send_and_receive(
            ip,
            port,
            build_message(
                MSG_INDEX_EXCHANGE,
                node_id=node_id,
                tcp_port=tcp_port,
                files=local_index,
            ),
            quiet=True,
        )

    @staticmethod
    def request_file(ip, port, filename, save_path):
        # o arquivo definitivo só é substituído depois das validações de tamanho e hash
        destination = Path(save_path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = None
        try:
            with TCPClient.connect(ip, port) as sock:
                protocol_send(sock, build_message(MSG_FILE_REQUEST, filename=filename))
                start = recv_message(sock)
                if start.get("type") == MSG_ERROR:
                    raise FileNotFoundError(start.get("error", filename))
                if start.get("type") != MSG_FILE_TRANSFER_START:
                    raise ProtocolError("resposta inicial de arquivo inválida")
                if start.get("filename") != filename:
                    raise ProtocolError("servidor respondeu com outro arquivo")

                expected_size = TCPClient._integer(start, "size", minimum=0)
                expected_chunks = TCPClient._integer(start, "total_chunks", minimum=1)
                expected_hash = start.get("hash")
                if not isinstance(expected_hash, str) or len(expected_hash) != 64:
                    raise ProtocolError("hash esperado inválido")

                # hash, tamanho e sequência são verificados durante a escrita incremental
                digest = hashlib.sha256()
                received_size = 0
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    prefix=f".{destination.name}.",
                    suffix=".part",
                    dir=destination.parent,
                    delete=False,
                ) as temporary:
                    temp_path = Path(temporary.name)
                    for expected_index in range(expected_chunks):
                        chunk_message = recv_message(sock)
                        if chunk_message.get("type") != MSG_FILE_CHUNK:
                            raise ProtocolError("esperado file_chunk")
                        # índices crescentes detectam chunks perdidos, repetidos ou fora de ordem
                        if chunk_message.get("chunk_index") != expected_index:
                            raise ProtocolError("chunk fora de ordem")
                        data = chunk_message.get("data")
                        if not isinstance(data, str):
                            raise ProtocolError("dados do chunk inválidos")
                        try:
                            raw = base64.b64decode(data, validate=True)
                        except (ValueError, binascii.Error) as exc:
                            raise ProtocolError("base64 inválido") from exc
                        is_last = chunk_message.get("is_last") is True
                        if is_last != (expected_index == expected_chunks - 1):
                            raise ProtocolError("marcador de último chunk inconsistente")
                        temporary.write(raw)
                        digest.update(raw)
                        received_size += len(raw)

                actual_hash = digest.hexdigest()
                if received_size != expected_size:
                    raise ProtocolError(f"tamanho recebido {received_size}, esperado {expected_size}")
                if actual_hash != expected_hash:
                    raise ProtocolError("hash recebido não confere")

                # os.replace é atômico porque temporário e destino ficam no mesmo diretório
                os.replace(temp_path, destination)
                temp_path = None
                protocol_send(
                    sock,
                    build_message(MSG_FILE_TRANSFER_COMPLETE, filename=filename, hash=actual_hash),
                )
            log_sync(f"download concluído: {filename}")
            return True
        except Exception as exc:
            log_error(f"falha no download de {filename}: {exc}")
            return False
        finally:
            # uma falha remove somente o temporário e preserva o arquivo anterior
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    @staticmethod
    def _integer(message, field, minimum):
        value = message.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise ProtocolError(f"campo inteiro inválido: {field}")
        return value
