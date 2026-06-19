"""
protocol.py — Definição do protocolo de comunicação SyncP2P.

Este módulo centraliza:
  1. Constantes de tipos de mensagens
  2. Funções para construir mensagens (build_message)
  3. Funções para serializar/desserializar com framing TCP (send_message, recv_message)

FRAMING TCP (Length-Prefix):
  ┌────────────────┬──────────────────────┐
  │ 4 bytes (BE)   │ N bytes              │
  │ tamanho payload│ JSON UTF-8 payload   │
  └────────────────┴──────────────────────┘

  - Usar struct.pack('>I', tamanho) para empacotar o header
  - Usar struct.unpack('>I', header) para desempacotar
  - recv_exact(sock, n) deve ler exatamente N bytes do socket

TODO (Grupo):
  - Definir todas as constantes MSG_*
  - Implementar build_message() com validação de campos
  - Implementar send_message(sock, msg_dict) com framing
  - Implementar recv_message(sock) com framing
  - Implementar recv_exact(sock, n) — loop até ler N bytes
"""

import json
import struct

# ─── Constantes de Tipos de Mensagens ───────────────────────────────
MSG_HELLO = "HELLO"
MSG_HELLO_ACK = "HELLO_ACK"
MSG_INDEX_EXCHANGE = "INDEX_EXCHANGE"
MSG_FILE_REQUEST = "FILE_REQUEST"
MSG_FILE_TRANSFER_START = "FILE_TRANSFER_START"
MSG_FILE_CHUNK = "FILE_CHUNK"
MSG_FILE_TRANSFER_COMPLETE = "FILE_TRANSFER_COMPLETE"
MSG_FILE_NOTIFY = "FILE_NOTIFY"
MSG_DELETE_NOTIFY = "DELETE_NOTIFY"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"
MSG_NODE_LEAVING = "NODE_LEAVING"

MAX_BYTES = 8192 #8kb
TIME_LIMIT = 10.0 #10 segundo de tempo maximo de espera para envio de pacotes

# TODO: Implementar as funções abaixo

def build_message(msg_type, **kwargs):
    """Constrói um dicionário de mensagem com o tipo e campos extras."""
    msg = {"type": msg_type}
    msg.update(kwargs)  # adiciona outros campos passados na função
    return msg


def send_message(sock, msg_dict):
    """Serializa msg_dict para JSON, adiciona header de 4 bytes com tamanho, envia pelo socket."""
    txt = json.dumps(msg_dict).encode("utf-8") # transforma o dict em bytes JSON para enviar
    tam = len(txt)                              # tamanho do payload em bytes
    header = struct.pack(">I", tam)             # empacota tamanho em 4 bytes big-endian
    sock.sendall(header + txt)                  # envia header + payload de uma vez


def recv_message(sock):
    """Lê header de 4 bytes, depois lê N bytes do payload, desserializa JSON."""
    header = recv_exact(sock, 4)

    if len(header) != 4:
        raise EOFError("Header incompleto recebido")

    tam = struct.unpack(">I", header)[0]  # desempacota tamanho do payload

    payload = recv_exact(sock, tam)       # lê exatamente tam bytes
    return json.loads(payload.decode("utf-8"))  # desserializa JSON e retorna dict


def recv_exact(sock, n):
    """Lê exatamente N bytes do socket (loop até completar)."""
    sock.settimeout(TIME_LIMIT)  # tempo máximo de espera por dados no buffer

    buffer = bytearray() #vetor temporarios e vazio para recebimento dos bytes
    
    while len(buffer) < n: #loop de leitura
        remaining = n - len(buffer) #
        data = sock.recv(remaining)
        if not data:
            raise EOFError("Conexão fechada inesperadamente")
        buffer.extend(data)

    return bytes(buffer)  # retorna os N bytes completos
