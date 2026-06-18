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

# TODO: Implementar as funções abaixo

def build_message(msg_type, **kwargs):
#     """Constrói um dicionário de mensagem com o tipo e campos extras.""
#     ...
  msg = {"type": msg_type}
  msg.update(kwargs)
  return msg

# def send_message(sock, msg_dict):
#     """Serializa msg_dict para JSON, adiciona header de 4 bytes com tamanho, envia pelo socket."""
#     ...

# def recv_message(sock):
#     """Lê header de 4 bytes, depois lê N bytes do payload, desserializa JSON."""
#     ...

# def recv_exact(sock, n):
#     """Lê exatamente N bytes do socket (loop até completar)."""
#     ...




