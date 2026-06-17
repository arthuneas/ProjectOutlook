"""
tcp_client.py — Cliente TCP para enviar mensagens ativamente para outros nós.

Responsabilidades:
  1. Conectar a um nó remoto via TCP
  2. Enviar mensagens usando framing do protocol.py (send_message)
  3. Opcionalmente aguardar resposta (send_and_receive)
  4. Solicitar e receber arquivos (request_file)

TODO (Grupo):
  - Implementar send_message(ip, port, msg_dict) — conecta, envia com framing, fecha
  - Implementar send_and_receive(ip, port, msg_dict) — conecta, envia, espera resposta
  - Implementar request_file(ip, port, filename, save_path) — envia FILE_REQUEST, recebe chunks
  - Implementar send_index(ip, port, local_index) — envia INDEX_EXCHANGE
  - IMPORTANTE: usar socket.settimeout(10) para evitar bloqueios infinitos
  - IMPORTANTE: implementar retry com backoff exponencial (2s, 4s, 8s, max 3 tentativas)
"""

# import socket
# import json

# class TCPClient:
#     @staticmethod
#     def send_message(ip, port, msg_dict):
#         ...
