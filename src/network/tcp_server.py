"""
tcp_server.py — Servidor TCP para receber conexões de outros nós.

Responsabilidades:
  1. Escutar na porta TCP por conexões de outros nós
  2. Aceitar conexões e criar thread separada para cada cliente
  3. Receber mensagens usando framing do protocol.py (recv_message)
  4. Rotear cada mensagem para o handler correto baseado no 'type'
  5. Responder adequadamente (ex: enviar arquivo quando receber FILE_REQUEST)

Handlers a implementar:
  - _handle_index_exchange(msg, sock) → recebe índice remoto, compara, troca arquivos
  - _handle_file_request(msg, sock) → lê arquivo e envia em chunks
  - _handle_file_notify(msg, sock) → recebe notificação de alteração
  - _handle_delete_notify(msg, sock) → recebe notificação de deleção
  - _handle_heartbeat(msg, sock) → responde com HEARTBEAT_ACK

TODO (Grupo):
  - Implementar TCPServer que recebe referências para state_db, file_manager, reconciler
  - Implementar start() — bind, listen, accept loop
  - Implementar _handle_client(sock, addr) — recv_message + switch no type
  - Implementar cada handler individualmente
  - IMPORTANTE: usar protocol.recv_message() para ler (com framing!)
  - IMPORTANTE: NÃO fechar conexão após 1 mensagem se houver conversa multi-mensagem
"""

# import socket
# import threading
# from config import TCP_PORT

# class TCPServer:
#     def __init__(self, state_db, file_manager, reconciler, discovery):
#         ...
