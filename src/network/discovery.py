"""
discovery.py — Módulo de Descoberta de Nós via UDP Broadcast.

Responsabilidades:
  1. Enviar broadcasts UDP periódicos (HELLO) anunciando presença na rede
  2. Escutar broadcasts de outros nós e registrá-los em known_nodes
  3. Manter a lista known_nodes atualizada (thread-safe com Lock)
  4. Notificar o sistema quando um novo nó é descoberto (callback)

Como funciona:
  - Ao iniciar, o nó "grita" na rede: "Eu existo, minha porta TCP é X"
  - Outros nós escutam e registram o IP e porta do novo nó
  - Broadcasts são repetidos periodicamente (heartbeat de descoberta)

TODO (Grupo):
  - Implementar DiscoveryManager com known_nodes protegido por Lock
  - Implementar _listen_for_broadcasts() — socket UDP, bind, recvfrom loop
  - Implementar _broadcast_presence() — socket UDP com SO_BROADCAST
  - Implementar _broadcast_loop() — loop periódico
  - Implementar get_active_nodes() — retorna cópia do dicionário
  - Implementar remove_node(node_id) — remove nó que falhou
  - Adicionar callback on_new_node para sincronização automática
  - IMPORTANTE: Usar threading.Lock() para proteger known_nodes
  - IMPORTANTE: Configurar SO_REUSEADDR e SO_REUSEPORT no socket
"""

# import socket
# import threading
# import time
# import json
# from config import UDP_PORT, TCP_PORT, BROADCAST_IP, NODE_ID, NODE_NAME, DISCOVERY_INTERVAL

# class DiscoveryManager:
#     def __init__(self, on_new_node=None):
#         """
#         Args:
#             on_new_node: callback chamado quando novo nó é descoberto.
#                          Recebe (node_id, node_info_dict).
#         """
#         ...
