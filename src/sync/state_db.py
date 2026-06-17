"""
state_db.py — Banco de dados de estado local (persistência do índice de arquivos).

Responsabilidades:
  1. Persistir o estado de cada arquivo: nome, hash, timestamp, tamanho, status
  2. Fornecer o índice completo para troca com outros nós (INDEX_EXCHANGE)
  3. Atualizar estado individual de arquivos
  4. Marcar arquivos como DELETED (tombstone) — NÃO REMOVER do banco
  5. Thread-safe: proteger leitura/escrita com Lock

TOMBSTONES (IMPORTANTE):
  Quando um arquivo é deletado, NÃO remova a entrada do banco.
  Mude o status para "DELETED" e atualize o timestamp.
  Se você remover, na próxima INDEX_EXCHANGE o nó vai pensar que
  não conhece o arquivo e vai baixá-lo de novo — desfazendo a deleção!

OPÇÕES DE IMPLEMENTAÇÃO:
  Opção A: Arquivo JSON simples (mais fácil, mas menos robusto)
  Opção B: SQLite (mais robusto, recomendado — usa sqlite3 da stdlib)

TODO (Grupo):
  - Implementar StateDB com Lock para thread-safety
  - Implementar get_full_index() → dict completo para INDEX_EXCHANGE
  - Implementar get_file_state(filename) → dados de um arquivo
  - Implementar update_file_state(filename, hash, timestamp, size, status='ACTIVE')
  - Implementar mark_deleted(filename, timestamp)
  - Implementar file_exists(filename) → bool (apenas ACTIVE)
  - Implementar get_active_files() → lista de filenames ativos
"""

# import json
# import os
# import threading
# from config import DB_PATH

# class StateDB:
#     def __init__(self):
#         ...
