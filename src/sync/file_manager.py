"""
file_manager.py — Gerenciamento de arquivos: hash, leitura/escrita em chunks.

Responsabilidades:
  1. Calcular hash SHA-256 de arquivos (lendo em chunks para não explodir RAM)
  2. Ler arquivos grandes em pedaços (generator)
  3. Escrever arquivos a partir de chunks recebidos
  4. Escanear diretório completo e retornar índice de arquivos
  5. Deletar arquivos do disco

TODO (Grupo):
  - Implementar get_file_hash(filepath) → SHA-256 hex string
  - Implementar read_file_chunks(filepath, chunk_size) → generator de bytes
  - Implementar save_file_from_chunks(filepath, chunks_list) → escreve no disco
  - Implementar get_file_size(filepath) → int em bytes
  - Implementar scan_directory(folder_path) → dict {filename: {hash, timestamp, size}}
  - Implementar delete_file(filepath)
  - Para Base64 nos chunks TCP: usar base64.b64encode() e base64.b64decode()
"""

# import hashlib
# import os
# import base64

# class FileManager:
#     ...
