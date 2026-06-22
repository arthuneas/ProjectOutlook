"""gerenciamento de arquivos: hash, leitura/escrita em chunks e varredura."""

import hashlib
import os
import base64
from pathlib import Path

from ..config import CHUNK_SIZE, SHARED_FOLDER


class FileManager:

    @staticmethod
    def get_file_hash(filepath):
        if not os.path.exists(filepath):
            return None
        digest = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                block = f.read(CHUNK_SIZE)
                if not block:
                    break
                digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def read_file_chunks(filepath, chunk_size=CHUNK_SIZE):
        with open(filepath, "rb") as f:
            while True:
                block = f.read(chunk_size)
                if not block:
                    break
                yield block

    @staticmethod
    def save_file_from_chunks(filepath, chunks_list):
        with open(filepath, "wb") as f:
            for chunk in chunks_list:
                if isinstance(chunk, str):
                    f.write(base64.b64decode(chunk))
                else:
                    f.write(chunk)

    @staticmethod
    def get_file_size(filepath):
        if not os.path.exists(filepath):
            return 0
        return os.path.getsize(filepath)

    @staticmethod
    def delete_file(filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    def scan_directory(self, folder_path=SHARED_FOLDER):
        index = {}
        for filename in os.listdir(folder_path):
            filepath = os.path.join(folder_path, filename)
            if os.path.isfile(filepath):
                index[filename] = {
                    "size": FileManager.get_file_size(filepath),
                    "timestamp": os.path.getmtime(filepath),
                    "hash": FileManager.get_file_hash(filepath),
                }
        return index
