import hashlib
import os

class FileManager:
    @staticmethod
    def get_file_hash(filepath):
        """Retorna o SHA-256 de um arquivo."""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            return None

    @staticmethod
    def read_file_chunks(filepath, chunk_size=4096):
        """Gerador para ler arquivos grandes em pedaços."""
        with open(filepath, 'rb') as f:
            while chunk := f.read(chunk_size):
                yield chunk
