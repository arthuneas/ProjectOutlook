"""índice persistente e thread-safe dos arquivos conhecidos."""

import sqlite3
import threading
from pathlib import Path

from ..config import DB_PATH


class StateDB:
    def __init__(self, db_path=DB_PATH):
        # a conexão é compartilhada pelos workers e protegida por um lock reentrante
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._closed = False
        self._create_table()

    def _create_table(self):
        # o nome é a chave porque existe apenas um estado atual por arquivo
        with self.lock, self.conn:
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS file_state (
                       filename TEXT PRIMARY KEY,
                       hash TEXT NOT NULL,
                       timestamp REAL NOT NULL,
                       size INTEGER NOT NULL,
                       status TEXT NOT NULL DEFAULT 'ACTIVE'
                   )"""
            )

    def update_file_state(self, filename, file_hash, timestamp, size, status="ACTIVE"):
        # o upsert cria ou substitui todos os metadados da versão conhecida
        if status not in {"ACTIVE", "DELETED"}:
            raise ValueError(f"status inválido: {status}")
        with self.lock, self.conn:
            self.conn.execute(
                """INSERT INTO file_state (filename, hash, timestamp, size, status)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(filename) DO UPDATE SET
                     hash=excluded.hash,
                     timestamp=excluded.timestamp,
                     size=excluded.size,
                     status=excluded.status""",
                (filename, file_hash, float(timestamp), int(size), status),
            )

    def mark_deleted(self, filename, timestamp):
        # o tombstone também é criado quando o arquivo ainda não existe neste banco
        with self.lock, self.conn:
            self.conn.execute(
                """INSERT INTO file_state (filename, hash, timestamp, size, status)
                   VALUES (?, '', ?, 0, 'DELETED')
                   ON CONFLICT(filename) DO UPDATE SET
                     timestamp=excluded.timestamp,
                     status='DELETED'""",
                (filename, float(timestamp)),
            )

    def get_file_state(self, filename):
        with self.lock:
            row = self.conn.execute(
                "SELECT hash, timestamp, size, status FROM file_state WHERE filename = ?",
                (filename,),
            ).fetchone()
        if row is None:
            return None
        return {"hash": row[0], "timestamp": row[1], "size": row[2], "status": row[3]}

    def get_full_index(self):
        # o dicionário pode ser enviado diretamente em uma mensagem index_exchange
        with self.lock:
            rows = self.conn.execute(
                "SELECT filename, hash, timestamp, size, status FROM file_state"
            ).fetchall()
        return {
            row[0]: {"hash": row[1], "timestamp": row[2], "size": row[3], "status": row[4]}
            for row in rows
        }

    def file_exists(self, filename):
        state = self.get_file_state(filename)
        return bool(state and state["status"] == "ACTIVE")

    def get_active_files(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT filename FROM file_state WHERE status = 'ACTIVE' ORDER BY filename"
            ).fetchall()
        return [row[0] for row in rows]

    def close(self):
        # chamadas repetidas são ignoradas para tornar sinais concorrentes seguros
        with self.lock:
            if not self._closed:
                self.conn.close()
                self._closed = True
