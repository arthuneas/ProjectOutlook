import json
import os
from config import DB_PATH

class StateDB:
    def __init__(self):
        self.db_path = DB_PATH
        if not os.path.exists(self.db_path):
            self._save_state({})
            
    def _load_state(self):
        with open(self.db_path, 'r') as f:
            return json.load(f)

    def _save_state(self, state):
        with open(self.db_path, 'w') as f:
            json.dump(state, f, indent=4)

    def get_file_state(self, filename):
        state = self._load_state()
        return state.get(filename)

    def update_file_state(self, filename, hash_val, timestamp, status="ACTIVE"):
        state = self._load_state()
        state[filename] = {
            "hash": hash_val,
            "timestamp": timestamp,
            "status": status
        }
        self._save_state(state)
