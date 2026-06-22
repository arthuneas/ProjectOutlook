"""logs coloridos da interface de terminal."""

from datetime import datetime

from ..config import NODE_NAME

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"


def _log(level, color, message):
    # todos os logs seguem o mesmo formato para facilitar leitura e evidências
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{timestamp}] [{NODE_NAME}] [{level}] {message}{RESET}", flush=True)


def log_info(message):
    _log("INFO", GREEN, message)


def log_error(message):
    _log("ERROR", RED, message)


def log_warn(message):
    _log("WARN", YELLOW, message)


def log_sync(message):
    _log("SYNC", CYAN, message)
