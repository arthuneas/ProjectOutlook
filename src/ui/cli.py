import datetime
from config import NODE_NAME

def log_info(msg):
    # Formata um log bonitinho para o terminal
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{NODE_NAME}] {msg}")

def log_error(msg):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{NODE_NAME}] ERROR: {msg}")
