"""
cli.py — Interface de linha de comando e funções de log.

Fornece funções de log formatadas com timestamp, nome do nó e cores ANSI.
Todos os módulos do projeto devem usar estas funções para output no terminal.

"""

# Códigos de cor ANSI:
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'

from datetime import datetime
from config import NODE_NAME

#função para retornar o datetime
def getTime():
  return datetime.now().strftime("%H:%M:%S")


#informa que é uma mensagem de sucesso
def log_info(msg):
  print(f"{GREEN} [{getTime()}] [{NODE_NAME}] [INFO] {msg} {RESET}")


#informa que é um erro
def log_error(msg):
  print(f"{RED} [{getTime()}] [{NODE_NAME}] [ERROR] {msg} {RESET}")


#informa que é um aviso
def log_warn(msg):
  print(f"{YELLOW} [{getTime()}] [{NODE_NAME}] [WARN] {msg} {RESET}")

#informa que é uma sincronicidade
def log_sync(msg):
  print(f"{CYAN} [{getTime()}] [{NODE_NAME}] [SYNC] {msg} {RESET}")
