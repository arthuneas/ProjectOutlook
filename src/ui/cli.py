"""
cli.py — Interface de linha de comando e funções de log.

Fornece funções de log formatadas com timestamp, nome do nó e cores ANSI.
Todos os módulos do projeto devem usar estas funções para output no terminal.

TODO (Grupo):
  - Implementar log_info(), log_error(), log_warn(), log_sync()
  - Usar códigos ANSI para cores (verde, vermelho, amarelo, ciano)
  - Formato: [HH:MM:SS] [NodeName] [TIPO] mensagem
"""

# Códigos de cor ANSI:
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'

from datetime import datetime
from src.config import NODE_NAME

#função para retornar o datetime
def get_Time():
  return datetime.now().strftime("%H:%M:%S")



#informa que é uma mensagem de sucesso
def log_info(msg):
  print(f"{GREEN} [{get_Time()}] [{NODE_NAME}] [INFO] {msg} {RESET}")


#informa que é um erro
def log_error(msg):
  print(f"{RED} [{get_Time()}] [{NODE_NAME}] [ERROR] {msg} {RESET}")


#informa que é um aviso
def log_warn(msg):
  print(f"{YELLOW} [{get_Time()}] [{NODE_NAME}] [WARN] {msg} {RESET}")


def log_sync(msg):
  print(f"{CYAN} [{get_Time()}] [{NODE_NAME}] [SYNC] {msg} {RESET}")
