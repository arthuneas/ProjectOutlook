import time
from ui.cli import log_info

class DirectoryWatcher:
    def __init__(self):
        pass

    def start(self):
        # Aqui o grupo deve instanciar a biblioteca "watchdog"
        # Para ficar ouvindo eventos de FileSystemEvent (criado, modificado, deletado)
        # na pasta compartilhada (config.SHARED_FOLDER).
        log_info("Monitorador de diretório iniciado...")
        
        while True:
            # Simulação do watchdog loop
            time.sleep(1)
