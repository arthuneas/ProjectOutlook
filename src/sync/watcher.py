"""monitoramento de alterações na shared_folder com watchdog."""

import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ..config import DEBOUNCE_DELAY, SHARED_FOLDER
from ..ui.cli import log_info, log_warn


class SyncEventHandler(FileSystemEventHandler):
    """detecta criação, modificação, deleção e movimentação de arquivos."""

    def __init__(self, on_change, files_being_synced, debounce_delay=DEBOUNCE_DELAY):
        super().__init__()
        # on_change recebe somente eventos consolidados depois do debounce
        self.on_change = on_change
        # files_being_synced identifica escritas remotas que não podem voltar para a rede
        self.files_being_synced = files_being_synced
        # debounce_delay define quanto tempo esperar por eventos duplicados do editor
        self.debounce_delay = debounce_delay

        # timers pendentes por filename — reiniciados a cada evento duplicado
        self._pending: dict[str, threading.Timer] = {}
        # o lock protege o dicionário porque timers executam em threads diferentes
        self._lock = threading.Lock()
        # active impede callbacks tardios depois que o watcher foi encerrado
        self._active = True

    # ------------------------------------------------------------------
    # handlers do watchdog
    # ------------------------------------------------------------------

    def on_created(self, event):
        # eventos de diretório são ignorados porque a versão atual sincroniza só arquivos
        if not event.is_directory:
            self._schedule(Path(event.src_path).name, "CREATED")

    def on_modified(self, event):
        # path.name remove a parte absoluta e mantém apenas o nome usado pelo protocolo
        if not event.is_directory:
            self._schedule(Path(event.src_path).name, "MODIFIED")

    def on_deleted(self, event):
        # o arquivo já não existe, então somente nome e ação seguem para o main
        if not event.is_directory:
            self._schedule(Path(event.src_path).name, "DELETED")

    def on_moved(self, event):
        if not event.is_directory:
            # renomear = deletar o nome antigo e criar o novo
            self._schedule(Path(event.src_path).name, "DELETED")
            self._schedule(Path(event.dest_path).name, "CREATED")

    # ------------------------------------------------------------------
    # debouncing
    # ------------------------------------------------------------------

    def _schedule(self, filename: str, action: str):
        # arquivos que chegaram via rede não devem ser propagados de volta
        if filename in self.files_being_synced:
            return

        with self._lock:
            # um handler parado não deve aceitar novos timers durante o shutdown
            if not self._active:
                return
            # salvar um arquivo pode emitir vários modified; o timer mais recente substitui o anterior
            existing = self._pending.get(filename)
            if existing:
                existing.cancel()
            timer = threading.Timer(
                self.debounce_delay,
                self._fire,
                args=(filename, action),
            )
            self._pending[filename] = timer
            # timers daemon não impedem o encerramento do processo em caso de falha externa
            timer.daemon = True
            timer.start()

    def _fire(self, filename: str, action: str):
        # o timer concluído é removido antes do callback para liberar novos eventos do mesmo nome
        with self._lock:
            self._pending.pop(filename, None)
            if not self._active:
                return
        log_info(f"mudança detectada: {action} → {filename}")
        try:
            # exceções do main são registradas sem encerrar a thread interna do watchdog
            self.on_change(filename, action)
        except Exception as exc:
            log_warn(f"erro no callback do watcher para {filename}: {exc}")

    def cancel_all(self):
        # cancelamento coletivo evita que timers escrevam no banco depois do stop
        with self._lock:
            self._active = False
            for timer in self._pending.values():
                timer.cancel()
            self._pending.clear()

    def resume(self):
        # a mesma instância de handler pode ser reutilizada depois de stop e start
        with self._lock:
            self._active = True


class SyncingGuard:
    """controla quais arquivos estão sendo escritos via sincronização de rede."""

    def __init__(self):
        # o conjunto permite consulta rápida e o lock coordena watcher com threads de rede
        self._set: set[str] = set()
        self._lock = threading.Lock()

    def add(self, filename: str):
        # deve ser chamado antes que a escrita recebida pela rede comece
        with self._lock:
            self._set.add(filename)

    def remove(self, filename: str):
        # discard não levanta KeyError se o arquivo já foi removido por outra thread
        with self._lock:
            self._set.discard(filename)

    def __contains__(self, filename: str):
        # permite usar "filename in guard" sem expor _set ou _lock para o chamador
        with self._lock:
            return filename in self._set

    def release_later(self, filename: str, delay: float = 1.0):
        """mantém a proteção até os eventos atrasados do sistema de arquivos chegarem."""
        # alguns eventos chegam depois do fechamento do arquivo, por isso a remoção não é imediata
        timer = threading.Timer(delay, self.remove, args=(filename,))
        timer.daemon = True
        timer.start()
        return timer


class DirectoryWatcher:
    """inicializa o Observer do watchdog e expõe start/stop."""

    def __init__(self, on_change=None, folder=SHARED_FOLDER, debounce_delay=DEBOUNCE_DELAY):
        # folder é convertido para string porque esta é a interface esperada pelo observer
        self.folder = str(folder)
        self.on_change = on_change

        # guard compartilhado: main.py chama .add() antes de escrever um arquivo recebido
        self.syncing = SyncingGuard()

        self._handler = SyncEventHandler(
            on_change=self._handle_change,
            files_being_synced=self.syncing,
            debounce_delay=debounce_delay,
        )
        self._observer: Observer | None = None

    def start(self):
        # start idempotente evita registrar a mesma pasta duas vezes no mesmo observer
        if self._observer and self._observer.is_alive():
            return
        # a pasta é criada antes do schedule para evitar erro em instalações novas
        Path(self.folder).mkdir(parents=True, exist_ok=True)
        self._handler.resume()
        self._observer = Observer()
        # recursive false limita o escopo à raiz compartilhada, como definido no projeto atual
        self._observer.schedule(self._handler, self.folder, recursive=False)
        self._observer.start()
        log_info(f"watchdog monitorando: {self.folder}")

    def stop(self):
        # primeiro bloqueia timers novos, depois interrompe e aguarda a thread do observer
        if self._observer:
            self._handler.cancel_all()
            self._observer.stop()
            self._observer.join(timeout=3)
            self._observer = None

    def _handle_change(self, filename: str, action: str):
        # a camada externa recebe uma interface simples, sem objetos específicos do watchdog
        if self.on_change:
            self.on_change(filename, action)

    def mark_syncing(self, filename: str):
        """marca um arquivo antes que uma escrita recebida pela rede comece."""
        # este método evita que o main precise acessar o guard interno diretamente
        self.syncing.add(filename)

    def release_syncing(self, filename: str, delay: float = 1.0):
        """remove a proteção depois que eventos atrasados do watchdog forem emitidos."""
        return self.syncing.release_later(filename, delay)
