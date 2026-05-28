import socket
import threading
from config import TCP_PORT
from ui.cli import log_info

class TCPServer:
    def __init__(self):
        self.port = TCP_PORT

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        log_info(f"Servidor TCP escutando na porta {self.port}...")
        
        while True:
            client_socket, addr = server.accept()
            log_info(f"Conexão TCP aceita de {addr}")
            handler = threading.Thread(
                target=self._handle_client, 
                args=(client_socket, addr), 
                daemon=True
            )
            handler.start()

    def _handle_client(self, client_socket, addr):
        try:
            # Lógica para receber mensagens e lidar com requisições
            # Ex: trocar índice de arquivos, transferir chunks de arquivos.
            data = client_socket.recv(1024)
            if data:
                log_info(f"Mensagem recebida: {data.decode('utf-8')}")
        except Exception as e:
            log_info(f"Erro na conexão com {addr}: {e}")
        finally:
            client_socket.close()
