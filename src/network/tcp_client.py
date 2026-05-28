import socket
import json
from ui.cli import log_info, log_error

class TCPClient:
    @staticmethod
    def send_message(ip, port, message_dict):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, port))
            msg = json.dumps(message_dict)
            client.send(msg.encode('utf-8'))
            log_info(f"Mensagem enviada para {ip}:{port}")
            client.close()
            return True
        except Exception as e:
            log_error(f"Falha ao enviar mensagem para {ip}:{port} -> {e}")
            return False
