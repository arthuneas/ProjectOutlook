import json

# Constantes de Tipos de Mensagens
MSG_HELLO = "HELLO"
MSG_HELLO_ACK = "HELLO_ACK"
MSG_INDEX_EXCHANGE = "INDEX_EXCHANGE"
MSG_FILE_REQUEST = "FILE_REQUEST"
MSG_FILE_CHUNK = "FILE_CHUNK"

def build_message(msg_type, **kwargs):
    msg = {"type": msg_type}
    msg.update(kwargs)
    return json.dumps(msg)
