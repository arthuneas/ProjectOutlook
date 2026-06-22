"""framing e validação das mensagens do syncp2p."""

import json
import struct

MSG_HELLO = "HELLO"
MSG_HELLO_ACK = "HELLO_ACK"
MSG_INDEX_EXCHANGE = "INDEX_EXCHANGE"
MSG_FILE_REQUEST = "FILE_REQUEST"
MSG_FILE_TRANSFER_START = "FILE_TRANSFER_START"
MSG_FILE_CHUNK = "FILE_CHUNK"
MSG_FILE_TRANSFER_COMPLETE = "FILE_TRANSFER_COMPLETE"
MSG_FILE_NOTIFY = "FILE_NOTIFY"
MSG_DELETE_NOTIFY = "DELETE_NOTIFY"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"
MSG_NODE_LEAVING = "NODE_LEAVING"
MSG_ERROR = "ERROR"

MESSAGE_TYPES = frozenset(
    {
        MSG_HELLO,
        MSG_HELLO_ACK,
        MSG_INDEX_EXCHANGE,
        MSG_FILE_REQUEST,
        MSG_FILE_TRANSFER_START,
        MSG_FILE_CHUNK,
        MSG_FILE_TRANSFER_COMPLETE,
        MSG_FILE_NOTIFY,
        MSG_DELETE_NOTIFY,
        MSG_HEARTBEAT,
        MSG_HEARTBEAT_ACK,
        MSG_NODE_LEAVING,
        MSG_ERROR,
    }
)

MAX_MESSAGE_SIZE = 8 * 1024 * 1024


class ProtocolError(ValueError):
    """erro de formato ou framing da mensagem."""


def build_message(msg_type, **fields):
    # centralizar a criação impede que tipos desconhecidos circulem pela aplicação
    if msg_type not in MESSAGE_TYPES:
        raise ProtocolError(f"tipo de mensagem desconhecido: {msg_type!r}")
    message = {"type": msg_type}
    message.update(fields)
    return message


def validate_message(message):
    # toda mensagem precisa ser um objeto json com um tipo reconhecido
    if not isinstance(message, dict):
        raise ProtocolError("a mensagem precisa ser um objeto json")
    msg_type = message.get("type")
    if not isinstance(msg_type, str) or msg_type not in MESSAGE_TYPES:
        raise ProtocolError(f"tipo de mensagem inválido: {msg_type!r}")
    return message


def encode_message(message):
    # o json vira bytes antes de receber o cabeçalho de quatro bytes
    validate_message(message)
    try:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"mensagem não serializável: {exc}") from exc
    # o limite evita alocações excessivas causadas por mensagens inválidas
    if not payload or len(payload) > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"payload fora do limite: {len(payload)} bytes")
    return struct.pack(">I", len(payload)) + payload


def send_message(sock, message):
    # sendall garante que cabeçalho e payload sejam entregues ao sistema operacional
    sock.sendall(encode_message(message))


def recv_exact(sock, size):
    # tcp pode devolver apenas uma parte dos bytes solicitados em cada leitura
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("conexão fechada antes do fim da mensagem")
        data.extend(chunk)
    return bytes(data)


def recv_message(sock):
    # primeiro é lido o tamanho e depois exatamente a quantidade indicada
    header = recv_exact(sock, 4)
    payload_size = struct.unpack(">I", header)[0]
    if payload_size == 0 or payload_size > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"tamanho de payload inválido: {payload_size}")
    payload = recv_exact(sock, payload_size)
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"json inválido: {exc}") from exc
    return validate_message(message)
