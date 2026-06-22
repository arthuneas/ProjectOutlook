"""
tcp_client.py — Cliente TCP para enviar mensagens ativamente para outros nós.

Responsabilidades:
  1. Conectar a um nó remoto via TCP
  2. Enviar mensagens usando framing do protocol.py (send_message)
  3. Opcionalmente aguardar resposta (send_and_receive)
  4. Solicitar e receber arquivos (request_file)

TODO (Grupo):
  - Implementar send_message(ip, port, msg_dict) — conecta, envia com framing, fecha [ok]
  - Implementar send_and_receive(ip, port, msg_dict) — conecta, envia, espera resposta [ok]
  - Implementar request_file(ip, port, filename, save_path) — envia FILE_REQUEST, recebe chunks
  - Implementar send_index(ip, port, local_index) — envia INDEX_EXCHANGE [ok]
  - IMPORTANTE: usar socket.settimeout(10) para evitar bloqueios infinitos [ok]
  - IMPORTANTE: implementar retry com backoff exponencial (2s, 4s, 8s, max 3 tentativas) [ok]
"""

import socket
import json
import time #importante pra contar o período do backoff

from .protocol import (
     build_message,
     send_message,
     recv_message,
     MSG_INDEX_EXCHANGE,
     MSG_FILE_REQUEST,       
     MSG_FILE_TRANSFER_START,
     MSG_FILE_CHUNK,         
     MSG_FILE_TRANSFER_COMPLETE
)

from ..sync.file_manager import FileManager

from ..ui.cli import (
     log_info,
     log_error,
     log_sync,
     log_warn
)

class TCPClient:
  #nota para si: aqui não tem __init__ porque o TCPClient é stateless! não precisa guardar nada, logo não precisa de construtor.
  #primeiro implementando esse backoff exponencial, para evitar inundar a rede com requisições demais sendo que as últimas acabaram de ser negadas (ou seja, esperar um tiquinho)
  
  @staticmethod
  def conexao_backoff(ip, port):
          tentativas = 0
          tempo_espera = 2 #segundos. tempo de espera inicial (olhe linha 55)

          while tentativas < 3: #implementando máximo de 3 tentativas
               try:
                    #criando o socket tcp
                    socketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    socketTCP.settimeout(10)
                    socketTCP.connect((ip, port))
                    return socketTCP #conectado!
                    
               except:
                    tentativas += 1
                    if tentativas == 3:
                         raise RuntimeError("Conexão mal-sucedida: Servidor inacessível após 3 tentativas")
                    time.sleep(tempo_espera)
                    tempo_espera = tempo_espera * 2 #na segunda será 4s, na terceira será 8s
              
  @staticmethod
  def send_message(ip, port, msg_dict):
            try:
                 socketTCP = TCPClient.conexao_backoff(ip, port)
                 send_message(socketTCP, msg_dict) #botando framing nesse troço
                 socketTCP.close()
                 log_info("Mensagem enviada com sucesso")

            except Exception as e:
                 log_error(f"Erro: não foi possível enviar a mensagem. Erro: {e}") #chamando o cli

  @staticmethod
  def send_and_receive(ip, port, msg_dict):
          try:
            socketTCP = TCPClient.conexao_backoff(ip, port)
            send_message(socketTCP, msg_dict)

            resposta = recv_message(socketTCP)
            socketTCP.close()
            return resposta
          
          except Exception as e:
            log_error(f"Erro: não foi possível concluir o envio e a recepção da resposta. Erro: {e}")
            return None

  @staticmethod        
  def send_index(ip, port, local_index):
       msg = build_message(MSG_INDEX_EXCHANGE, files=local_index)
       TCPClient.send_message(ip, port, msg)

  @staticmethod
  def request_file(ip, port, filename, save_path):
       try:
            socketTCP = TCPClient.conexao_backoff(ip, port)

            #ei me da arquivo
            msg_request = build_message(MSG_FILE_REQUEST, filename=filename)
            send_message(socketTCP, msg_request)

            #aguardando processamento pelo protocolo...
            msg_start = recv_message(socketTCP)

            #dependendo da resposta do protocolo (msg_start), eu começo ou não a transferência
            if msg_start["type"] == MSG_FILE_TRANSFER_START:
                 tamanho_esperado = msg_start["size"]
                 hash_esperado = msg_start["hash"]
                
                #vou criar uma lista de recebimento, porque o arquivo não vai ser enviado inteiro, e sim em pedaços para não 
                #sobrecarregar a ram. Inicialmente, a lista está vazia (ela vai armazenar o que já foi recebido, pra depois)
                #juntar tudo num arquivo só e guardar no disco rígido 
                 lista_de_chunks = []

                #agora vamos criar o loop pra receber os chunks!
                 while True:
                  msg_chunk = recv_message(socketTCP)
                  if msg_chunk["type"] == MSG_FILE_CHUNK:
                       lista_de_chunks.append(msg_chunk["data"])

                       if msg_chunk["is_last"] == True:
                        break
                
                 #agora que recebi tudo, vou gravar no disco
                 FileManager.save_file_from_chunks(save_path, lista_de_chunks)
                 
                 #checando se deu certo mesmo
                 hash_local = FileManager.get_file_hash(save_path)
                 if hash_local == hash_esperado:
                      #confirmando pro usuario via cli
                      msg_sucesso = build_message(MSG_FILE_TRANSFER_COMPLETE, filename=filename)
                      send_message(socketTCP, msg_sucesso)
                      log_sync(f"Download concluído com sucesso: {filename}")
                 else:
                      log_error("Arquivo corrompido na transmissão (Hash Mismatch)")
          
            socketTCP.close()

       except Exception as e:
            log_error(f"Falha no download do arquivo. Erro: {e}")
          
      
      
                 
