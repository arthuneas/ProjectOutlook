"""
file_manager.py — Gerenciamento de arquivos: hash, leitura/escrita em chunks.

Responsabilidades:
  1. Calcular hash SHA-256 de arquivos (lendo em chunks para não explodir RAM)
  2. Ler arquivos grandes em pedaços (generator)
  3. Escrever arquivos a partir de chunks recebidos
  4. Escanear diretório completo e retornar índice de arquivos
  5. Deletar arquivos do disco

"""

import hashlib
import os
import base64
from ..config import CHUNK_SIZE, SHARED_FOLDER

class FileManager:

    #método para identificar a modificação do arquivo, gera um id (sha-256) do conteúdo.
    
    @staticmethod
    def get_file_hash(filepath):
      
        if not os.path.exists(filepath): #confere a existência do arquivo
            return None
          
        idhash = hashlib.sha256() #identificador 
        
        with open(filepath, 'rb') as arquivo: #abertura de arquivo binário no modo de leitura
            
            while True: 
                conteudoBloco = arquivo.read(CHUNK_SIZE) #lê os blocos de tamanho de CHUNK_SIZE 
                
                if not conteudoBloco: #se bloco for vazio quebra o loop
                    break
                  
                idhash.update(conteudoBloco) #adiciona o conteudo da chunk no idhash
                
            return idhash.hexdigest() #retorna a string/ID do arquivo
          
          
    #lê o chunk de dado e retorna o pedaço. Faz que a entrega não sobrecarrege a RAM. Variação na CHUNK_SIZE faz com que a leitura seja dinãmica
    @staticmethod
    def read_file_chunks(filepath, chunk_size = CHUNK_SIZE):
        
        with open(filepath, 'rb') as arquivo: #abertura no modo de leitura
          
          while True: 
              conteudoBloco = arquivo.read(chunk_size) 
        
              if not conteudoBloco: #se bloco for vazio quebra o loop
                break
              
              yield conteudoBloco #retorna o pacote e esse retorno permite entrega parcial e continua dos dados de um conteúo completo
              
            
    #salva o pedaço de dado no arquivo
    @staticmethod
    def save_file_from_chunks(filepath, chunks_list):    
        
        with open(filepath, 'wb') as arquivo: #abre o arquivo no modo de escrita
            
            for chunk in chunks_list:
                arquivo.write(chunk) #escreve a chunk da lista no arquivo
                
                
    #metodo para acessar o tamanho do arquivo             
    @staticmethod
    def get_file_size(filepath):  
      
        if not os.path.exists(filepath): #verifica existencia de arquivo
            return 0
          
        else:   
            return os.path.getsize(filepath) #retorna o tamanho em bytes
          
         
    #metodo para apagar arquivos
    @staticmethod
    def delete_file(filepath):  
      
        if os.path.exists(filepath):
            os.remove(filepath)        
            
            
    #mapeia os arquivos da pasta e constroi dicionário de dados sobre dados (metadados)        
    def scan_directory(self, folder_path = SHARED_FOLDER):  
      
        index = {} #dicionario vazio para armazenar metadados 
        
        for filename in os.listdir(folder_path): #loop para cada arquivo no folder
            filepath = os.path.join(folder_path, filename) #filepath vai colocar no folder o arquivo atual no diretório do loop
            
            if os.path.isfile(filepath):
                size = FileManager.get_file_size(filepath) #pega o tamanho do arquivo
                time = os.path.getmtime(filepath) #pega o timestamp do arquivo
                gethash = FileManager.get_file_hash(filepath) #pega o hash do arquivo 
                
                #coloca no dicionario as informações capturadas no index do filename
                index[filename] = {
                    "size" : size,
                    "timestamp" : time, 
                    "hash" : gethash
                }
                
        return index 
                