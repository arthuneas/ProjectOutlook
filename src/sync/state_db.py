"""
state_db.py — Banco de dados de estado local (persistência do índice de arquivos).

Responsabilidades:
  1. Persistir o estado de cada arquivo: nome, hash, timestamp, tamanho, status
  2. Fornecer o índice completo para troca com outros nós (INDEX_EXCHANGE)
  3. Atualizar estado individual de arquivos
  4. Marcar arquivos como DELETED (tombstone) — NÃO REMOVER do banco
  5. Thread-safe: proteger leitura/escrita com Lock

TOMBSTONES (IMPORTANTE):
  Quando um arquivo é deletado, NÃO remova a entrada do banco.
  Mude o status para "DELETED" e atualize o timestamp.
  Se você remover, na próxima INDEX_EXCHANGE o nó vai pensar que
  não conhece o arquivo e vai baixá-lo de novo — desfazendo a deleção!

"""


import json
import os
import threading
import sqlite3
from config import DB_PATH

class StateDB:
    def __init__(self):
        self.lock = threading.Lock() # protege operações de banco
        
        self.conn = sqlite3.connect(DB_PATH, check_same_thread = False) #instancia o banco de dados em um path fixo, a flag permite que as threads do serevidor tcp e watchdog trabalhem juntas
        
        self._create_table() #instancia a criação do banco de dados (schema)
        
        
    def _create_table(self):
        with self.lock: #protege as threads, para evito de conflitos na atuação do schema
          cursor = self.conn.cursor() #metodo para a criação do banco
          
          #cria campos da tabela
          create_table = """ 
          
              CREATE TABLE IF NOT EXISTS file_state (
                filename TEXT PRIMARY KEY,
                hash TEXT NOT NULL,
                timestamp REAL NOT NULL,
                size INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE'
              )
        
            """
          
          cursor.execute(create_table) #cria tabela 
          
          self.conn.commit() #salva alteraçõs no hd
        
        
    def update_file_state(self, filename, fileHash, timestamp, size, status = 'ACTIVE'):
        with self.lock:
            cursor = self.conn.cursor()
            
            #query para update no sqlite3
            #usa-se (?, ?, ?, ?, ?) para evitar sql injection
            update_table = """

                INSERT OR REPLACE INTO file_state (filename, hash, timestamp, size, status)
                VALUES (?, ?, ?, ?, ?)
      
              """
            
            cursor.execute(update_table, (filename, fileHash, timestamp, size, status))
            
            self.conn.commit()
            
            
    def mark_deleted(self, filename, timestamp):
        with self.lock:
            cursor = self.conn.cursor()
            
            #query para deletar 
            delete_table = """

                UPDATE file_state 
                SET timestamp = ?, status = 'DELETED' 
                WHERE filename = ?
      
              """
            
            cursor.execute(delete_table, (timestamp, filename))
            
            self.conn.commit()
            
            
    def get_file_name(self, filename):
      with self.lock:
          cursor = self.conn.cursor()
          
          select_table = """

              SELECT hash, timestamp, size, status
              FROM file_state 
              WHERE filename = ?
      
            """
          
          cursor.execute(select_table, (filename,))
          
          row = cursor.fetchone() #recupera a proxima linha na consulta no banco de dados
            
          
          if row:
            campos = {
              "hash" : row[0], 
              "timestamp" : row[1], 
              "size" : row[2], 
              "status" : row[3]
            }
            
            return campos #apos verificar a existencia dos campos o retorna um dicionários com os campos(coluna) da linha
            
          return None #case haja falhas, retorna nada
          
         
    def get_full_index(self):
        with self.lock:
            cursor = self.conn.cursor() 
            
            selectAll_table = """

                SELECT filename, hash, timestamp, size, status
                FROM file_state 
    
              """
          
            cursor.execute(selectAll_table)     
            
            rows = cursor.fetchall() #armazena rodas as linhas de uma vez em tuplas, formando uma lista de tuplas
            
            index = {} #será armazenados os valores formatados da consulta
            
            for row in rows:
                index[row[0]] = {
                    "hash" : row[1], 
                    "timestamp" : row[2], 
                    "size" : row[3], 
                    "status" : row[4]
                }
                
            return index #retorna dicionário com o resultado da consulta
          
          
          
    def file_exists(self, filename):
        with self.lock:
            cursor = self.conn.cursor()       
            
            selectOne_table = """

                SELECT 1 FROM file_state 
                WHERE filename = ?
                AND STATUS = 'ACTIVE'
    
              """
              
            cursor.execute(selectOne_table, (filename, ))     
            
            row = cursor.fetchone()
            
            #conferencia na existencia de dados na linha 
            return row is not None
          
          
    def get_active_files(self):
        with self.lock:
            cursor = self.conn.cursor()  
            
            select_table = """

                    SELECT filename FROM file_state 
                    WHERE STATUS = 'ACTIVE'
      
                  """
                  
            cursor.execute(select_table) 
            
            rows = cursor.fetchall()
            
            arquivos = [] #coloca os arquivos em uma lista
            
            for row in rows:
                arquivos.append(row[0]) #coloca a linha do arquivo ativo na lista com todos
                    
              
            return arquivos #retorna a lista dos arquivos ativos 