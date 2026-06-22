# SyncP2P - Clone do OneDrive P2P (Sistemas Distribuídos)

Sistema distribuído de sincronização de arquivos P2P para a disciplina de Redes.

## Status atual

- Etapa 1 concluída: pacote, imports, configuração, SQLite, `main` executável e encerramento gracioso.
- Etapa 2 concluída: protocolo validado, TCP Server concorrente, TCP Client, heartbeat, índices e transferência segura em chunks.
- 13 testes automatizados aprovados.
- Descoberta automática e Watchdog ainda pertencem às próximas etapas; portanto, a sincronização automática completa ainda não está ativa.

Para executar os testes:

```bash
python -m unittest discover -s tests -v
```

## Estrutura do Projeto

```
src/
├── main.py              # Ponto de entrada — orquestra todas as threads
├── config.py            # Constantes: portas, caminhos, IDs
├── network/             # Camada de rede (UDP + TCP)
│   ├── protocol.py      # Tipos de mensagens + framing TCP
│   ├── discovery.py     # Descoberta UDP (broadcast + listener)
│   ├── tcp_server.py    # Servidor TCP (aceita conexões, roteia msgs)
│   └── tcp_client.py    # Cliente TCP (envia mensagens ativamente)
├── sync/                # Camada de sincronização
│   ├── file_manager.py  # Hash SHA-256, leitura/escrita em chunks
│   ├── state_db.py      # Banco de estado local (JSON ou SQLite)
│   ├── watcher.py       # Watchdog: monitora criação/edição/deleção
│   └── reconciler.py    # Lógica de reconciliação e conflitos (LWW)
└── ui/                  # Interface de saída
    └── cli.py           # Logging formatado com timestamp e cores
```

## Como Executar Localmente (Sem Docker)

1. Instale a dependência:
   ```bash
   pip install watchdog
   ```

2. Execute o nó:
   ```bash
   python -m src.main
   ```

3. Para simular múltiplos nós na mesma máquina, abra múltiplos terminais com portas diferentes:
   ```bash
   NODE_NAME=Node1 TCP_PORT=5001 python -m src.main
   NODE_NAME=Node2 TCP_PORT=5002 python -m src.main
   ```

## Como Executar com Docker (Simular Múltiplos Nós)

Para rodar 3 instâncias isoladas na mesma rede, simulando 3 computadores diferentes:

```bash
docker-compose up --build
```

As pastas `node1_data/`, `node2_data/` e `node3_data/` serão criadas na raiz do projeto.
Qualquer arquivo colocado em uma delas deve sincronizar automaticamente com as outras.

### Comandos úteis

```bash
# Ver logs de um nó
docker-compose logs -f node1

# Parar um nó (simular falha)
docker stop projectoutlook-node2-1

# Reiniciar nó (simular reconexão)
docker start projectoutlook-node2-1

# Copiar arquivo para um nó
docker cp foto.jpg projectoutlook-node1-1:/app/shared_folder/

# Derrubar tudo
docker-compose down
```

## Documentação

- [Especificação do Protocolo](docs/protocolo.md)
- [Cenários de Teste](docs/cenarios_teste.md)
- [Relatório Técnico](docs/relatorio_tecnico.md)
