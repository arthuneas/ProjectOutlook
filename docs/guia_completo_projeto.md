# 📘 Guia Completo — Projeto de Sincronização P2P (Clone OneDrive)

> **Opção 2 do Trabalho de Redes — Sistemas Distribuídos**
> Linguagem: Python 3.10+ | Protocolo: TCP + UDP | Infraestrutura: Docker Compose

---

## Sumário

1. [Visão Geral do Projeto](#1-visão-geral-do-projeto)
2. [Arquitetura do Sistema](#2-arquitetura-do-sistema)
3. [Estrutura de Pastas e Responsabilidade de Cada Arquivo](#3-estrutura-de-pastas)
4. [Protocolo de Comunicação (Especificação Completa)](#4-protocolo-de-comunicação)
5. [Etapa 1 — Configuração e Infraestrutura Base](#5-etapa-1--configuração-e-infraestrutura-base)
6. [Etapa 2 — Descoberta de Nós (UDP Broadcast)](#6-etapa-2--descoberta-de-nós-udp-broadcast)
7. [Etapa 3 — Comunicação TCP (Servidor e Cliente)](#7-etapa-3--comunicação-tcp)
8. [Etapa 4 — Monitoramento de Arquivos (Watchdog)](#8-etapa-4--monitoramento-de-arquivos)
9. [Etapa 5 — Banco de Estado Local (StateDB)](#9-etapa-5--banco-de-estado-local)
10. [Etapa 6 — Troca de Índices e Reconciliação](#10-etapa-6--troca-de-índices-e-reconciliação)
11. [Etapa 7 — Transferência de Arquivos (Chunks)](#11-etapa-7--transferência-de-arquivos)
12. [Etapa 8 — Propagação de Alterações e Deleções](#12-etapa-8--propagação-de-alterações-e-deleções)
13. [Etapa 9 — Tolerância a Falhas e Reconexão](#13-etapa-9--tolerância-a-falhas)
14. [Etapa 10 — Integração no main.py (Orquestração)](#14-etapa-10--orquestração-no-mainpy)
15. [Etapa 11 — Docker e Simulação Multi-Nó](#15-etapa-11--docker)
16. [Etapa 12 — Testes e Cenários de Demonstração](#16-etapa-12--testes-e-cenários)
17. [Etapa 13 — Relatório Técnico](#17-etapa-13--relatório-técnico)
18. [Cronograma Sugerido de Divisão de Tarefas](#18-cronograma-e-divisão)
19. [Bibliografia, Vídeos e Recursos de Estudo](#19-bibliografia-e-recursos)

---

## 1. Visão Geral do Projeto

O objetivo é construir um sistema P2P onde **múltiplas instâncias** (nós) rodando na mesma rede local mantêm uma **pasta compartilhada** (`shared_folder/`) sincronizada automaticamente. Quando qualquer nó cria, modifica ou deleta um arquivo, essa alteração deve se propagar para todos os demais nós.

### Requisitos do Enunciado (Checklist)
- [ ] Sincronização automática do conteúdo de uma pasta compartilhada
- [ ] Descoberta automática de novos nós na rede
- [ ] Protocolo de integração de novos dispositivos
- [ ] Mecanismo de propagação de alterações
- [ ] Detecção de modificações em arquivos
- [ ] Resolução de conflitos
- [ ] Sem uso de bibliotecas/frameworks P2P (apenas `socket`, `threading`, `json`, `hashlib`, `watchdog`)
- [ ] Docker Compose para simular múltiplos nós (ponto extra)
- [ ] Relatório técnico completo
- [ ] Instruções de compilação/execução

### Bibliotecas Permitidas
| Biblioteca | Uso | Permitida? |
|---|---|---|
| `socket` | Comunicação TCP/UDP | ✅ Sim (stdlib) |
| `threading` | Concorrência | ✅ Sim (stdlib) |
| `json` | Serialização de mensagens | ✅ Sim (stdlib) |
| `hashlib` | Hash SHA-256 de arquivos | ✅ Sim (stdlib) |
| `os`, `time`, `uuid`, `datetime` | Utilitários | ✅ Sim (stdlib) |
| `struct` | Framing TCP | ✅ Sim (stdlib) |
| `watchdog` | Monitoramento do filesystem | ✅ Sim (auxiliar/UI) |
| `sqlite3` | Banco de dados local | ✅ Sim (auxiliar) |

---

## 2. Arquitetura do Sistema

### Diagrama de Componentes

```
┌─────────────────────────────────────────────────────────┐
│                       NÓ (Peer)                         │
│                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────┐  │
│  │  Discovery   │    │  TCP Server  │    │  Watcher  │  │
│  │  (UDP Bcast) │    │  (Escuta)    │    │ (Watchdog)│  │
│  │  Thread #1   │    │  Thread #2   │    │ Thread #3 │  │
│  └──────┬───────┘    └──────┬───────┘    └─────┬─────┘  │
│         │                   │                  │        │
│         ▼                   ▼                  ▼        │
│  ┌─────────────────────────────────────────────────────┐│
│  │              Coordenador Central (main.py)          ││
│  │         Gerencia known_nodes, state_db, locks       ││
│  └──────────────────────┬──────────────────────────────┘│
│                         │                               │
│  ┌──────────┐   ┌───────┴──────┐   ┌────────────────┐   │
│  │ StateDB  │   │ Reconciler   │   │  File Manager  │   │
│  │ (JSON/   │   │ (LWW /       │   │  (Hash, R/W    │   │
│  │  SQLite) │   │  Conflitos)  │   │   Chunks)      │   │
│  └──────────┘   └──────────────┘   └────────────────┘   │
│                                                         │
│  ┌──────────────┐                                       │
│  │  TCP Client  │  ← Usado para enviar mensagens        │
│  │  (Ativo)     │    ativamente para outros nós         │
│  └──────────────┘                                       │
└─────────────────────────────────────────────────────────┘
```

### Fluxo Principal de Funcionamento

```
1. Nó inicia → dispara 3 threads (Discovery, TCPServer, Watcher)
2. Discovery envia UDP HELLO (broadcast) → outros nós respondem
3. Nó novo recebe lista de peers → inicia INDEX_EXCHANGE via TCP
4. Reconciler compara índices → identifica arquivos faltantes
5. Nó requisita arquivos faltantes → FILE_REQUEST via TCP
6. Nó remoto envia arquivo em chunks → FILE_TRANSFER via TCP
7. Watcher detecta alteração local → propaga FILE_NOTIFY para todos
8. Ciclo se repete continuamente
```

---

## 3. Estrutura de Pastas

> [!IMPORTANT]
> Cada arquivo tem uma responsabilidade clara. **Nenhum arquivo deve implementar lógica de outro módulo.** Isso facilita a divisão de trabalho entre os integrantes do grupo.

```
ProjectOutlook/
│
├── README.md                    # Instruções de execução (para o professor)
├── Dockerfile                   # Receita Docker do nó
├── docker-compose.yml           # Orquestra 3+ nós em rede virtual
├── requirements.txt             # Dependências pip (watchdog)
├── .gitignore                   # Ignora __pycache__, shared_folder, etc.
├── LICENSE                      # Licença do projeto
│
├── docs/                        # Documentação
│   ├── relatorio_tecnico.md     # Relatório final (entrega)
│   ├── protocolo.md             # Especificação formal do protocolo
│   └── cenarios_teste.md        # Cenários de teste documentados
│
├── src/                         # Código-fonte
│   ├── main.py                  # Ponto de entrada — orquestra todas as threads
│   ├── config.py                # Constantes: portas, caminhos, IDs
│   │
│   ├── network/                 # Camada de rede (UDP + TCP)
│   │   ├── __init__.py
│   │   ├── protocol.py          # Tipos de mensagens + funções build/parse
│   │   ├── discovery.py         # Descoberta UDP (broadcast + listener)
│   │   ├── tcp_server.py        # Servidor TCP (aceita conexões, roteia msgs)
│   │   └── tcp_client.py        # Cliente TCP (envia mensagens ativamente)
│   │
│   ├── sync/                    # Camada de sincronização
│   │   ├── __init__.py
│   │   ├── file_manager.py      # Hash SHA-256, leitura/escrita em chunks
│   │   ├── state_db.py          # Banco de estado local (JSON ou SQLite)
│   │   ├── watcher.py           # Watchdog: monitora criação/edição/deleção
│   │   └── reconciler.py        # Lógica de reconciliação e conflitos (LWW)
│   │
│   └── ui/                      # Interface de saída
│       ├── __init__.py
│       └── cli.py               # Logging formatado com timestamp e cores
│
└── shared_folder/               # Pasta sincronizada (criada em runtime)
```

### Responsabilidade de Cada Arquivo

| Arquivo | Responsabilidade | Quem pode implementar |
|---|---|---|
| `config.py` | Centraliza TODAS as constantes (portas, caminhos, UUID do nó) | PRONTO |
| `protocol.py` | Define os tipos de mensagem, funções `build_message()` e `parse_message()`, e o **framing TCP** | PRONTO |
| `discovery.py` | Broadcast UDP, escuta de peers, manutenção da lista `known_nodes`, heartbeat periódico | PRONTO |
| `tcp_server.py` | Escuta na porta TCP, aceita conexões, faz parse da mensagem e **roteia** para o handler correto | PRONTO |
| `tcp_client.py` | Conecta a um peer via TCP e envia uma mensagem (com framing) | PRONTO |
| `watcher.py` | Usa `watchdog` para detectar criação, modificação e deleção de arquivos na `shared_folder` | Integrante C |
| `file_manager.py` | Calcula hash SHA-256, lê arquivos em chunks, escreve arquivos recebidos | PRONTO |
| `state_db.py` | Persiste e consulta o estado de cada arquivo (nome, hash, timestamp, status) | PRONTO |
| `reconciler.py` | Compara índices local vs. remoto, decide quem está desatualizado (LWW) | PRONTO |
| `main.py` | Instancia e conecta TODOS os componentes, gerencia threads e o loop principal | Todos juntos |
| `cli.py` | Funções `log_info`, `log_error`, `log_warn` com formatação bonita | PRONTO |

---

## 4. Protocolo de Comunicação

> [!IMPORTANT]
> Este é o **coração** do projeto. O professor avaliará a qualidade do protocolo. Documente-o em `docs/protocolo.md`.

### 4.1. Camada de Transporte

| Funcionalidade | Protocolo | Justificativa |
|---|---|---|
| Descoberta de peers | **UDP** (broadcast) | Não precisa de conexão prévia; alcança toda a rede |
| Troca de índices | **TCP** | Confiável, garante entrega |
| Transferência de arquivos | **TCP** | Confiável, garante ordem e integridade |
| Notificações de alteração | **TCP** | Confiável |

### 4.2. Formato das Mensagens

Todas as mensagens são **JSON**, serializadas em UTF-8. Cada mensagem TCP é precedida por um **header de 4 bytes** indicando o tamanho do payload (framing com `struct`).

```
┌────────────┬──────────────────────┐
│ 4 bytes    │ N bytes              │
│ (tamanho)  │ (JSON payload)       │
└────────────┴──────────────────────┘
```

### 4.3. Tipos de Mensagens

#### 4.3.1. Descoberta (UDP)

**HELLO** — Anúncio de presença
```json
{
  "type": "HELLO",
  "node_id": "uuid-do-nó",
  "name": "Node1",
  "tcp_port": 5001,
  "version": "1.0"
}
```

**HELLO_ACK** — Resposta ao HELLO (pode ser via UDP ou TCP)
```json
{
  "type": "HELLO_ACK",
  "node_id": "uuid-do-nó-respondente",
  "name": "Node2",
  "tcp_port": 5001
}
```

#### 4.3.2. Sincronização (TCP)

**INDEX_EXCHANGE** — Troca de índice de arquivos
```json
{
  "type": "INDEX_EXCHANGE",
  "node_id": "uuid",
  "files": {
    "foto.jpg": {"hash": "abc123...", "timestamp": 1718600000.0, "size": 204800, "status": "ACTIVE"},
    "doc.pdf":  {"hash": "def456...", "timestamp": 1718590000.0, "size": 1048576, "status": "ACTIVE"},
    "old.txt":  {"hash": "ghi789...", "timestamp": 1718580000.0, "size": 512, "status": "DELETED"}
  }
}
```

**FILE_REQUEST** — Solicita download de um arquivo
```json
{
  "type": "FILE_REQUEST",
  "node_id": "uuid-solicitante",
  "filename": "foto.jpg"
}
```

**FILE_TRANSFER_START** — Início de transferência
```json
{
  "type": "FILE_TRANSFER_START",
  "filename": "foto.jpg",
  "size": 204800,
  "hash": "abc123...",
  "total_chunks": 50
}
```

**FILE_CHUNK** — Pedaço do arquivo (binário encodado em Base64 para JSON)
```json
{
  "type": "FILE_CHUNK",
  "filename": "foto.jpg",
  "chunk_index": 0,
  "data": "<base64-encoded-bytes>",
  "is_last": false
}
```

> [!TIP]
> **Alternativa mais eficiente:** Em vez de usar Base64 dentro de JSON para os chunks, vocês podem implementar um **protocolo binário** para a transferência de arquivos. Envie o header JSON com metadados e depois os bytes puros. Isso é mais eficiente, mas mais complexo de implementar. A escolha de Base64 é mais simples e perfeitamente aceitável para o trabalho.

**FILE_TRANSFER_COMPLETE** — Confirmação de conclusão
```json
{
  "type": "FILE_TRANSFER_COMPLETE",
  "filename": "foto.jpg",
  "hash": "abc123..."
}
```

#### 4.3.3. Notificações (TCP)

**FILE_NOTIFY** — Avisa que um arquivo foi criado/modificado
```json
{
  "type": "FILE_NOTIFY",
  "node_id": "uuid",
  "action": "CREATED",
  "filename": "novo_arquivo.txt",
  "hash": "xyz...",
  "timestamp": 1718600500.0,
  "size": 1024
}
```
- `action` pode ser: `"CREATED"`, `"MODIFIED"`, `"DELETED"`

**DELETE_NOTIFY** — Avisa que um arquivo foi deletado
```json
{
  "type": "DELETE_NOTIFY",
  "node_id": "uuid",
  "filename": "arquivo_velho.txt",
  "timestamp": 1718601000.0
}
```

#### 4.3.4. Controle (TCP)

**HEARTBEAT** — Sinal periódico de vida
```json
{
  "type": "HEARTBEAT",
  "node_id": "uuid",
  "timestamp": 1718601500.0
}
```

**HEARTBEAT_ACK** — Resposta ao heartbeat
```json
{
  "type": "HEARTBEAT_ACK",
  "node_id": "uuid"
}
```

**NODE_LEAVING** — Aviso gracioso de desconexão
```json
{
  "type": "NODE_LEAVING",
  "node_id": "uuid"
}
```

---

## 5. Etapa 1 — Configuração e Infraestrutura Base

### O que fazer
1. **Ajustar `config.py`** para centralizar todas as constantes
2. **Criar `requirements.txt`** com `watchdog`
3. **Implementar `cli.py`** com logs coloridos

### Passo a passo — `config.py`
O arquivo deve conter:
- `UDP_PORT` = 5000 (porta de descoberta)
- `TCP_PORT` = 5001 (porta de comunicação TCP — deve ser configurável via variável de ambiente para Docker)
- `BROADCAST_IP` = `'255.255.255.255'` (ou `'<broadcast>'`)
- `NODE_ID` = UUID gerado com `uuid.uuid4()`
- `NODE_NAME` = lido de `os.environ.get("NODE_NAME")` ou gerado automaticamente
- `SHARED_FOLDER` = caminho absoluto para `shared_folder/`
- `DB_PATH` = caminho para o arquivo de estado (`state.json` ou `state.sqlite3`)
- `CHUNK_SIZE` = 4096 (tamanho dos pedaços para leitura de arquivo)
- `HEARTBEAT_INTERVAL` = 15 (segundos entre heartbeats)
- `NODE_TIMEOUT` = 45 (segundos sem heartbeat para considerar nó morto)
- `DISCOVERY_INTERVAL` = 30 (segundos entre broadcasts UDP)

### Passo a passo — `cli.py`
Implementar funções de log com:
- Timestamp formatado `[HH:MM:SS]`
- Nome do nó `[Node1]`
- Tipo de log com cores ANSI: `INFO` (verde), `ERROR` (vermelho), `WARN` (amarelo), `SYNC` (ciano)
- Usar códigos ANSI: `\033[92m` (verde), `\033[91m` (vermelho), `\033[93m` (amarelo), `\033[96m` (ciano), `\033[0m` (reset)

### Passo a passo — `requirements.txt`
Criar o arquivo contendo apenas:
```
watchdog
```

---

## 6. Etapa 2 — Descoberta de Nós (UDP Broadcast)

> [!NOTE]
> **Conceito:** Cada nó "grita" periodicamente na rede: "Eu existo, meu IP é X, minha porta TCP é Y". Os outros nós escutam e registram.

### O que implementar em `discovery.py`

1. **Classe `DiscoveryManager`** com atributo `known_nodes` (dicionário thread-safe)
2. **Método `start()`** que inicia 2 sub-threads:
   - `_listen_for_broadcasts()` — escuta UDP na porta 5000
   - `_broadcast_loop()` — envia HELLO a cada `DISCOVERY_INTERVAL` segundos

3. **`_listen_for_broadcasts()`**
   - Criar socket UDP: `socket.socket(AF_INET, SOCK_DGRAM)`
   - Setar `SO_REUSEADDR` e `SO_REUSEPORT` (para múltiplos nós na mesma máquina)
   - Fazer `bind(('', UDP_PORT))`
   - Loop infinito: `recvfrom(1024)` → decodificar JSON → se `node_id != meu_id`, registrar em `known_nodes`
   - Ao detectar novo nó: logar e disparar sincronização inicial (callback ou evento)

4. **`_broadcast_presence()`**
   - Criar socket UDP
   - Setar `SO_BROADCAST = 1`
   - Montar mensagem HELLO com `protocol.build_message()`
   - Enviar para `(BROADCAST_IP, UDP_PORT)`
   - Fechar socket

5. **`_broadcast_loop()`**
   - Loop: `_broadcast_presence()` → `time.sleep(DISCOVERY_INTERVAL)`

6. **`get_active_nodes()`** — retorna cópia do dicionário `known_nodes`

7. **`remove_node(node_id)`** — remove nó que falhou

### Detalhes Importantes
- Use `threading.Lock()` para proteger o acesso a `known_nodes` (thread-safety!)
- Ao detectar novo nó, a thread principal deve ser notificada para iniciar sincronização. Use `threading.Event` ou um callback.

### Como testar isoladamente
- Rodar 2 instâncias do `discovery.py` em terminais separados (use portas diferentes ou Docker)
- Verificar se ambas detectam uma à outra

---

## 7. Etapa 3 — Comunicação TCP

### O que implementar

### 7.1. Framing TCP — O Problema e a Solução

> [!WARNING]
> **TCP é um protocolo de STREAM**, não de mensagens. Se você enviar "HELLO" seguido de "FILE_REQUEST", o receptor pode receber "HELLOFILE_REQUEST" de uma vez. Você precisa implementar **framing** (delimitação de mensagens).

**Solução: Length-Prefix Framing**
```
Envio:
  1. Calcular tamanho do JSON em bytes
  2. Enviar 4 bytes com o tamanho (struct.pack('>I', tamanho))
  3. Enviar os bytes do JSON

Recebimento:
  1. Ler exatamente 4 bytes (struct.unpack('>I', ...))
  2. Ler exatamente N bytes (o tamanho obtido)
  3. Decodificar JSON
```

Implementar funções auxiliares em `protocol.py`:
- `send_message(sock, msg_dict)` — serializa, calcula tamanho, envia com framing
- `recv_message(sock)` — lê o header de 4 bytes, depois lê N bytes, desserializa
- `recv_exact(sock, n)` — lê exatamente N bytes (loop até ter tudo)

### 7.2. `tcp_server.py`

1. **Classe `TCPServer`** que recebe referências para `state_db`, `file_manager`, `reconciler`, `discovery`
2. **`start()`** — cria socket TCP, `bind('0.0.0.0', TCP_PORT)`, `listen(5)`, loop de `accept()`
3. **`_handle_client(sock, addr)`** — em thread separada:
   - Usa `protocol.recv_message()` para ler a mensagem
   - Switch no `type` da mensagem:
     - `INDEX_EXCHANGE` → chama `_handle_index_exchange()`
     - `FILE_REQUEST` → chama `_handle_file_request()`
     - `FILE_NOTIFY` → chama `_handle_file_notify()`
     - `DELETE_NOTIFY` → chama `_handle_delete_notify()`
     - `HEARTBEAT` → responde com `HEARTBEAT_ACK`
   - **Manter conexão aberta** durante a conversa (não fechar após 1 mensagem)
4. **`_handle_index_exchange(msg, sock)`**:
   - Receber índice remoto
   - Chamar `reconciler.compare_indices(local_index, remote_index)`
   - Para cada arquivo que precisamos: enviar `FILE_REQUEST`
   - Para cada arquivo que o remoto precisa: aguardar `FILE_REQUEST` dele
5. **`_handle_file_request(msg, sock)`**:
   - Ler o arquivo em chunks com `file_manager`
   - Enviar `FILE_TRANSFER_START`
   - Enviar cada `FILE_CHUNK`
   - Enviar `FILE_TRANSFER_COMPLETE`

### 7.3. `tcp_client.py`

1. **Classe `TCPClient`** (pode ser estático ou instância)
2. **`send_message(ip, port, msg_dict)`** — conecta, envia (com framing), fecha
3. **`send_and_receive(ip, port, msg_dict)`** — conecta, envia, espera resposta, fecha
4. **`request_file(ip, port, filename, save_path)`** — envia `FILE_REQUEST`, recebe chunks, salva
5. **`send_index(ip, port, local_index)`** — envia `INDEX_EXCHANGE`

---

## 8. Etapa 4 — Monitoramento de Arquivos

### O que implementar em `watcher.py`

Usar a biblioteca `watchdog` para monitorar a `shared_folder`:

1. **Criar classe `SyncEventHandler`** que herda de `FileSystemEventHandler`
2. **Sobrescrever métodos:**
   - `on_created(event)` — arquivo novo → calcular hash → atualizar `state_db` → notificar peers
   - `on_modified(event)` — arquivo editado → recalcular hash → atualizar `state_db` → notificar peers
   - `on_deleted(event)` — arquivo deletado → marcar como DELETED no `state_db` → notificar peers
   - `on_moved(event)` — tratar como deleção + criação
3. **Ignorar eventos de diretório** (verificar `event.is_directory`)
4. **Criar classe `DirectoryWatcher`** que inicializa o `Observer` do watchdog
5. **Debouncing (MUITO IMPORTANTE):**
   - O watchdog pode disparar MÚLTIPLOS eventos para uma única alteração
   - Implementar um timer/debounce: ao receber evento, esperar 0.5s antes de processar. Se receber outro evento para o mesmo arquivo nesse período, reiniciar o timer.
   - Usar um dicionário de `pending_events` com timestamps

6. **Flag `is_syncing`:**
   - Quando o nó está recebendo um arquivo de outro nó, ele vai ESCREVER na `shared_folder`
   - O watchdog vai detectar essa escrita como uma alteração LOCAL
   - Isso criaria um **loop infinito** de sincronização!
   - **Solução:** Manter um `set()` de `files_being_synced`. Antes de escrever um arquivo recebido, adicionar o nome ao set. No handler do watchdog, ignorar eventos para arquivos que estão no set. Após terminar de escrever, remover do set (após um breve delay).

### Como testar isoladamente
- Rodar o watcher apontando para uma pasta
- Em outro terminal, criar/editar/deletar arquivos nessa pasta
- Verificar se os eventos são capturados e logados corretamente

---

## 9. Etapa 5 — Banco de Estado Local

### O que implementar em `state_db.py`

O banco de estado é a "fonte da verdade" de cada nó sobre quais arquivos ele possui.

### Opção A: JSON (mais simples)
```
Estrutura do state.json:
{
  "foto.jpg": {
    "hash": "abc123...",
    "timestamp": 1718600000.0,
    "size": 204800,
    "status": "ACTIVE"
  },
  "old.txt": {
    "hash": "ghi789...",
    "timestamp": 1718580000.0,
    "size": 512,
    "status": "DELETED"
  }
}
```

### Opção B: SQLite (mais robusto — recomendado)
```sql
CREATE TABLE file_state (
    filename TEXT PRIMARY KEY,
    hash TEXT NOT NULL,
    timestamp REAL NOT NULL,
    size INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
);
```

### Métodos da Classe `StateDB`
1. `get_full_index()` → retorna dicionário completo (para enviar em `INDEX_EXCHANGE`)
2. `get_file_state(filename)` → retorna dados de um arquivo específico
3. `update_file_state(filename, hash, timestamp, size, status='ACTIVE')`
4. `mark_deleted(filename, timestamp)`
5. `file_exists(filename)` → verifica se arquivo existe e está ACTIVE
6. `get_active_files()` → retorna apenas arquivos com status ACTIVE

### Detalhes Importantes
- Use `threading.Lock()` para proteger acesso ao banco (múltiplas threads vão ler/escrever)
- O campo `status` é crucial: quando um arquivo é deletado, NÃO remova do banco. Marque como `"DELETED"`. Isso é necessário para propagar deleções para outros nós.
- O `timestamp` deve usar `time.time()` (epoch float, segundos desde 1970)

---

## 10. Etapa 6 — Troca de Índices e Reconciliação

### Quando acontece a troca de índices?
1. **Quando um novo nó é descoberto** — automaticamente após receber HELLO
2. **Periodicamente** — a cada X minutos, como "checkpoint" de segurança
3. **Quando um nó reconecta** — após ter ficado offline

### O que implementar em `reconciler.py`

1. **`compare_indices(local_index, remote_index)`** — retorna 3 listas:
   - `files_to_download` — arquivos que o remoto tem e eu não (ou remoto é mais novo)
   - `files_to_upload` — arquivos que eu tenho e o remoto não (ou eu sou mais novo)
   - `files_to_delete` — arquivos que o remoto deletou e eu ainda tenho

2. **Lógica de comparação (para cada arquivo):**
```
Para cada filename no REMOTO:
    Se eu NÃO tenho esse arquivo:
        Se status remoto == ACTIVE → adicionar a files_to_download
    Se eu TENHO esse arquivo:
        Se hash igual → nada a fazer (EQUAL)
        Se hash diferente:
            Se timestamp remoto > timestamp local → DOWNLOAD (remoto é mais novo)
            Se timestamp local > timestamp remoto → UPLOAD (eu sou mais novo)
            Se timestamps iguais → conflito real → LWW com desempate por node_id
        Se remoto tem status DELETED e eu tenho ACTIVE:
            Se timestamp da deleção > meu timestamp → deletar localmente

Para cada filename LOCAL que NÃO está no remoto:
    Adicionar a files_to_upload (eu tenho algo que o remoto não conhece)
```

3. **`resolve_conflict(local_state, remote_state)`** — decide entre LOCAL e REMOTE usando LWW (Last Write Wins)

### Estratégia LWW (Last Write Wins)
- Compare os timestamps: **maior timestamp vence**
- Em caso de empate de timestamp: **maior node_id (string comparison) vence** — é determinístico
- Essa é a estratégia mais simples e perfeitamente aceitável para o trabalho

---

## 11. Etapa 7 — Transferência de Arquivos

### O que implementar em `file_manager.py`

1. **`get_file_hash(filepath)`** — calcula SHA-256 lendo em chunks de 4096 bytes
2. **`read_file_chunks(filepath, chunk_size=4096)`** — generator que yield chunks
3. **`save_file_from_chunks(filepath, chunks_list)`** — escreve chunks em arquivo
4. **`get_file_size(filepath)`** — retorna tamanho em bytes
5. **`scan_directory(folder_path)`** — varre toda a pasta e retorna dicionário com hash/timestamp/size de cada arquivo
6. **`delete_file(filepath)`** — deleta arquivo do disco

### Fluxo de Transferência Completo

```
Nó A (precisa do arquivo)                  Nó B (tem o arquivo)
        │                                         │
        │──── FILE_REQUEST("foto.jpg") ──────────→│
        │                                         │
        │←── FILE_TRANSFER_START ─────────────────│
        │    (filename, size, hash, total_chunks)  │
        │                                         │
        │←── FILE_CHUNK(index=0, data=...) ───────│
        │←── FILE_CHUNK(index=1, data=...) ───────│
        │←── FILE_CHUNK(index=2, data=..., last)──│
        │                                         │
        │  [Nó A remonta arquivo, verifica hash]   │
        │                                         │
        │──── FILE_TRANSFER_COMPLETE ────────────→│
        │    (filename, hash_ok=true)              │
```

### Detalhes do Encoding dos Chunks
- Ler chunk como bytes do arquivo
- Converter para Base64 string: `base64.b64encode(chunk).decode('ascii')`
- No lado receptor: `base64.b64decode(data_string)`
- Após receber todos os chunks, **verificar o hash** do arquivo remontado contra o hash esperado. Se não bater, descartar e re-solicitar.

---

## 12. Etapa 8 — Propagação de Alterações e Deleções

### Fluxo de Propagação

```
Evento local (watchdog detecta) →
  Atualizar state_db →
    Para cada nó em known_nodes:
      Enviar FILE_NOTIFY ou DELETE_NOTIFY via TCP
```

### Criação/Modificação de Arquivo
1. Watcher detecta `on_created` ou `on_modified`
2. Calcular hash do arquivo novo
3. Atualizar `state_db`
4. Para cada nó conhecido: enviar `FILE_NOTIFY` com action=CREATED ou MODIFIED
5. O nó receptor recebe `FILE_NOTIFY` → compara com seu estado local → se precisa, envia `FILE_REQUEST`

### Deleção de Arquivo
1. Watcher detecta `on_deleted`
2. Marcar como DELETED no `state_db` (com timestamp da deleção)
3. Para cada nó conhecido: enviar `DELETE_NOTIFY`
4. O nó receptor recebe `DELETE_NOTIFY`:
   - Verifica se o timestamp da deleção > timestamp local do arquivo
   - Se sim: deleta o arquivo localmente e marca como DELETED no `state_db`
   - Se não: ignora (a versão local é mais nova que a deleção)

> [!WARNING]
> **Cuidado com tombstones:** Ao deletar um arquivo, NÃO remova a entrada do `state_db`. Mantenha com status DELETED. Se você remover, quando o nó receber um `INDEX_EXCHANGE`, vai pensar que não conhece o arquivo e vai baixá-lo de novo — desfazendo a deleção!

---

## 13. Etapa 9 — Tolerância a Falhas

### O que implementar

1. **Heartbeat periódico:**
   - A cada `HEARTBEAT_INTERVAL` segundos, enviar `HEARTBEAT` via TCP para todos os `known_nodes`
   - Se o envio falhar (connection refused, timeout): incrementar um contador de falhas
   - Se `falhas >= 3`: marcar nó como offline e remover de `known_nodes`
   - Se receber `HEARTBEAT`: responder com `HEARTBEAT_ACK`

2. **Timeout de conexões TCP:**
   - Configurar `socket.settimeout(10)` em todas as conexões TCP
   - Tratar `socket.timeout` como falha temporária

3. **Reconexão automática:**
   - O módulo de discovery continua enviando broadcasts periodicamente
   - Se um nó que estava offline voltar, será re-descoberto automaticamente
   - Ao re-descobrir, iniciar `INDEX_EXCHANGE` para sincronizar o que foi perdido

4. **Saída graciosa:**
   - Ao pressionar Ctrl+C, enviar `NODE_LEAVING` para todos os peers antes de encerrar
   - Usar `signal.signal(SIGINT, handler)` ou `try/except KeyboardInterrupt`

5. **Retry com backoff:**
   - Se envio de mensagem TCP falhar, tentar novamente após 2s, 4s, 8s (exponential backoff)
   - Máximo de 3 tentativas

6. **Verificação de integridade:**
   - Após receber arquivo completo, verificar hash
   - Se hash não bater: descartar e re-solicitar

### Como testar
- Rodar 3 nós → colocar arquivo no nó 1 → verificar que nó 2 e 3 receberam
- Derrubar nó 2 (Ctrl+C ou `docker stop`)
- Colocar novo arquivo no nó 1 → verificar que nó 3 recebeu
- Religar nó 2 → verificar que ele sincroniza o que perdeu

---

## 14. Etapa 10 — Orquestração no `main.py`

### O que o `main.py` deve fazer

```
1. Carregar configurações (config.py)
2. Inicializar componentes:
   - state_db = StateDB()
   - file_manager = FileManager()
   - reconciler = Reconciler()
   - discovery = DiscoveryManager(on_new_node=sync_with_node)
   - tcp_server = TCPServer(state_db, file_manager, reconciler, discovery)
   - watcher = DirectoryWatcher(state_db, file_manager, on_change=notify_peers)

3. Fazer scan inicial da shared_folder → popular state_db

4. Iniciar threads:
   - Thread: discovery.start()
   - Thread: tcp_server.start()
   - Thread: watcher.start()
   - Thread: heartbeat_loop()
   - Thread: periodic_sync_loop() (a cada 5 min, faz INDEX_EXCHANGE com todos)

5. Loop principal:
   - Manter o processo vivo
   - Opcionalmente: interface CLI para comandos manuais

6. Shutdown gracioso:
   - Enviar NODE_LEAVING para todos
   - Parar threads
```

### Injeção de Dependências
O `main.py` é o "cola" que conecta tudo. Ele deve:
- Criar instâncias de todos os componentes
- Passar referências entre eles (ex: `tcp_server` precisa de `state_db` e `file_manager`)
- Definir os callbacks (ex: `on_new_node_discovered` → iniciar sincronização)

### Compartilhamento de Estado entre Threads
- `known_nodes`: protegido com `Lock` dentro de `DiscoveryManager`
- `state_db`: protegido com `Lock` dentro de `StateDB`
- `files_being_synced`: `set()` protegido com `Lock`, compartilhado entre `watcher` e `tcp_server`

---

## 15. Etapa 11 — Docker

> [!TIP]
> Usar Docker rende **pontos extras** e facilita MUITO a demonstração. É altamente recomendado.

### Dockerfile
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
RUN mkdir -p /app/shared_folder
CMD ["python", "-u", "src/main.py"]
```
- O flag `-u` desabilita buffering do stdout (importante para ver logs em tempo real)

### docker-compose.yml
```yaml
version: '3.8'

services:
  node1:
    build: .
    volumes:
      - ./node1_data:/app/shared_folder
    environment:
      - NODE_NAME=Node1
      - TCP_PORT=5001
    networks:
      - sync_net

  node2:
    build: .
    volumes:
      - ./node2_data:/app/shared_folder
    environment:
      - NODE_NAME=Node2
      - TCP_PORT=5001
    networks:
      - sync_net

  node3:
    build: .
    volumes:
      - ./node3_data:/app/shared_folder
    environment:
      - NODE_NAME=Node3
      - TCP_PORT=5001
    networks:
      - sync_net

networks:
  sync_net:
    driver: bridge
```

### Comandos úteis
```bash
# Subir todos os nós
docker-compose up --build

# Subir em background
docker-compose up --build -d

# Ver logs de um nó específico
docker-compose logs -f node1

# Parar um nó (simular falha)
docker stop projectoutlook-node2-1

# Reiniciar nó (simular reconexão)
docker start projectoutlook-node2-1

# Copiar arquivo para dentro do container
docker cp foto.jpg projectoutlook-node1-1:/app/shared_folder/

# Derrubar tudo
docker-compose down
```

### Problema: Broadcast UDP no Docker
> [!WARNING]
> O broadcast `255.255.255.255` pode **não funcionar** entre containers Docker. Alternativas:
> 1. Usar **broadcast da subnet** do Docker (ex: `172.18.255.255`)
> 2. Usar **multicast** (endereço tipo `224.0.0.1`)
> 3. Passar lista de IPs dos outros nós via variável de ambiente e conectar diretamente via TCP
> 4. Usar DNS do Docker: os containers podem se encontrar pelo nome (`node1`, `node2`, `node3`)

**Solução recomendada:** Adicionar suporte a **"seed nodes"** via variável de ambiente. Se a variável `SEED_NODES` estiver definida, conectar diretamente via TCP em vez de depender de broadcast.

```yaml
environment:
  - SEED_NODES=node1:5001,node2:5001,node3:5001
```

---

## 16. Etapa 12 — Testes e Cenários

> [!IMPORTANT]
> Documente TODOS os testes em `docs/cenarios_teste.md` com screenshots.

### Cenário 1 — Sincronização Básica
1. Subir 3 nós
2. Copiar `foto.jpg` para `node1_data/`
3. **Resultado esperado:** `foto.jpg` aparece em `node2_data/` e `node3_data/`

### Cenário 2 — Ingresso de Novo Nó
1. Subir node1 e node2 com arquivos
2. Subir node3 (vazio)
3. **Resultado esperado:** node3 sincroniza todos os arquivos existentes

### Cenário 3 — Modificação de Arquivo
1. Todos os nós sincronizados com `doc.txt`
2. Editar `doc.txt` no node1
3. **Resultado esperado:** versão atualizada propaga para node2 e node3

### Cenário 4 — Deleção de Arquivo
1. Todos sincronizados com `old.txt`
2. Deletar `old.txt` no node1
3. **Resultado esperado:** `old.txt` é removido de node2 e node3

### Cenário 5 — Tolerância a Falhas
1. Todos sincronizados
2. Derrubar node2 (`docker stop`)
3. Adicionar `novo.pdf` no node1
4. Verificar que node3 recebeu
5. Religar node2 (`docker start`)
6. **Resultado esperado:** node2 sincroniza `novo.pdf` ao reconectar

### Cenário 6 — Arquivos Grandes
1. Copiar um PDF de 50MB+ para node1
2. **Resultado esperado:** arquivo transferido em chunks para node2 e node3

### Cenário 7 — Múltiplos Tipos de Arquivo
1. Copiar PDF, imagem JPG, vídeo MP4 e arquivo TXT
2. **Resultado esperado:** todos os tipos sincronizam corretamente

### Cenário 8 — Conflito Simultâneo
1. Editar `config.txt` no node1 e no node2 ao mesmo tempo (ou quase)
2. **Resultado esperado:** LWW resolve — versão mais recente vence em todos os nós

---

## 17. Etapa 13 — Relatório Técnico

### Estrutura do Relatório (para `docs/relatorio_tecnico.md` ou PDF)

```
1. Capa
   - Nome da disciplina, professor, integrantes, data

2. Introdução
   - Objetivo do trabalho
   - Opção escolhida (Opção 2)
   - Visão geral da solução

3. Arquitetura do Sistema
   - Diagrama de componentes (o diagrama da seção 2 deste guia)
   - Descrição de cada módulo
   - Decisões arquiteturais e justificativas

4. Protocolo de Comunicação
   - Descrição detalhada de cada mensagem
   - Formato e encoding
   - Diagramas de sequência dos fluxos principais
   - Justificativa da escolha UDP vs TCP para cada funcionalidade

5. Mecanismos Implementados
   5.1. Descoberta de nós
   5.2. Sincronização de índices
   5.3. Transferência de arquivos
   5.4. Detecção de alterações (watchdog)
   5.5. Resolução de conflitos (LWW)
   5.6. Propagação de deleções
   5.7. Tolerância a falhas (heartbeat, reconexão)

6. Decisões de Projeto
   - Por que Python?
   - Por que JSON para mensagens?
   - Por que LWW para conflitos?
   - Por que Base64 para chunks?
   - Trade-offs e limitações conhecidas

7. Instruções de Execução
   - Sem Docker (pip install + python)
   - Com Docker (docker-compose up)
   - Passo a passo de cada cenário de teste

8. Exemplos de Execução (com screenshots/logs)
   - Prints do terminal mostrando cada cenário
   - Captura da sincronização funcionando

9. Dificuldades Encontradas
   - Problemas com broadcast no Docker
   - Bugs de concorrência
   - Debouncing do watchdog
   - etc.

10. Conclusão
    - O que foi aprendido
    - Possíveis melhorias futuras

11. Referências Bibliográficas
```

---

## 18. Cronograma e Divisão

### Sugestão de Divisão para 4 Integrantes

| Fase | Integrante A | Integrante B | Integrante C | Integrante D |
|---|---|---|---|---|
| **Semana 1** | `protocol.py` + `discovery.py` | `tcp_server.py` + `tcp_client.py` | `watcher.py` + `file_manager.py` | `state_db.py` + `reconciler.py` |
| **Semana 2** | Integrar discovery ↔ TCP | Implementar handlers TCP | Integrar watcher ↔ state_db | Integrar reconciler ↔ TCP |
| **Semana 3** | Todos: integrar no `main.py`, testar end-to-end, Docker |
| **Semana 4** | Tolerância a falhas | Docker + cenários | Relatório técnico | Testes finais |

### Ordem de Implementação (Bottom-Up)

```
1º → config.py + cli.py (base)
2º → protocol.py (framing + tipos de mensagem)
3º → state_db.py (banco de estado)
4º → file_manager.py (hash + chunks)
5º → discovery.py (UDP broadcast)
6º → tcp_server.py + tcp_client.py (comunicação TCP)
7º → watcher.py (monitoramento com watchdog)
8º → reconciler.py (lógica de merge)
9º → main.py (orquestração)
10º → Docker + docker-compose
11º → Testes de cenários
12º → Relatório
```

---

## 19. Bibliografia e Recursos

### 📚 Documentação Oficial
| Recurso | Link |
|---|---|
| Python `socket` — HOWTO | https://docs.python.org/3/howto/sockets.html |
| Python `socket` — Referência | https://docs.python.org/3/library/socket.html |
| Python `threading` | https://docs.python.org/3/library/threading.html |
| Python `struct` | https://docs.python.org/3/library/struct.html |
| Python `hashlib` | https://docs.python.org/3/library/hashlib.html |
| Python `json` | https://docs.python.org/3/library/json.html |
| Watchdog — Documentação | https://python-watchdog.readthedocs.io/en/stable/ |
| Docker Compose — Networking | https://docs.docker.com/compose/networking/ |
| Docker — Referência Dockerfile | https://docs.docker.com/reference/dockerfile/ |

### 🎥 Vídeos Recomendados (YouTube)

#### Sockets TCP/UDP em Python
| Vídeo | Idioma | O que cobre |
|---|---|---|
| **"Socket Programming in Python"** — Tech With Tim | EN | Cliente-servidor TCP básico, transferência de dados |
| **"Python Socket Programming Tutorial"** — NeuralNine | EN | TCP + UDP, chat, múltiplos clientes |
| **"Transferência de Arquivos com Sockets em Python"** — The Python Code | EN | Envio de arquivos grandes em chunks via TCP |
| **"Python Network Programming"** — sentdex | EN | Série completa de programação de redes |
| **"Programação de Sockets Python TCP"** — Simplificando Redes | PT-BR | Tutorial TCP/UDP em português |
| **"UDP Peer-to-Peer Messaging with Python"** — Engineer Man | EN | Comunicação P2P com UDP |

> **Pesquise no YouTube por:** `"Python socket file transfer tutorial"`, `"Python UDP broadcast peer discovery"`, `"Python watchdog tutorial"`, `"Docker Compose networking tutorial"`

#### Sistemas Distribuídos (Conceituais)
| Vídeo | Idioma | O que cobre |
|---|---|---|
| **"Distributed Systems lecture series"** — Martin Kleppmann (Cambridge) | EN | Fundamentos teóricos completos |
| **"How Dropbox Works"** — vários canais | EN | Arquitetura de sincronização de arquivos |
| **"Last Write Wins (LWW) Explained"** — pesquisar no YouTube | EN | Estratégia de resolução de conflitos |

### 📖 Artigos e Tutoriais
| Recurso | Link |
|---|---|
| Real Python — Socket Programming | https://realpython.com/python-sockets/ |
| GeeksForGeeks — File Transfer TCP Python | https://www.geeksforgeeks.org/simple-chat-room-using-python/ |
| PythonClub — Upload de Arquivos com Socket | https://pythonclub.com.br/ |
| Digital Ocean — Python Socket Tutorial | https://www.digitalocean.com/community/tutorials/python-socket-programming-server-client |
| Dev.to — P2P File Sharing in Python | Buscar: "P2P file sharing Python sockets" |

### 📖 Livros de Referência
| Livro | Autor | Tópico |
|---|---|---|
| **"Distributed Systems: Principles and Paradigms"** | Tanenbaum & Van Steen | Fundamentos de sistemas distribuídos |
| **"Designing Data-Intensive Applications"** | Martin Kleppmann | Replicação, consistência, conflitos |
| **"Computer Networking: A Top-Down Approach"** | Kurose & Ross | TCP/UDP, protocolos de rede |
| **"Python Network Programming Cookbook"** | Dr. M. O. Faruque Sarker | Receitas práticas de rede em Python |

### 🔗 Repositórios de Referência (para estudo, NÃO copiar)
| Repositório | O que estudar |
|---|---|
| github.com/Cyb3rba3/LAN_P2P | Estrutura de P2P em rede local |
| github.com/MaddyDev-glitch/Distributed-Computing-using-Python-Sockets | Comunicação distribuída com sockets |
| Buscar: "python p2p file sync" no GitHub | Vários exemplos para estudar padrões |

> [!CAUTION]
> **IMPORTANTE:** Esses repositórios são apenas para **estudo de padrões**. NÃO copie código. O professor exigiu que todos os integrantes saibam explicar cada parte da solução.

---

## Resumo dos Conceitos-Chave para a Apresentação

Certifique-se de que **todos os integrantes** saibam explicar:

1. **Por que usamos UDP para descoberta e TCP para transferência?**
   - UDP é connectionless, ideal para broadcast. TCP garante entrega e ordem.

2. **O que é framing TCP e por que é necessário?**
   - TCP é stream-based, não message-based. Precisamos delimitar onde uma mensagem começa e termina.

3. **Como funciona o LWW (Last Write Wins)?**
   - Compara timestamps. A escrita mais recente vence. Simples mas pode perder dados.

4. **Como evitamos loop infinito de sincronização?**
   - Flag `files_being_synced` para ignorar eventos de watchdog causados por writes da sincronização.

5. **O que são tombstones e por que não removemos deleções do banco?**
   - Se removermos, o nó vai re-baixar o arquivo deletado na próxima troca de índices.

6. **Como detectamos se um nó caiu?**
   - Heartbeat periódico via TCP. Se falhar 3 vezes seguidas, consideramos offline.

7. **Como um nó novo sincroniza ao entrar na rede?**
   - Broadcast HELLO → recebe ACK dos peers → INDEX_EXCHANGE → FILE_REQUEST para o que falta.

8. **O que acontece se dois nós editam o mesmo arquivo ao mesmo tempo?**
   - LWW: o timestamp mais recente vence. Todos convergem para o mesmo estado.
