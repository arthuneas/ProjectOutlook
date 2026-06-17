# Especificação do Protocolo de Comunicação — SyncP2P

## 1. Visão Geral

O protocolo SyncP2P utiliza duas camadas de transporte:
- **UDP** (porta 5000): Descoberta de nós via broadcast
- **TCP** (porta 5001): Todas as demais comunicações (sincronização, transferência, controle)

## 2. Formato de Mensagens

### 2.1. Mensagens UDP
Mensagens UDP são pacotes JSON simples, limitados a 1024 bytes.

### 2.2. Mensagens TCP (com Framing)
Mensagens TCP usam Length-Prefix Framing:
```
┌────────────────┬──────────────────────┐
│ 4 bytes (BE)   │ N bytes              │
│ tamanho payload│ JSON UTF-8 payload   │
└────────────────┴──────────────────────┘
```
- O tamanho é codificado como unsigned int 32-bit big-endian (`struct.pack('>I', n)`)

## 3. Tipos de Mensagens

### 3.1. HELLO (UDP)
- **Direção:** Broadcast → todos os nós
- **Quando:** Ao iniciar e periodicamente (a cada 30s)
- **Campos:** `type`, `node_id`, `name`, `tcp_port`, `version`

### 3.2. HELLO_ACK (UDP ou TCP)
- **Direção:** Resposta direta ao emissor do HELLO
- **Campos:** `type`, `node_id`, `name`, `tcp_port`

### 3.3. INDEX_EXCHANGE (TCP)
- **Direção:** Ponto-a-ponto
- **Quando:** Após descoberta de novo nó ou periodicamente
- **Campos:** `type`, `node_id`, `files` (dicionário completo de arquivos)

### 3.4. FILE_REQUEST (TCP)
- **Campos:** `type`, `node_id`, `filename`

### 3.5. FILE_TRANSFER_START (TCP)
- **Campos:** `type`, `filename`, `size`, `hash`, `total_chunks`

### 3.6. FILE_CHUNK (TCP)
- **Campos:** `type`, `filename`, `chunk_index`, `data` (Base64), `is_last`

### 3.7. FILE_TRANSFER_COMPLETE (TCP)
- **Campos:** `type`, `filename`, `hash`

### 3.8. FILE_NOTIFY (TCP)
- **Campos:** `type`, `node_id`, `action` (CREATED/MODIFIED/DELETED), `filename`, `hash`, `timestamp`, `size`

### 3.9. DELETE_NOTIFY (TCP)
- **Campos:** `type`, `node_id`, `filename`, `timestamp`

### 3.10. HEARTBEAT (TCP)
- **Campos:** `type`, `node_id`, `timestamp`

### 3.11. HEARTBEAT_ACK (TCP)
- **Campos:** `type`, `node_id`

### 3.12. NODE_LEAVING (TCP)
- **Campos:** `type`, `node_id`

## 4. Diagramas de Sequência

### 4.1. Descoberta e Sincronização Inicial
```
Nó A (novo)              Rede              Nó B (existente)
    │                      │                      │
    │── HELLO (broadcast)─→│─────────────────────→│
    │                      │                      │
    │←─────────────────────│←── HELLO_ACK ────────│
    │                      │                      │
    │──── INDEX_EXCHANGE (TCP) ──────────────────→│
    │                      │                      │
    │←──── INDEX_EXCHANGE (TCP) ──────────────────│
    │                      │                      │
    │  [Reconciler compara os dois índices]        │
    │                      │                      │
    │──── FILE_REQUEST ──────────────────────────→│
    │←── FILE_TRANSFER_START ─────────────────────│
    │←── FILE_CHUNK(0) ──────────────────────────│
    │←── FILE_CHUNK(1, last=true) ────────────────│
    │──── FILE_TRANSFER_COMPLETE ────────────────→│
```

### 4.2. Propagação de Alteração Local
```
Usuário          Nó A               Nó B              Nó C
  │                │                  │                  │
  │─ salva arq. ─→│                  │                  │
  │                │                  │                  │
  │           [watchdog detecta]      │                  │
  │           [calcula hash]          │                  │
  │           [atualiza state_db]     │                  │
  │                │                  │                  │
  │                │── FILE_NOTIFY ─→│                  │
  │                │── FILE_NOTIFY ─→│─────────────────→│
  │                │                  │                  │
  │                │←─ FILE_REQUEST ──│                  │
  │                │←─ FILE_REQUEST ──│──────────────────│
  │                │                  │                  │
  │                │── FILE_CHUNKS ─→│                  │
  │                │── FILE_CHUNKS ─→│─────────────────→│
```
