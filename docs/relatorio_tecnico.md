# Relatório Técnico — SyncP2P: Sincronização de Arquivos P2P

## 1. Capa
- **Disciplina:** Redes de Computadores
- **Professor:** [Nome do Professor]
- **Integrantes:**
  - [Nome 1] — [Matrícula]
  - [Nome 2] — [Matrícula]
  - [Nome 3] — [Matrícula]
  - [Nome 4] — [Matrícula]
- **Data:** [Data de entrega]

---

## 2. Introdução

[PREENCHER: Descrever o objetivo do trabalho, a opção escolhida (Opção 2 - Clone OneDrive), e uma visão geral da solução implementada.]

---

## 3. Arquitetura do Sistema

[PREENCHER: Inserir diagrama de componentes, descrever cada módulo (network, sync, ui), explicar como as threads se comunicam, decisões arquiteturais.]

---

## 4. Protocolo de Comunicação

[PREENCHER: Descrever cada tipo de mensagem, formato JSON, framing TCP com struct, diagramas de sequência. Referenciar docs/protocolo.md.]

---

## 5. Mecanismos Implementados

### 5.1. Descoberta de Nós
[PREENCHER: Explicar broadcast UDP, heartbeat, manutenção da lista de peers.]

### 5.2. Sincronização de Índices
[PREENCHER: Explicar INDEX_EXCHANGE, quando é disparado, como funciona.]

### 5.3. Transferência de Arquivos
[PREENCHER: Explicar chunking, Base64, verificação de hash.]

### 5.4. Detecção de Alterações
[PREENCHER: Explicar uso do watchdog, debouncing, flag is_syncing.]

### 5.5. Resolução de Conflitos
[PREENCHER: Explicar LWW, desempate por node_id, tombstones.]

### 5.6. Propagação de Deleções
[PREENCHER: Explicar DELETE_NOTIFY, tombstones, por que manter no state_db.]

### 5.7. Tolerância a Falhas
[PREENCHER: Explicar heartbeat, timeout, reconexão automática, retry com backoff.]

---

## 6. Decisões de Projeto

[PREENCHER: Justificar escolhas como: Python, JSON, LWW, Base64, watchdog, etc.]

---

## 7. Instruções de Execução

### 7.1. Sem Docker
```bash
pip install watchdog
python src/main.py
```

### 7.2. Com Docker
```bash
docker-compose up --build
```

[PREENCHER: Detalhar passo a passo.]

---

## 8. Exemplos de Execução

[PREENCHER: Inserir screenshots/logs de cada cenário de teste.]

---

## 9. Dificuldades Encontradas

[PREENCHER: Descrever problemas reais encontrados durante o desenvolvimento.]

---

## 10. Conclusão

[PREENCHER: O que foi aprendido, possíveis melhorias futuras.]

---

## 11. Referências Bibliográficas

- TANENBAUM, A. S.; VAN STEEN, M. **Distributed Systems: Principles and Paradigms**. Pearson, 2017.
- KUROSE, J. F.; ROSS, K. W. **Computer Networking: A Top-Down Approach**. Pearson, 2021.
- KLEPPMANN, M. **Designing Data-Intensive Applications**. O'Reilly, 2017.
- Python Software Foundation. **Socket Programming HOWTO**. Disponível em: https://docs.python.org/3/howto/sockets.html
- Watchdog Documentation. Disponível em: https://python-watchdog.readthedocs.io/en/stable/
- Docker Documentation. **Networking in Compose**. Disponível em: https://docs.docker.com/compose/networking/
