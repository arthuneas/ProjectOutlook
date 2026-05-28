# Clone do OneDrive P2P (Sistemas Distribuídos)

Este projeto implementa um sistema distribuído de sincronização de arquivos P2P.

## Como Executar Localmente (Sem Docker)

1. Instale a dependência do watchdog:
   ```bash
   pip install watchdog
   ```

2. Execute o nó:
   ```bash
   python src/main.py
   ```

## Como Executar com Docker (Simular Múltiplos Nós)

Para rodar 3 instâncias isoladas na mesma rede simulando 3 computadores diferentes:

```bash
docker-compose up --build
```

As pastas `node1_data`, `node2_data` e `node3_data` serão criadas na raiz do projeto. Qualquer arquivo colocado em uma delas deve sincronizar automaticamente com as outras.

## Estrutura Atual
- `src/network/`: Contém protocolos UDP (descoberta) e TCP (transferência/sync).
- `src/sync/`: Contém lógica de banco de dados local de estado, hashing e reconciliação.
- `src/ui/`: CLI simples.
