# Cenários de Teste — SyncP2P

## Cenário 1: Sincronização Básica (Arquivo Novo)
**Pré-condição:** 3 nós rodando, todos vazios.
**Ação:** Copiar `foto.jpg` para `node1_data/`.
**Resultado Esperado:** `foto.jpg` aparece em `node2_data/` e `node3_data/` em até 10 segundos.
**Verificação:** Comparar hashes SHA-256 dos 3 arquivos.

---

## Cenário 2: Ingresso de Novo Nó
**Pré-condição:** node1 e node2 rodando com 3 arquivos sincronizados. node3 desligado.
**Ação:** Iniciar node3.
**Resultado Esperado:** node3 descobre os peers, faz INDEX_EXCHANGE e baixa todos os 3 arquivos.
**Verificação:** `node3_data/` contém os mesmos arquivos com mesmos hashes.

---

## Cenário 3: Modificação de Arquivo
**Pré-condição:** Todos sincronizados com `doc.txt` contendo "versão 1".
**Ação:** Editar `doc.txt` no node1 para "versão 2".
**Resultado Esperado:** `doc.txt` em node2 e node3 atualizado para "versão 2".

---

## Cenário 4: Deleção de Arquivo
**Pré-condição:** Todos sincronizados com `old.txt`.
**Ação:** Deletar `old.txt` do `node1_data/`.
**Resultado Esperado:** `old.txt` removido de `node2_data/` e `node3_data/`.

---

## Cenário 5: Tolerância a Falhas
**Pré-condição:** 3 nós sincronizados.
**Ação:**
1. `docker stop` no node2
2. Adicionar `novo.pdf` no node1
3. Verificar que node3 recebeu
4. `docker start` no node2
**Resultado Esperado:** node2 sincroniza `novo.pdf` após reconectar.

---

## Cenário 6: Arquivo Grande (50MB+)
**Pré-condição:** 3 nós rodando.
**Ação:** Copiar arquivo de 50MB+ para `node1_data/`.
**Resultado Esperado:** Arquivo transferido em chunks, sem corrupção (hash verificado).

---

## Cenário 7: Múltiplos Tipos de Arquivo
**Ação:** Copiar PDF, JPG, MP4 e TXT para node1.
**Resultado Esperado:** Todos sincronizam sem erros.

---

## Cenário 8: Conflito Simultâneo (LWW)
**Ação:** Editar `config.txt` no node1 e node2 quase simultaneamente.
**Resultado Esperado:** Após reconciliação, todos os nós convergem para a versão com timestamp mais recente.
