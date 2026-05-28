# Guia da Arquitetura P2P - Clone do OneDrive

Este documento explica o objetivo e o funcionamento de cada arquivo e diretório gerados no projeto. Ele servirá de guia para dividirmos as tarefas e entendermos como as peças se conectam.

---

## 1. Visão Geral (Raiz do Projeto)

- **`README.md`**: É o manual do projeto. Nele ficam as instruções de como instalar dependências e como rodar o sistema localmente ou usando o Docker.
- **`Dockerfile`**: É o arquivo de receita que ensina o Docker a criar um "computador virtual" (container) contendo o Python e o nosso código. É útil para isolar o ambiente.
- **`docker-compose.yml`**: Orquestra vários containers de uma vez. Ele sobe `node1`, `node2` e `node3` simultaneamente, conectados em uma rede virtual. É perfeito para o professor testar tudo com um só comando e ganhar os **pontos extras**.
- **`.gitignore`**: Lista as pastas e arquivos que NÃO devem ser salvos no GitHub (como arquivos de banco de dados e as pastas geradas de `shared_folder`).

---

## 2. A Pasta de Código (`src/`)

O diretório `src/` concentra todo o nosso código Python. Ele está dividido em módulos lógicos (Rede, Sincronização e Interface).

### 2.1. Arquivos Base
- **`main.py`**: É o coração do programa. Onde tudo começa. Ele tem a responsabilidade de ligar as threads de Rede (escutar conexões), Descoberta (procurar colegas) e Sincronização (observar arquivos locais).
- **`config.py`**: Centraliza todas as configurações, portas UDP e TCP, endereço de broadcast, nome do nó e o caminho absoluto da nossa `shared_folder`. Se precisar mudar uma porta, mudamos aqui e reflete no sistema todo.

---

### 2.2. Módulo de Rede (`src/network/`)
Aqui fica toda a comunicação usando "Sockets" brutos (exigência do professor para não usarmos frameworks P2P).

- **`discovery.py`**: Usa o protocolo **UDP (Broadcast)**. Quando o nosso programa liga, ele "grita" na rede "Oi, eu sou o Nó 1 e minha porta TCP é 5001!". Também fica ouvindo caso outros nós entrem e gritem, para então registrar o IP deles em um dicionário de contatos (`known_nodes`).
- **`tcp_server.py`**: Fica parado de "ouvidos abertos" na porta TCP. Serve para *receber* mensagens confiáveis (tipo índices de arquivos, avisos de edição) ou conexões para transferir pedaços de arquivo de outro computador para o nosso.
- **`tcp_client.py`**: É a ação inversa do Servidor. É usado quando *nós* queremos conectar de forma ativa em outro nó que já descobrimos, seja para mandar um aviso ou requisitar um download de arquivo que nos falta.
- **`protocol.py`**: Concentra o dicionário de mensagens P2P (JSON). É aqui que padronizamos como são as mensagens (ex: `HELLO`, `FILE_REQUEST`, `INDEX_EXCHANGE`). Facilita para todo mundo não errar nomes de atributos.

---

### 2.3. Módulo de Sincronização e Lógica (`src/sync/`)
Responsável por garantir que os arquivos estejam iguais em todos os nós sem trafegar os arquivos inteiros toda hora.

- **`watcher.py`**: Usa a biblioteca `watchdog`. Fica vigiando a pasta `shared_folder`. Se o usuário jogar uma foto nova lá dentro, ou editar um .txt, o watcher detecta e avisa o nosso sistema que houve mudança.
- **`file_manager.py`**: Lida diretamente com leitura/escrita no disco. Uma função importante aqui é o calculador de **Hash (SHA-256)**. Nós só trafegamos arquivos na rede se o Hash deles for diferente do que o vizinho já tem (poupando internet). Ele também lê arquivos gigantes em "chunks" (pedaços pequenos de 4MB) para não explodir a memória RAM.
- **`state_db.py`**: É o banco de dados local. Pode ser um JSON simples ou SQLite. Ele anota o estado de cada arquivo: o Nome, o Timestamp de modificação e o Hash. É nossa "fonte da verdade".
- **`reconciler.py`**: O cérebro resolvedor de brigas ("Conflitos"). Se o Nó A editou um arquivo hoje às 14h, e o Nó B ligou agora com uma versão do mesmo arquivo das 10h, o reconciliador compara o estado dos dois usando LWW (Last-Write-Wins / Última Escrita Vence) e fala: "Ei, a sua versão está velha, atualize a sua". E então manda baixar.

---

### 2.4. Módulo de Interface (`src/ui/`)
- **`cli.py`**: É a interface de linha de comando. Reúne funções de `print` com formatações bonitas (logs formatados com data e hora). Isso faz o terminal ficar muito mais legível quando o professor for rodar o teste.

---

## 3. Dinâmica e Fluxo de Trabalho (Como as peças se falam)

1. A pessoa roda o `main.py`.
2. O `main.py` roda o `discovery.py` (UDP) numa Thread separada, achando os amigos da rede local.
3. O `watcher.py` liga os olhos na `shared_folder`.
4. Se uma foto for jogada na pasta, o `watcher.py` avisa o `state_db.py` para salvar o registro dessa foto.
5. Em seguida, o `main.py` usa o `tcp_client.py` e dispara uma mensagem do tipo `INDEX_EXCHANGE` (que tá no `protocol.py`) pra todo mundo da rede.
6. O computador do amigo recebe essa mensagem através do `tcp_server.py`.
7. O `reconciler.py` do amigo percebe que ele não tem a foto, então ele manda de volta um `FILE_REQUEST`.
8. Nosso nó recebe o `FILE_REQUEST`, pega a foto com o `file_manager.py` (em pedaços) e manda via TCP.
9. Todos os nós ficam em sincronia!
