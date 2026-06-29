"""
reconciler.py — Lógica de reconciliação e resolução de conflitos.

Responsabilidades:
  1. Comparar índice local com índice remoto
  2. Identificar arquivos para download, upload e deleção
  3. Resolver conflitos usando LWW (Last Write Wins)
  4. Desempatar timestamps iguais usando node_id (determinístico)

ESTRATÉGIA LWW (Last Write Wins):
  - Compara timestamps: maior timestamp = versão mais recente = vence
  - Se timestamps iguais: maior node_id (comparação de string) vence
  - Simples, determinístico, mas pode perder dados em edição simultânea
  - Perfeitamente aceitável para o escopo do trabalho

LÓGICA DE COMPARAÇÃO:
  Para cada arquivo no REMOTO:
    Se eu NÃO tenho → download (se ACTIVE)
    Se eu tenho e hash igual → nada
    Se eu tenho e hash diferente → LWW decide
    Se remoto DELETED e eu ACTIVE → verificar timestamps

  Para cada arquivo LOCAL que NÃO está no remoto:
    → O remoto precisa baixar de mim (upload)
"""


class Reconciler:

    # compara o timestamp de ambos os arquivos, aquele que tiver o timestamp mais recente será considerado
    # caso, os tempos sejam muito parecidos, o node_id será comparado alfabeticamente
    @staticmethod
    def resolve_conflict(local_state, remote_state, local_node_id, remote_node_id):
        # agora faremos a lógica do timestamps e retornar de acordo

        if local_state["timestamp"] > remote_state["timestamp"]:
            return "LOCAL"

        elif local_state["timestamp"] < remote_state["timestamp"]:
            return "REMOTE"

        else:
            # por segurança, há a verificação do ID dos nós
            if local_node_id > remote_node_id:
                return "LOCAL"

            elif local_node_id < remote_node_id:
                return "REMOTE"

            else:
                return "EQUAL"

    # avalia todos os arquivos mapeados de uma vez, cruza os indices dos computadores. A partir disso, identifica o que foi alterado, o que é novo e excluído.
    # se haver tiver divergência no mesmo arquivo, chama o resolve_conflit e resolve o conflito. No fim, distribui o resultado final em três listas.
    @staticmethod
    def compare_indices(local_index, remote_index, local_node_id, remote_node_id):
        """
        Compara dois índices e retorna:
          files_to_download: list[str] — arquivos para baixar do remoto
          files_to_upload: list[str] — arquivos para enviar ao remoto
          files_to_delete: list[str] — arquivos para deletar localmente
        """

        # listas de retorno de comparação
        download = []
        upload = []
        delete = []

        for filename, remote_file in remote_index.items():
            remote_status = remote_file.get("status", "ACTIVE")
            if filename not in local_index:
                if remote_file["status"] == "ACTIVE":
                    download.append(filename)

            else:
                local_file = local_index[filename]

                if (
                    local_file["hash"] != remote_file["hash"]
                    or local_file["status"] != remote_file["status"]
                ):
                    winner = Reconciler.resolve_conflict(
                        local_file, remote_file, local_node_id, remote_node_id
                    )

                    if winner == "REMOTE":
                        if remote_file["status"] == "ACTIVE":
                            download.append(filename)

                        elif remote_file["status"] == "DELETED":
                            delete.append(filename)

                    if winner == "LOCAL" and local_file["status"] == "ACTIVE":
                        upload.append(filename)

        for filename, local_file in local_index.items():
            if filename not in remote_index:
                if local_file["status"] == "ACTIVE":
                    upload.append(filename)

        return download, upload, delete
