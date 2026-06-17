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

TODO (Grupo):
  - Implementar compare_indices(local_index, remote_index) → 3 listas
  - Implementar resolve_conflict(local_state, remote_state) → 'LOCAL'|'REMOTE'|'EQUAL'
  - Tratar caso de deleção: se remoto tem DELETED com timestamp > local → deletar
"""

# class Reconciler:
#     @staticmethod
#     def compare_indices(local_index, remote_index):
#         """
#         Compara dois índices e retorna:
#           files_to_download: list[str] — arquivos para baixar do remoto
#           files_to_upload: list[str] — arquivos para enviar ao remoto
#           files_to_delete: list[str] — arquivos para deletar localmente
#         """
#         ...
#
#     @staticmethod
#     def resolve_conflict(local_state, remote_state):
#         """
#         Retorna 'LOCAL', 'REMOTE' ou 'EQUAL'.
#         """
#         ...
