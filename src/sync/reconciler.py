"""reconciliação de índices e resolução de conflitos LWW."""


class Reconciler:

    @staticmethod
    def resolve_conflict(local_state, remote_state, local_node_id, remote_node_id):
        """retorna 'LOCAL', 'REMOTE' ou 'EQUAL' usando LWW com desempate por node_id."""
        local_ts = local_state["timestamp"]
        remote_ts = remote_state["timestamp"]
        if local_ts > remote_ts:
            return "LOCAL"
        if local_ts < remote_ts:
            return "REMOTE"
        # timestamps idênticos: node_id mais alto vence (determinístico)
        if local_node_id > remote_node_id:
            return "LOCAL"
        if local_node_id < remote_node_id:
            return "REMOTE"
        return "EQUAL"

    @staticmethod
    def compare_indices(local_index, remote_index, local_node_id, remote_node_id):
        """
        download  — arquivos para baixar do remoto
        upload    — arquivos para enviar ao remoto
        delete    — arquivos para deletar localmente
        """
        download = []
        upload = []
        delete = []

        for filename, remote_file in remote_index.items():
            if filename not in local_index:
                if remote_file["status"] == "ACTIVE":
                    download.append(filename)
                continue

            local_file = local_index[filename]
            if local_file["hash"] == remote_file["hash"] and local_file["status"] == remote_file["status"]:
                continue

            winner = Reconciler.resolve_conflict(local_file, remote_file, local_node_id, remote_node_id)
            if winner == "REMOTE":
                if remote_file["status"] == "ACTIVE":
                    download.append(filename)
                elif remote_file["status"] == "DELETED":
                    delete.append(filename)
            elif winner == "LOCAL" and local_file["status"] == "ACTIVE":
                upload.append(filename)

        for filename, local_file in local_index.items():
            if filename not in remote_index and local_file["status"] == "ACTIVE":
                upload.append(filename)

        return download, upload, delete
