class Reconciler:
    @staticmethod
    def resolve_conflict(local_state, remote_state):
        """
        Retorna 'LOCAL' se a versão local for mais nova.
        Retorna 'REMOTE' se a versão remota for mais nova.
        Retorna 'EQUAL' se forem iguais.
        """
        if not local_state:
            return 'REMOTE'
        if not remote_state:
            return 'LOCAL'
            
        if local_state['hash'] == remote_state['hash']:
            return 'EQUAL'
            
        # LWW - Last Write Wins (Última escrita vence)
        if local_state['timestamp'] > remote_state['timestamp']:
            return 'LOCAL'
        else:
            return 'REMOTE'
