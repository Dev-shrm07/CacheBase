import json
import os

class Persistence:
    def __init__(self, node_id: str, persistence_file_dir, logger):
        self.node_id = node_id
        self.logger = logger
        self.state_file = os.path.join(persistence_file_dir, f"raft_state_{node_id}.json")
        self.log_file = os.path.join(persistence_file_dir, f"raft_log_{node_id}.json")
    
    def save_state(self, current_term, voted_for):

        try:
            state = {
                'current_term': current_term,
                'voted_for': voted_for
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            self.logger.error(f"Error saving state: {e}")
    
    def load_state(self):
     
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                return state.get('current_term', 0), state.get('voted_for', None)
            except Exception as e:
                self.logger.error(f"Error loading state: {e}")
        return 0, None
    
    def save_log(self, log_entries: list):
        
        try:
            with open(self.log_file, 'w') as f:
                json.dump(log_entries, f)
        except Exception as e:
            self.logger.error(f"Error saving log: {e}")
    
    def load_log(self):
        
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading log: {e}")
        return []