import threading
import time
import random
import json
from enum import Enum
from typing import  List, Optional

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
    
    

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], cache_server = None, logger = None, persisted = False):
        self.node_id = node_id
        self.peers = peers
        self.cache_server = cache_server
        self.logger = logger
        
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        self.commit_index = -1
        self.last_applied = -1
        
        self.next_index = {}
        self.match_index = {}
        
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.votes_recieved = set()
        
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5,10)
        
        self.lock = threading.RLock()
        self.running = False
        self.election_timer = None
        self.heartbeat_timer = None
        self.persisted = persisted
        
        
    def start(self):
        self.running = True
        self.reset_election_timer()
        
    
    def stop(self):
        self.running = False
        if self.election_timer:
            self.election_timer.cancel()
        
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            
    
    def restore_state(self, term, voted_for, log_entries):
        if self.persisted:
            self.current_term = term
            self.voted_for = voted_for  
            self.log = log_entries

            for i, entry in enumerate(log_entries):
                if i <= self.commit_index:  
                    command = entry['command'][0]
                    args = entry['command'][1:]
                    self.cache_server.execute_local_write(command, args)

    def persist_state(self):
        if self.persisted:
            self.cache_server.persistence.save_state(self.current_term, self.voted_for)

    def persist_log(self):
        if self.persisted:
            self.cache_server.persistence.save_log(self.log)
                
                
    def handle_raft_command(self, cmd:List[str]):
        
        if not cmd:
            return "ERROR: empty command"
        

        elif cmd[0] == "RAFT_STATUS":
            return self.get_status()
        
        elif cmd[0] == "RAFT_REPLICATE":
            return self.replicate_command(cmd[1:])
        
        else:
            return f"ERROR: Unknown raft command {cmd}"
            

        
        
    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
            
        self.election_timeout = random.uniform(5,10)
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()
        
    
    def start_election(self):
        with self.lock:
            if not self.running:
                return
            
            self.state = NodeState.CANDIDATE
            
            self.current_term += 1
            self.voted_for = self.node_id
            if self.persisted:
                self.persist_state()
            self.votes_recieved = {self.node_id}
            self.leader_id = None
            
            self.logger.info(f"Election started for term {self.current_term} by Node {self.node_id}")
            
            self.reset_election_timer()
            
            last_log_index = len(self.log)-1
            last_log_term = self.log[-1]["term"] if self.log else 0
            
            for peer in self.peers:
                threading.Thread(
                    target=self.send_vote_request,
                    args = (peer, self.current_term, last_log_index, last_log_term),
                    daemon=True
                ).start()
                
                
                
    def send_vote_request(self,peer,term,last_log_index,last_log_term):
   
        try:
            if self.cache_server and hasattr(self.cache_server, 'send_rpc_to_peer'):
                self.logger.info(f"trying to call rpc for vote for {term} to {peer} by {self.node_id}")
                response = self.cache_server.send_rpc_to_peer(
                    peer, 'request_vote', term, self.node_id, last_log_index, last_log_term
                    
                )
                
             
                if response:
                    response_term = response.get('term', 0)
                    vote_granted = response.get('vote_granted', False)
                    self.handle_vote_response(peer, response_term, vote_granted)
        
        except Exception as e:
            self.logger.info(f"Failed to send vote request to {peer} {str(e)}")
            
            
    
    
    def handle_vote_response(self, peer, term, vote_granted):
        with self.lock:
            if self.state != NodeState.CANDIDATE or term != self.current_term:
                return
            
            if term > self.current_term:
                self.become_follower(term)
                return
            
            if vote_granted:
                self.votes_recieved.add(peer)
                
                if len(self.votes_recieved) > (len(self.peers)+1)//2:
                    self.become_leader()
                    
                    
    def become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        next_index = len(self.log)
        self.next_index = {peer: next_index for peer in self.peers}
        self.match_index = {peer: -1 for peer in self.peers}
        
        self.logger.info(f"Node {self.node_id} became leader for term {self.current_term}")

        if self.election_timer:
            self.election_timer.cancel()
            
        self.send_heartbeats()
        
        
    def become_follower(self, term):
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        if self.persisted:
                self.persist_state()
        self.leader_id = None
        
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            
        self.reset_election_timer()
        
        
    def send_heartbeats(self):
        if self.state != NodeState.LEADER or not self.running:
            return
        
        for peer in self.peers:
            threading.Thread(
                target = self.send_append_entries,
                args = (peer,),
                daemon=True
            ).start()
            
        self.heartbeat_timer = threading.Timer(2, self.send_heartbeats)
        self.heartbeat_timer.start()
        
        
    def send_append_entries(self, peer):
        with self.lock:
            if self.state != NodeState.LEADER:
                return
            
            next_index = self.next_index.get(peer, len(self.log))
            prev_log_index = next_index-1
            prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
            
            entries = self.log[next_index:] if next_index < len(self.log) else []
            entries_json = entries
            
            try:
                
               if self.cache_server and hasattr(self.cache_server, 'send_rpc_to_peer'):
                    response = self.cache_server.send_rpc_to_peer(
                        peer, 'append_entries', 
                        self.current_term, self.node_id, prev_log_index, 
                        prev_log_term, entries_json, self.commit_index
                    )
                    
                    if response:
                        response_term = response.get('term', 0)
                        success = response.get('success', False)
                        match_index = response.get('match_index', -1)
                        self.handle_append_entries_response(peer, response_term, success, match_index)
                        
            except Exception as e:
                self.logger.info(f"Failed to append entries to {peer}: {str(e)}")
        
    
    
    def handle_append_entries_response(self,peer, term , success, match_index):
        with self.lock:
            if self.state != NodeState.LEADER or term != self.current_term:
                return
            
            if term > self.current_term:
                self.become_follower(term)
                return
            
            if success:
                self.match_index[peer] = match_index
                self.next_index[peer] = match_index + 1
                self.update_commit_index()
                
            else:
                self.next_index[peer] = max(0, self.next_index[peer]-1)
                
    
    def update_commit_index(self):
        if self.state != NodeState.LEADER:
            return
        
        for index in range(len(self.log)-1, self.commit_index, -1):
            if self.log[index]['term']==self.current_term:
                replicated_count = 1
                for peer in self.peers:
                    if self.match_index.get(peer,-1)>=index:
                        replicated_count += 1
                if replicated_count > (len(self.peers)+1) // 2:
                    self.commit_index = index
                    self.apply_commited_entries()
                    break
                
                    
                
    def apply_commited_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            
            if self.cache_server:
                try:
                    self.cache_server.apply_raft_command(entry['command'], entry['args'])
                
                except Exception as e:
                    self.logger.error(f"Error Applying entry {self.last_applied} : {e}")
    
    
    
    
    def handle_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        with self.lock:
            if term > self.current_term:
                self.become_follower(term)
                
            if term >=self.current_term:
                self.last_heartbeat = time.time()
                self.leader_id = leader_id
                self.reset_election_timer()

            if term < self.current_term:
                return False, -1
            
            if (prev_log_index >=0 and 
                (prev_log_index>=len(self.log) or 
                 self.log[prev_log_index]['term']!=prev_log_term
                 )):
                return False, -1
            
            
            if entries:
                insert_index = prev_log_index+1
                self.log = self.log[:insert_index]+entries
                                
                if self.persisted:
                    self.persist_log()
            
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log)-1)
                self.apply_commited_entries()
                
            match_index = prev_log_index + len(entries)
            return True, match_index
        
        
        
    def handle_request_vote(self, term , candidate_id, last_log_index, last_log_term):
        with self.lock:
            if term > self.current_term:
                self.become_follower(term)
                
            if (term >=self.current_term
                and (self.voted_for is None or self.voted_for == candidate_id)):
                our_last_index = len(self.log)-1
                our_last_term = self.log[-1]['term'] if self.log else 0
                
                if (last_log_term > our_last_term or 
                    (last_log_term==our_last_term and last_log_index>=our_last_index)):
                    self.voted_for = candidate_id
                    if self.persisted:
                        self.persist_state()
                    self.reset_election_timer()
                    return True
                
            return False
    
    
    
        
    def replicate_command(self, args: List[str]) -> str:
        with self.lock:
            if self.state != NodeState.LEADER:
                return "ERROR: Not leader"
                
            if not args:
                return "ERROR: No command specified"
                
            command = args[0]
            cmd_args = args[1:] if len(args) > 1 else []
            
            entry = {
                "term": self.current_term,
                "index": len(self.log),
                "command": command,
                "args": cmd_args,
                "timestamp": time.time()
            }
            self.log.append(entry)
            
            if self.persisted:
                self.persist_log()

            for peer in self.peers:
                threading.Thread(
                    target=self.send_append_entries,
                    args=(peer,),
                    daemon=True
                ).start()
            
            return f"OK replicated command {command}"
    
    def get_status(self) -> str:
        return f"STATE:{self.state.value} TERM:{self.current_term} LEADER:{self.leader_id} LOG_SIZE:{len(self.log)}"
    
    def is_leader(self) -> bool:
        return self.state == NodeState.LEADER
    
    def get_leader_id(self) -> Optional[str]:
        return self.leader_id
    
            
                            
                    
    
            
            
            
                        