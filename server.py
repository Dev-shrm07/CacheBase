import socket
import threading
from resp import RESP_PARSER
import time
from typing import List
from store import CACHE_STORE


class CACHE_SERVER:
    def __init__(self,host='localhost',port=6379,max_clients = 5):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.store = CACHE_STORE()
        self.clients:List[socket.socket]=[]
        self.server_socket = None
        self.running = False
        self.blocked_clients = {}
        self.blocking_lock = threading.Lock()
        
    
    
    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen(self.max_clients)
        self.running = True
        
        print(f"The cache server is running live at host : {self.host} and port : {self.port}")
        
        try:
            while self.running:
                try:
                    client_socket,addr = self.server_socket.accept()
                    self.clients.append(client_socket)
                    
                    client_thread = threading.Thread(target=self.handle_client, args=(client_socket,addr),daemon=True)
                    client_thread.start()
                
                except Exception as e:
                    if self.running:
                        print(f'Error excepting the client {str(e)}')
        
        except KeyboardInterrupt:
            print('Shutting Down the server')
            
        finally:
            self.shutdown()
    
    
    def shutdown(self):
        self.clean_expired_keys()
        
        for client in self.clients:
            client.close()
        
        if self.server_socket:
            self.server_socket.close()
            
        self.running = False
            
        
        print('Server has been shut down')
        
    

        
    def handle_client(self,client_socket,address):
        print(f"New client connected from address {address}")
        
        threading.current_thread().client_socket = client_socket
        try:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        break
                        
                    parsed = RESP_PARSER.parse(data)
                    command = parsed[0]
                    response = self.execute_command(command,parsed[1:])
                    if response is None:
                        response = f"ERROR IN COMMAND {command}"
                    response_to_send = RESP_PARSER.encode(response)
                    
                    client_socket.send(response_to_send)
                    
                
                except socket.timeout:
                    continue
                
                except Exception as e:
                    try:
                        client_socket.send(RESP_PARSER.encode(f'ERROR {str(e)}'))
                    except:
                        pass
                
        except Exception as e:
            print(f"Error {str(e)}")
        
        finally:
            with self.blocking_lock:
                if client_socket in self.blocked_clients:
                    del self.blocked_clients[client_socket]
                    
            if client_socket in self.clients:
                self.clients.remove(client_socket)
                
            client_socket.close()
            
            print(f'Client {address} disconnected from server')
            
    
    def execute_command(self, command, args):

        
        if command is None or not isinstance(command,str):
            return "ERROR: Not a valid command"
        command = command.upper()
        if command =="PING":
            return "PONG"
        elif command == "GET":
            return self.cmd_get(args)
        elif command == "SET":
            return self.cmd_set(args)
        elif command == "DEL":
            return self.cmd_del(args)
        
        elif command =='EXPIRE':
            return self.cmd_expire(args)
        
        elif command == "TYPE":
            return self.cmd_type(args)
        
        elif command == "XADD":
            return self.cmd_xadd(args)
        
        elif command == "XRANGE":
            return self.cmd_xrange(args)
        
        elif command =='XREAD':
            return self.cmd_xread(args)
        
        elif command =="XLEN":
            return self.cmd_xlen(args)
        
        elif command == "XDEL":
            return self.cmd_xdel(args)
        
        elif command == "ECHO":
            if len(args)<1:
                return "ERROR: Insufficient Data"
            return " ".join(args)
        
        else:
            return 'ERROR: Not a valid command'
        
    
    
    
    def cmd_xadd(self,args):
        if len(args)<4:
            return "ERROR: Insufficient values"
        
        key = args[0]
        if args[1]=="MAXLEN":
            if args[2]=='~':
                lim = args[3]
                if not isinstance(lim,int):
                    return "ERROR: Invalid Limit"
                id = args[4]
                if len(args[5:])%2!=0:
                    return "ERROR: Invalid entry for stream"
                
                return self.store.xadd(key=key,id=id,maxLen=lim,Fields=args[5:],Approx=True)
            
            else:
                lim = args[2]
                if not isinstance(lim,int):
                    return "ERROR: Invalid Limit"
                id = args[3]
                if len(args[4:])%2!=0:
                    return "ERROR: Invalid entry for stream"
                
                return self.store.xadd(key=key,id=id,maxLen=lim,Fields=args[4:])
            
        id = args[1]
        if len(args[2:])%2!=0:
                    return "ERROR: Invalid entry for stream"
                
        response =  self.store.xadd(key=key,id=id,Fields=args[2:])
        if response is not None:
            self.notify_blocked_clients(key=key)
        
        return response
            
    
    
    def cmd_xrange(self,args):
        if len(args)<3:
            return "ERROR: Invalid command"
        
        if len(args)==3:
            return self.store.xrange(key=args[0],start=args[1],end=args[2])
        
        elif len(args)==5:
            if not isinstance(args[4],int):
                return "ERROR: Invalid count"
            return self.store.xrange(key=args[0],start=args[1],end=args[2],count=args[4])
        
        return "ERROR: Invalid Command"
    
    
    def cmd_xread(self,args):

        if len(args)<3:
            return "ERROR: Invalid command"
        block = None
        count = None
        idx = 0
        if args[idx]=='COUNT':
            try:
                count = int(args[idx+1])
                if count == 0:
                    return 'ERROR: Invalid count value'
            except:
                return "ERROR: Invalid count value"
            idx += 2
            if idx==len(args)-1:
                return "ERROR: Invalid command"
            
        if args[idx]=='BLOCK':
            try:
                block = int(args[idx+1])
            except:
                return "ERROR: Invalid block value"
            
            idx += 2
            if idx==len(args)-1:
                return "ERROR: Invalid command"
            
        if args[idx]!='STREAMS':
            return "ERROR: Invalid commands"
        
        idx += 1
        if idx==len(args)-1 or len(args[idx:])%2!=0:
            return "ERROR: Invalid command"
        
        
        
        midpoint = int((len(args)-idx)/2)

        keys = args[idx:idx+midpoint]
        ids = args[idx+midpoint:]
        

        for i in range(len(keys)):
            if ids[i]=='$':
                ids[i] = self.store.get_stream_last_id(keys[i])
                if ids[i] is None:
                    return "ERROR: Invalid Key or ID"

                
        result = self.store.xread(keys=keys,ids=ids,count=count)

        if ((result != [] and result) and any(data for _,data in result)) or block is None:
            return result
        
        return self.wait_and_block(keys,ids,count,block)
    
    
    def wait_and_block(self,keys,ids,count,block):
        client_socket = getattr(threading.current_thread(),'client_socket',None)

        if not client_socket:
            return "ERROR: Client socket not found for blocking"
        with self.blocking_lock:
            self.blocked_clients[client_socket] = {
                'keys':keys,
                'ids':ids,
                'count':count,
                'event':threading.Event()
            }
            
        
        try:
            if block == 0:
                while True:
                    self.blocked_clients[client_socket]['event'].wait()
                    self.blocked_clients[client_socket]['event'].clear()
                    result = self.store.xread(keys=keys,ids=ids,count=count)
                    if ((result != [] and result) and any(data for _,data in result)):
                        return result
            
            else:
                timeout = block/1000
                start_time = time.time()
                
                while time.time()-start_time<timeout:
                    remaining_time = timeout-(time.time()-start_time)
                    if self.blocked_clients[client_socket]['event'].wait(timeout=remaining_time):
                        self.blocked_clients[client_socket]['event'].clear()
                        
                        result = self.store.xread(keys=keys,ids=ids,count=count)
                        if ((result != [] and result) and any(data for _,data in result)):
                            return result
                    else:
                        break
                    
                return [[key,[]] for key in keys]
        
        finally:
            with self.blocking_lock:
                if client_socket in self.blocked_clients:
                    del self.blocked_clients[client_socket]
                        
  
        
        
    def notify_blocked_clients(self,key):
        with self.blocking_lock:
            clients = []
            for client_socket,info in self.blocked_clients.items():
                if key in info['keys']:
                    clients.append(client_socket)
                    
            
            for client in clients:
                if client in self.blocked_clients:
                    self.blocked_clients[client]['event'].set()
                
                
    def cmd_xlen(self,args):
        if len(args)<1:
            return "ERROR: Insufficient Values"
        return self.store.xlen(args[0])
        
        
    def cmd_xdel(self,args):
        if len(args)<2:
            return "ERROR: Insufficient values"
        key = args[0]
        return self.store.xdel(key=key,ids=args[1:])
    
    def cmd_type(self,args):
        if len(args)<1:
            return "ERROR: Insufficient values"
        return self.store.get_type(args[0])
    
    
    def cmd_expire(self,args):
        if len(args)!=2:
            return "ERROR: Incorrect format"

        if not isinstance(args[1],float) and not isinstance(args[1],int):
            return "ERROR: Invalid value for time"
        
        return self.store.expire(args[0],args[1])

    def cmd_del(self,args):
        if len(args)<1:
                return "ERROR: No key provided"
            
        return self.store.delete(args)
        
    def cmd_set(self,args):
        if len(args)<2:
                return "ERROR: Insufficient values"
            
        elif len(args) == 2:
                return self.store.set(args[0],args[1])
            
        elif len(args) == 4:
                if args[2].upper() == "EX" and (isinstance(args[3],int) or isinstance(args[3],float)):
                    return self.store.set(key=args[0],val=args[1],ex=args[3])
                else:
                    return "ERROR: Insufficient values"
                
    def cmd_get(self,args):
        if len(args)<1:
            return "ERROR: No key provided"
            
        return self.store.get(args[0])
    
    def clean_expired_keys(self):
        while self.running:
            try:
                self.store.clean_up_expired_keys()
                self.running = False
                time.sleep(2)
            
            except Exception as e:
                print(f"Error in cleanup thread: {e}")
                time.sleep(5)
            
            
            
                    
            
            
                
            
        
        
                