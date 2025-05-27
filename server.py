import socket
import threading
from resp import RESP_PARSER
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
        
    
    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen(5)
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
        #self.save_data()
        
        for client in self.clients:
            client.close()
        
        if self.server_socket:
            self.server_socket.close()
            
        self.running = False
            
        
        print('Server has been shut down')
        
        
    def handle_client(self,client_socket,address):
        print(f"New client connected from address {address}")
        
        try:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        break
                        
                    parsed = RESP_PARSER.parse(data)
                    print(parsed)
                    command = parsed[0]
                    response = self.execute_command(command,parsed[1:])
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
            if client_socket in self.clients:
                self.clients.remove(client_socket)
                
            client_socket.close()
            
            print(f'Client {address} disconnected from server')
            
    
    def execute_command(self, command, args):
        print(command,args)
        
        if command is None or not isinstance(command,str):
            return "ERROR: Not a valid command"
        command = command.upper()
        if command =="PING":
            return "PONG"
        elif command == "GET":
            if len(args)<1:
                return "ERROR: No key provided"
            
            return self.store.get(args[0])
        elif command == "SET":
            if len(args)<2:
                return "ERROR: Insufficient values"
            
            elif len(args) == 2:
                return self.store.set(args[0],args[1])
            
            elif len(args) == 4:
                if args[2].upper() == "EX" and (isinstance(args[3],int) or isinstance(args[3],float)):
                    return self.store.set(key=args[0],val=args[1],ex=args[3])
                else:
                    return "ERROR: Insufficient values"
        elif command == "DEL":
            if len(args)<1:
                return "ERROR: No key provided"
            
            return self.store.delete(args[0])
        
        else:
            return 'ERROR: Not a valid command'
            
            
            
                    
            
            
                
            
        
        
                