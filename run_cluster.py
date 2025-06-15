
import threading
import time
import sys
import signal
from server.server import CACHE_SERVER 

servers = []

def signal_handler(sig, frame):
    print("\nShutting down servers...")
    for server in servers:
        server.shutdown()
    sys.exit(0)

def start_server(node_id, port, peers, log_file):
    server = CACHE_SERVER(
        log_file_path=log_file,
        host='localhost',
        port=port,
        node_id=node_id,
        peers=peers,
        enable_raft=True
    )
    servers.append(server)
    print(f"Starting {node_id} on port {port}")
    server.start()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    # sample cluster configuration
    # persistence is optional
    cluster_config = [
        {
            'node_id': 'node1',
            'port': 6379,
            'peers': ['node2:6380:localhost:7380', 'node3:6381:localhost:7381'],
            'log_file': 'logs/node1.log'
        },
        {
            'node_id': 'node2', 
            'port': 6380,
            'peers': ['node1:6379:localhost:7379', 'node3:6381:localhost:7381'],
            'log_file': 'logs/node2.log'
        },
        {
            'node_id': 'node3',
            'port': 6381,
            'peers': ['node1:6379:localhost:7379', 'node2:6380:localhost:7380'], 
            'log_file': 'logs/node3.log'
        }
    ]

    threads = []
    for config in cluster_config:
        thread = threading.Thread(
            target=start_server,
            args=(config['node_id'], config['port'], config['peers'], config['log_file']),
            daemon=True
        )
        thread.start()
        threads.append(thread)
        time.sleep(1)  
    
    print("All servers started. Waiting for leader election...")
    print("Press Ctrl+C to stop all servers")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()