<div align="center">

# CacheBase 💾

</div>

A distributed in-memory key-value store with built-in Raft consensus algorithm for fault tolerance and strong consistency, built using raw TCP sockets and RPCs. CacheBase provides Redis-compatible commands with distributed capabilities. It supports string type keys along with streams, to store and read data streams with features like xadd, xread with blocking etc.

## Features


- **Redis-Compatible**: Supports popular Redis commands and protocols, along with RESP (redis serialization protocol) to interact with any redis client
- **Distributed Architecture**: Built-in Raft consensus for distributed operations across nodes using RPCs
- **High Concurrency**: Handles multiple clients across multiple nodes concurrently with thread-safe operations and distributed blocking commands
- **Strong Consistency**: Guaranteed consistency across all nodes in the cluster
- **Fault Tolerance**: Automatic leader election and node failure handling along with data persistence using persistence handling on disk
- **Multiple Data Types**: Support for strings, and streams
- **TTL Support**: Automatic key expiration with background cleanup
- **Easy Configuration**: Simple setup for single node or cluster deployment


## Architecture

CacheBase implements the Raft consensus algorithm to maintain consistency across distributed nodes using RPCs. The system consists of:

- **Leader Node**: Handles all write operations and replicates to followers
- **Follower Nodes**: Replicate data from leader and can serve read operations
- **Candidate Nodes**: Participate in leader election when needed
- **Custom TCP Layer**: Communication between nodes and clients is built from scratch over raw TCP sockets, implementing RESP parsing and request handling manually without external Redis libraries.


### Raft Implementation
- **Leader Election**: Automatic selection of leader nodes with randomized timeouts
- **Log Replication**: Commands are replicated across majority of nodes before commitment
- **Safety**: Ensures no data loss even during network partitions or node failures

## Quick Start

### Single Node Setup
```bash
python run_single_node.py
```

### Cluster Setup 
```bash
python run_cluster.py
```

## Supported Commands 📋

### Basic Operations

#### `PING`
Test server connectivity
```
PING
# Returns: PONG
```

#### `ECHO <message>`
Echo the given message
```
ECHO "Hello World"
# Returns: Hello World
```

### String Operations

#### `SET <key> <value> [EX seconds]`
Set a key-value pair with optional expiration
```
SET mykey "myvalue"
SET "user_data" EX 3600
# Returns: OK
```

#### `GET <key>`
Retrieve value by key
```
GET mykey
# Returns: myvalue
```

#### `DEL <key> [key ...]`
Delete one or more keys
```
DEL mykey
DEL key1 key2 key3
# Returns: (integer) number of keys deleted
```

#### `TYPE <key>`
Get the data type of a key
```
TYPE mykey
# Returns: string
```

#### `EXPIRE <key> <seconds>`
Set TTL for an existing key
```
EXPIRE mykey 300
# Returns: 1 if timeout was set, 0 if key doesn't exist
```

### Stream Operations

#### `XADD <stream> <ID> <field> <value> [field value ...]`
Add entry to a stream
```
XADD mystream * temperature 25.5 humidity 60
XADD mystream 1234567890-0 sensor_id 42 value 100
# Returns: generated or specified ID
```

#### `XRANGE <stream> <start> <end> [COUNT count]`
Get range of entries from stream
```
XRANGE mystream - +
XRANGE mystream 1234567890 1234567999 COUNT 10
# Returns: list of [ID, [field1, value1, field2, value2, ...]]
```

#### `XREAD [COUNT count] [BLOCK milliseconds] STREAMS <stream> [stream ...] <ID> [ID ...]`
Read from streams with optional blocking. In cluster mode, blocking XREAD commands are handled consistently across nodes - when a client blocks on one node, the blocking state is maintained even during leader changes, ensuring reliable stream consumption in distributed environments.
```
XREAD STREAMS mystream 0
XREAD COUNT 5 STREAMS stream1 stream2 $ $
XREAD BLOCK 1000 STREAMS mystream $
# Returns: [[stream_name, [[ID, [field, value, ...]]]]]
```

#### `XLEN <stream>`
Get length of stream
```
XLEN mystream
# Returns: (integer) number of entries
```

#### `XDEL <stream> <ID> [ID ...]`
Delete entries from stream
```
XDEL mystream 1234567890-0 1234567890-1
# Returns: (integer) number of entries deleted
```

### Raft Operations (Cluster Mode)

#### `RAFT RAFT_STATUS`
Get current node status in Raft cluster
```
RAFT RAFT_STATUS
# Returns: STATE:1 TERM:5 LEADER:node1 LOG_SIZE:42
```

#### `RAFT RAFT_REPLICATE <command> [args...]`
Manually replicate a command across cluster
```
RAFT RAFT_REPLICATE SET distributed_key "cluster_value"
# Returns: OK replicated command SET
```

## Configuration For Server Node

- `host`: Server host (default: localhost)
- `port`: Server port (default: 6379)  
- `node-id`: Unique identifier for this node in cluster
- `peers`: Comma-separated list of peer nodes (format: id:port:host:rpc_port)
- `enable-raft`: Enable Raft consensus mode
- `max-clients`: Maximum concurrent clients (default: 5)
- `log-file`: Path to log file
- `persistence_file_dir`: Directory of File to save logs and state, To be provided to enable persistance handling (default: None)
- `default_expiry`: Defualt Expiration time of keys in seconds. (default: 15)



## Data Consistency

### Write Operations
In cluster mode, write operations (`SET`, `DEL`, `EXPIRE`, `XADD`, `XDEL`) are:
1. Submitted to the current leader
2. Replicated to majority of nodes
3. Committed only after majority acknowledgment
4. Applied to state machine on all nodes

### Read Operations  
Read operations (`GET`, `XRANGE`, `XREAD`) can be served by any node and reflect the most recently committed state.



### Single Node Demo

![Single Node Demo](https://github.com/user-attachments/assets/7c2db2a3-6cba-4cc2-9ab5-5f79ee92dd3d)


### Multi Node Cluster

![Cluster Demo](https://github.com/user-attachments/assets/5c6c46a7-7c9f-4406-b157-53721b13bb99)


## Contributing

Contributions are welcome! Please ensure your code follows the existing style and includes appropriate tests.


