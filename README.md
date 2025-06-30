# LazyDB

LazyDB is a simple, in-memory key-value store that speaks the Redis Serialization Protocol (RESP). It's written in Go and serves as a lightweight Redis clone, supporting essential commands and master-slave replication.

## Features

- **In-Memory Storage:** Fast key-value storage using a `map`.
- **Redis Protocol (RESP):** Compatible with Redis clients.
- **Supported Commands:**
    - `PING`: Checks if the server is alive.
    - `ECHO`: Returns the given string.
    - `SET`: Sets a key-value pair with an optional expiry (PX).
    - `GET`: Retrieves the value for a given key.
    - `INFO replication`: Provides information about the replication status.
- **Master-Slave Replication:**
    - `REPLCONF`: Configures replication settings.
    - `PSYNC`: Initiates replication with a master.
    - `WAIT`: Waits for a specified number of slaves to acknowledge a write.

## Getting Started

### Prerequisites

- Go 1.23 or higher

### Building the Server

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/sudo-luffy/lazydb.git
    cd lazydb
    ```

2.  **Build the executable:**
    ```sh
    go build -o lazydb ./cmd
    ```

### Running the Server

-   **As a master:**
    ```sh
    ./lazydb --port 6379
    ```

-   **As a slave:**
    ```sh
    ./lazydb --port 6380 --replicaof localhost 6379
    ```

## Replication

LazyDB supports master-slave replication, allowing you to create a distributed and resilient data store.

### How it Works

1.  **Handshake:** When a slave connects to a master, it initiates a handshake process:
    - The slave sends a `PING` to the master.
    - The slave sends its listening port and capabilities using `REPLCONF`.
    - The slave sends `PSYNC` to request a full resynchronization.

2.  **Full Resynchronization:**
    - The master responds with `+FULLRESYNC`, its replication ID, and offset.
    - The master sends an empty RDB file to the slave.

3.  **Command Propagation:**
    - The master sends all write commands to its connected slaves.
    - The master's replication offset is updated with each command.

4.  **Acknowledgements:**
    - Slaves acknowledge their processed offset to the master.
    - The `WAIT` command can be used to ensure that a certain number of slaves have processed a write.

### Example

1.  **Start the master:**
    ```sh
    ./lazydb --port 6379
    ```

2.  **Start the slave:**
    ```sh
    ./lazydb --port 6380 --replicaof localhost 6379
    ```

3.  **Connect to the master and set a key:**
    ```sh
    redis-cli -p 6379
    127.0.0.1:6379> SET mykey myvalue
    OK
    ```

4.  **Connect to the slave and get the key:**
    ```sh
    redis-cli -p 6380
    127.0.0.1:6380> GET mykey
    "myvalue"
    ```
