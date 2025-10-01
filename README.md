# GOKV

Gokv is a distributed in-memory key-value store written in Go. It is designed to be a simple, highly available, and scalable solution for caching data.

## Index

*   [Quickstart](#quickstart)
*   [Configuration](#configuration)
*   [gRPC API](#grpc-api)
*   [Architecture](#architecture)

## Quickstart

### Prerequisites

*   Docker
*   Docker Compose

### Running the Cluster

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/emilio/gokv.git
    cd gokv
    ```

2.  **Start the cluster:**

    To start a cluster with 3 nodes, run the following command:

    ```bash
    docker-compose up -d
    ```

    This will start three `gokv` containers, each running a `gokv` node. The nodes will automatically discover each other and form a cluster.

### Interacting with the cluster

You can interact with the cluster by connecting to any of the nodes. For example, to connect to the first node and set a key, you can use a gRPC client or the CLI tool.

#### CLI Tool

A command-line interface (CLI) tool is available for interacting with the cluster. You can find the source code in `cmd/gokv-cli`.

**Usage:**

```bash
./gokv-cli <commnad> [...arguments]
```

**Example:**

```bash
./gokv-cli status
./gokv-cli run HSET hash key1 value1 key2 value2 
```

**GOKV Available Commands:**

### General Commands

*   `DEL <key>`: Deletes a key.
*   `EXPIRE <key> <ttl>`: Sets an expiration time on a key. `ttl` is a duration (e.g., `10s`, `1m`).
*   `SCAN <cursor>`: Scans all keys in the cluster.

### Hash Commands

*   `HGET <key> <field...>`: Get one or more fields in a hash.
*   `HSET <key> <field> <value>...`: Set one or more fields in a hash.
*   `HDEL <key> <field...>`: Delete one or more fields from a hash.
*   `HKEYS <key>`: Get all the fields in a hash.

### List Commands

*   `LPUSH <key> <value...>`: Prepend one or more values to a list.
*   `LPOP <key> <count>`: Remove and get the first `count` elements from a list.
*   `RPUSH <key> <value...>`: Append one or more values to a list.
*   `RPOP <key> <count>`: Remove and get the last `count` elements from a list.
*   `LLEN <key>`: Get the length of a list.

### Running with TLS (Optional)

To run the cluster with TLS encryption, you need to generate certificates and configure the nodes to use them.

1.  **Generate certificates:**

    Run the following script to generate the necessary TLS certificates:

    ```bash
    ./certs.sh
    ```

    This will create a `certs` directory containing the CA, internal server, internal client, and external server certificates and keys.

2.  **Configure TLS:**

    You can configure the nodes to use TLS by setting the following environment variables:

    *   `GOKV_INTERNAL_TLS_CA_PATH`: Path to the Certificate Authority certificate file for internal gRPC.
    *   `GOKV_INTERNAL_TLS_SERVER_CERT_PATH`: Path to the TLS certificate for internal communication.
    *   `GOKV_INTERNAL_TLS_SERVER_KEY_PATH`: Path to the TLS key for internal communication.
    *   `GOKV_INTERNAL_TLS_CLIENT_CERT_PATH`: Path to the TLS certificate for internal communication.
    *   `GOKV_INTERNAL_TLS_CLIENT_KEY_PATH`: Path to the TLS key for internal communication.
    *   `GOKV_EXTERNAL_TLS_CA_PATH`: Path to the Certificate Authority certificate file for external gRPC.
    *   `GOKV_EXTERNAL_TLS_SERVER_CERT_PATH`: Path to the TLS certificate for external communication.
    *   `GOKV_EXTERNAL_TLS_SERVER_KEY_PATH`: Path to the TLS key for external communication.

    You can set these variables in a `.env` file or directly in the `docker-compose.yml` file.

## Data Structures

`gokv` supports the following data structures:

*   **Hashes**: A collection of field-value pairs.
*   **Lists**: A list of elements, ordered by insertion time.

## Configuration

### Environment Variables

The following env variables can be used to configure a `gokv` node:

#### General Env Variables

| Variable           | Description                         | Default      |
| ------------------ | ----------------------------------- | ------------ |
| `GOKV_LOG_LEVEL`   | Level of logs shown in the console. | `info`       |
| `GOKV_CONFIG_PATH` | Path to the configuration file.     | `config.yml` |

#### Cluster Env Variables

| Variable                      | Description                                               | Default               |
| ----------------------------- | --------------------------------------------------------- | --------------------- |
| `GOKV_CLUSTER_NODE_ID`        | A unique identifier for the node.                         |                       |
| `GOKV_CLUSTER_BIND_ADDR`      | The address to bind the internal gRPC server to.          | `0.0.0.0:50000`       |
| `GOKV_CLUSTER_ADVERTISE_ADDR` | The address to advertise to other nodes in the cluster.   | `localhost:50000`     |
| `GOKV_CLUSTER_SEEDS`          | A comma-separated list of seed nodes to join the cluster. |                       |

#### Internal TLS Env Variables

| Variable                           | Description                                                              | Default |
| ---------------------------------- | ------------------------------------------------------------------------ | ------- |
| `GOKV_INTERNAL_TLS_CA_PATH`        | Certificate Authority certificate path for internal gRPC.                |         |
| `GOKV_INTERNAL_TLS_SERVER_CERT_PATH` | TLS certificate for internal communication.                            |         |
| `GOKV_INTERNAL_TLS_SERVER_KEY_PATH`  | TLS key for internal communication.                                    |         |
| `GOKV_INTERNAL_TLS_CLIENT_CERT_PATH` | TLS certificate for internal communication.                            |         |
| `GOKV_INTERNAL_TLS_CLIENT_KEY_PATH`  | TLS key for internal communication.                                    |         |

#### External TLS Env Variables

| Variable                             | Description                                               | Default |
| ------------------------------------ | --------------------------------------------------------- | ------- |
| `GOKV_EXTERNAL_TLS_CA_PATH`          | Certificate Authority certificate path for external gRPC. |         |
| `GOKV_EXTERNAL_TLS_SERVER_CERT_PATH` | TLS certificate for internal communication.               |         |
| `GOKV_EXTERNAL_TLS_SERVER_KEY_PATH`  | TLS key for internal communication.                       |         |

#### External API Env Variables

| Variable                          | Description                                       | Default           |
| --------------------------------- | ------------------------------------------------- | ----------------- |
| `GOKV_EXTERNAL_GRPC_BIND_ADDR`    | The address to bind the external gRPC server to.  | `0.0.0.0:50051`   |
| `GOKV_EXTERNAL_GRPC_ADVERTISE_ADDR` | The address to advertise to clients.            | `localhost:50051` |
| `GOKV_EXTERNAL_REST_BIND_ADDR`    | The address to bind the REST API to.              | `0.0.0.0:8080`    |
| `GOKV_EXTERNAL_REST_ADVERTISE_ADDR` | The address to advertise to clients.            | `localhost:8080`  |

#### Client Env Variables

| Variable          | Description                                    | Default             |
| ----------------- | ---------------------------------------------- | ------------------- |
| `GOKV_CLIENT_URI` | The URI of the gokv cluster to connect to.     | `localhost:50051`   |
| `GOKV_CLIENT_CA`  | The CA for TLS.                                |                     |

### Cluster Configuration

The following config variables can be used to configure a `gokv` cluster:

| Variable            | Description                                     |  Default  |
| ------------------- | ----------------------------------------------- |  -------  |
| `cleanup_interval`  | Interval for cleaning up expired key            | `10s`     |
| `heartbeat_interval`| Interval for gossip heartbeats.                 | `5s`      |
| `gossip_peer_count` | Number of peers to gossip to.                   | `2`       |
| `v_node_count`      | Number of virtual nodes in the hashring.        | `3`       |
| `message_timeout`   | Timeout of grpc messages.                       | `5s`      |
| `replicas`          | Number of replicas in the cluster.              | `2`       |

## gRPC API

The `gokv` nodes communicate with each other using an internal gRPC API, while clients can interact with the cluster using a separate external gRPC API.

### Internal API

The internal gRPC API is used for communication between nodes in the cluster. The following services are available:

*   `InternalServer`:
    *   `Heartbeat`: Used by the nodes to exchange cluster state information.
    *   `ForwardCommand`: Forwards a command to another node in the cluster.
    *   `Rebalance`: Used for streaming commands to rebalance the cluster.

### External API

The external gRPC API is used for communication with clients. The following services are available:

*   `ExternalServer`:
    *   `Healthcheck`: Returns the status of the cluster.
    *   `RunCommand`: Runs a command in the cluster. It manages forwarding and replication.
    *   `StreamCommand`: Used for streaming commands.

To regenerate the proto buffer files, run the following command:

```bash
protoc --proto_path=proto \
  --go_out=proto --go_opt=paths=source_relative \
  --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
  commonpb/common.proto \
  internalpb/internal.proto \
  externalpb/external.proto

```

## Architecture

### Consistent Hashing

`gokv` uses a consistent hashing algorithm to distribute keys across the nodes in the cluster. Each node is assigned a number of virtual nodes, which are placed on a hash ring. When a key is stored, it is hashed to a point on the ring, and the key is stored on the node that is closest to that point in the clockwise direction.

This approach ensures that when a node is added or removed from the cluster, only a small number of keys need to be moved to a different node.

### Gossip Protocol

The nodes in the cluster use a gossip protocol to maintain a consistent view of the cluster state. Each node periodically sends a heartbeat message to a random selection of other nodes. The heartbeat message contains the sending node's view of the cluster, including the list of nodes and their status.

When a node receives a heartbeat message, it merges the received information with its own view of the cluster. This process ensures that all nodes eventually converge on the same view of the cluster.

### Data Replication

Write commands (e.g. `HSET`, `HDEL`) are replicated to other nodes (n non virtual nodes after the leader in the ring) in the cluster to ensure data durability. When a node receives a write command, it first applies the command to its own data store and then forwards the command to the other replicas. Read commands (e.g. `HGET`) can be sent to any node, they will be redirected to the leader.

### Data Rebalancing

When the cluster state changes each node will trigger a rebalance. As a result, some keys that were previously assigned to other nodes are now assigned to the new node. A node from the previous responsible nodes will be selected for rebalancing the keys, keys that no longer belong to the current node will be deleted once the rebalance is completed. This is done using the `Rebalance` gRPC internal endpoint.
