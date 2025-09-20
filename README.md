# GOKV

Gokv is a distributed in-memory key-value store written in Go. It is designed to be a simple, highly available, and scalable solution for caching data.

## Features

*   **Distributed**: Data is distributed across multiple nodes in the cluster.
*   **Consistent Hashing**: Uses a consistent hashing algorithm to distribute data, ensuring minimal data movement when nodes are added or removed.
*   **Peer-to-Peer Communication**: Nodes communicate with each other using gRPC for high-performance communication.
*   **Gossip Protocol**: Nodes use a gossip protocol to exchange cluster state information, ensuring that all nodes have a consistent view of the cluster.
*   **Command API**: Provides a simple command-based API for storing and retrieving data.
*   **Scalability**: New nodes can be added to the cluster to increase capacity and availability.
*   **Data Rebalancing**: When a new node joins the cluster, data is automatically rebalanced to ensure even distribution.

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

3.  **Interacting with the cluster:**

    You can interact with the cluster by connecting to any of the nodes. For example, to connect to the first node and set a key, you can use a gRPC client.

### Running with TLS (Optional)

To run the cluster with TLS encryption, you need to generate certificates and configure the nodes to use them.

1.  **Generate certificates:**

    Run the following script to generate the necessary TLS certificates:

    ```bash
    ./certs.sh
    ```

    This will create a `certs` directory containing the server certificate and key.

2.  **Configure TLS:**

    You can configure the nodes to use TLS by setting the following environment variables:

    *   `TLS_CERT_PATH`: Path to the server certificate file (e.g., `certs/server.crt`).
    *   `TLS_KEY_PATH`: Path to the server key file (e.g., `certs/server.key`).

    You can set these variables in a `.env` file or directly in the `docker-compose.yml` file.

## Configuration

The following env variables can be used to configure a `gokv` node:

| Variable         | Description                                     |  Default  |
| ---------------- | ----------------------------------------------- |  -------  |
| `NODE_ID`        | A unique identifier for the node.               |           |
| `HOST`           | The hostname or IP address of the node.         | `0.0.0.0` |
| `PORT`           | The port to listen on for gRPC connections.     | `8080`    |
| `SEED_NODE_ID`   | The ID of a seed node to connect to.            |           |
| `SEED_NODE_ADDR` | The address of a seed node to connect to.       |           |
| `TLS_CERT_PATH`  | Path to the TLS certificate file.               |           |
| `TLS_KEY_PATH`   | Path to the TLS key file.                       |           |

The following config variables can be used to configure a `gokv` cluster:

| Variable            | Description                                     |  Default  |
| ------------------- | ----------------------------------------------- |  -------  |
| `cleanup_interval`  | Interval for cleaning up expired key            | `10s`     |
| `heartbeat_interval`| Interval for gossip heartbeats.                 | `5s`      |
| `gossip_peer_count` | Number of peers to gossip to.                   | `2`       |
| `v_node_count`      | Number of virtual nodes in the hashring.        | `3`       |
| `message_timeout`   | Timeout of grpc messages.                       | `5s`      |

## gRPC API

The `gokv` nodes communicate with each other using a gRPC API. The following services are available:

*   `ClusterNode`:
    *   `Heartbeat`: Used by the nodes to exchange cluster state information.
    *   `RunCommand`: Runs a command in the cluster.
    *   `StreamCommand`: Used for streaming commands.

## Architecture

### Consistent Hashing

`gokv` uses a consistent hashing algorithm to distribute keys across the nodes in the cluster. Each node is assigned a number of virtual nodes, which are placed on a hash ring. When a key is stored, it is hashed to a point on the ring, and the key is stored on the node that is closest to that point in the clockwise direction.

This approach ensures that when a node is added or removed from the cluster, only a small number of keys need to be moved to a different node.

### Gossip Protocol

The nodes in the cluster use a gossip protocol to maintain a consistent view of the cluster state. Each node periodically sends a heartbeat message to a random selection of other nodes. The heartbeat message contains the sending node's view of the cluster, including the list of nodes and their status.

When a node receives a heartbeat message, it merges the received information with its own view of the cluster. This process ensures that all nodes eventually converge on the same view of the cluster.

### Data Rebalancing

When a new node joins the cluster, it needs to receive its share of the data from the other nodes. This process is called data rebalancing.

When a node joins, it is added to the hash ring. As a result, some keys that were previously assigned to other nodes are now assigned to the new node. The nodes that are no longer responsible for these keys will send them to the new node. This is done using the `StreamCommand` gRPC endpoint.