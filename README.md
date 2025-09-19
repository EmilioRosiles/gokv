### GOKV

Gokv is a distributed in-memory cache for storing and retrieving data. It is written in Go and uses a consistent hashing algorithm to distribute the data across the nodes in the cluster. The nodes use a gossip protocol to communicate with each other and keep the cluster state in sync.


#### gRPC API

*   `Heartbeat`: Used by the nodes to exchange cluster state information.
*   `RunCommand`: Runs a command in the cluster.
