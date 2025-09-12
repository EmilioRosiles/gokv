package cluster

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"gokv/internal/cluster/hashring"
	"gokv/internal/cluster/peer"
	"gokv/internal/cluster/pool"
	clusterpb "gokv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterManager: Contains the node info, list of peers, the data store, and the hash ring.
type ClusterManager struct {
	Mu       sync.RWMutex
	NodeID   string                   // Id of current node
	NodeAddr string                   // Address of the current node
	PeerMap  map[string]*peer.Peer    // Map of nodes in the cluster
	HashRing *hashring.HashRing       // Consitent Hashing implementation
	ConnPool *pool.GrpcConnectionPool // Connection pool of nodes in the cluster
}

// NewClusterManager creates a new cluster manager.
func NewClusterManager(nodeID string, nodeAddress string, vNodeCount int) *ClusterManager {
	cm := &ClusterManager{
		NodeID:   nodeID,
		NodeAddr: nodeAddress,
		PeerMap:  make(map[string]*peer.Peer),
		HashRing: hashring.New(vNodeCount, nil),
		ConnPool: pool.NewGrpcConnectionPool(func(address string) (*grpc.ClientConn, error) {
			return grpc.NewClient(address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithConnectParams(grpc.ConnectParams{
					MinConnectTimeout: 5 * time.Second,
				}),
			)
		}),
	}

	cm.HashRing.Add(cm.NodeID)
	log.Printf("Created cluster manager")
	return cm
}

// AddNode connects to a new node and adds it to the peer list and hash ring if the connection succeeds.
func (cm *ClusterManager) AddNode(nodeID string, addr string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	// Don't add self
	if addr == cm.NodeAddr {
		return
	}

	localNode := cm.PeerMap[nodeID]

	if localNode == nil {
		localNode = &peer.Peer{NodeID: nodeID, NodeAddr: addr, Alive: true, LastSeen: time.Now()}
	}

	_, err := cm.ConnPool.Get(addr)

	if err != nil {
		log.Printf("Failed to connect to node %s: %v", nodeID, err)
		localNode.Alive = false
	} else {
		log.Printf("Added new peer to cluster: %s", nodeID)
		localNode.Alive = true
		cm.HashRing.Add(nodeID)
		go cm.Rebalance()
	}

	cm.PeerMap[nodeID] = localNode
}

// RemoveNode closes the connection, removes a node from hash ring, and sets alive prop to false.
func (cm *ClusterManager) RemoveNode(nodeID string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if peer, ok := cm.PeerMap[nodeID]; ok {
		cm.ConnPool.Close(peer.NodeAddr)
		cm.PeerMap[nodeID].Alive = false
		cm.HashRing.Remove(nodeID)
		log.Printf("Removed peer from cluster: %s", nodeID)
	}
}

// Gets a peer ref by nodeID
func (cm *ClusterManager) GetPeer(nodeID string) (*peer.Peer, bool) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	peer, ok := cm.PeerMap[nodeID]
	return peer, ok
}

// Gets a peer gRPC client by nodeID
func (cm *ClusterManager) GetPeerClient(nodeID string) (clusterpb.ClusterNodeClient, bool) {
	peer, ok := cm.GetPeer(nodeID)
	if !ok {
		return nil, false
	}

	conn, err := cm.ConnPool.Get(peer.NodeAddr)
	if err != nil {
		return nil, false
	}

	return clusterpb.NewClusterNodeClient(conn), true
}

// Gets the ID of the node responsible for a s2 cell
func (cm *ClusterManager) GetResponsibleNode(key string) string {
	return cm.HashRing.Get(key)
}

// Returns count of peers alive
func (cm *ClusterManager) AlivePeers() int {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	alivePeers := 0
	for _, peer := range cm.PeerMap {
		if peer.Alive {
			alivePeers++
		}
	}
	return alivePeers
}

// Gets slice of ramdom n alive peers
func (cm *ClusterManager) GetRandomAlivePeers(num int) []*peer.Peer {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	if num <= 0 {
		return nil
	}

	alivePeers := make([]*peer.Peer, 0, cm.AlivePeers())
	for _, peer := range cm.PeerMap {
		if peer.Alive {
			alivePeers = append(alivePeers, peer)
		}
	}

	rand.Shuffle(len(alivePeers), func(i, j int) {
		alivePeers[i], alivePeers[j] = alivePeers[j], alivePeers[i]
	})

	if num > len(alivePeers) {
		num = len(alivePeers)
	}

	return alivePeers[:num]
}

// Notifies cluster of precense and starts gossip protocol
func (cm *ClusterManager) StartHeartbeat() {
	ticker := time.NewTicker(5 * time.Second) // Set here Heartbeat interval
	defer ticker.Stop()

	allPeers := cm.GetRandomAlivePeers(cm.AlivePeers())
	cm.Heartbeat(allPeers...)

	for range ticker.C {
		gossipTargets := cm.GetRandomAlivePeers(2) // Set here Peer count for gossip protocol
		go cm.Heartbeat(gossipTargets...)
	}
}

// Sends a heartbeat to a list of peers to sync cluster state
func (cm *ClusterManager) Heartbeat(peerList ...*peer.Peer) {
	if len(peerList) == 0 {
		log.Printf("Skipping heartbeat: No peers found.")
		return
	}

	log.Printf("Sending heartbeat to %d peers...", len(peerList))
	for _, peerToCheck := range peerList {
		client, ok := cm.GetPeerClient(peerToCheck.NodeID)
		if !ok {
			log.Printf("Heartbeat failed for peer %s: Client not found. Removing from cluster.", peerToCheck.NodeID)
			cm.RemoveNode(peerToCheck.NodeID)
			continue

		}

		self := &clusterpb.Node{NodeId: cm.NodeID, NodeAddr: cm.NodeAddr, Alive: true, LastSeen: time.Now().Unix()}

		cm.Mu.RLock()
		peerspb := make([]*clusterpb.Node, 0, len(cm.PeerMap))
		for _, peerToAdd := range cm.PeerMap {
			peerspb = append(peerspb, peer.ToProto(*peerToAdd))
		}
		cm.Mu.RUnlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req := &clusterpb.HeartbeatRequest{
			Self:  self,
			Peers: peerspb,
		}

		res, err := client.Heartbeat(ctx, req)
		if err != nil {
			log.Printf("Heartbeat check failed for peer %s: %v. Removing from cluster.", peerToCheck.NodeID, err)
			cm.RemoveNode(peerToCheck.NodeID)
		} else {
			log.Printf("Heartbeat check successful for peer %s.", peerToCheck.NodeID)
			cm.MergeState(res.Peers)
		}
		cancel()
	}
}

// Merge state of a slice of peers with the cluster state
func (cm *ClusterManager) MergeState(nodes []*clusterpb.Node) {
	for _, remoteNode := range nodes {

		if remoteNode.NodeId == cm.NodeID {
			continue
		}

		localNode, exists := cm.GetPeer(remoteNode.NodeId)

		if !exists {
			cm.AddNode(remoteNode.NodeId, remoteNode.NodeAddr)
			continue
		}

		if remoteNode.LastSeen > localNode.LastSeen.Unix() {
			cm.Mu.Lock()
			localNode.LastSeen = time.Unix(remoteNode.LastSeen, 0)
			cm.Mu.Unlock()

			if !remoteNode.Alive && localNode.Alive {
				cm.RemoveNode(localNode.NodeID)
			}

			if remoteNode.Alive && !localNode.Alive {
				cm.AddNode(localNode.NodeID, localNode.NodeAddr)
			}
		}
	}
}

// Rebalances cluster keys when a new node is added
func (cm *ClusterManager) Rebalance() {
	// TODO: implement for each data structure
}
