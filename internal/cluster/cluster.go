package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"time"

	"gokv/internal/command"
	"gokv/internal/context/config"
	"gokv/internal/context/environment"
	"gokv/internal/hashmap"
	"gokv/internal/hashring"
	"gokv/internal/models/peer"
	"gokv/internal/pool"
	"gokv/internal/response"
	"gokv/proto/clusterpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ClusterManager manages the cluster state, including node information, peer list, data store, and hash ring.
type ClusterManager struct {
	Mu              sync.RWMutex
	NodeID          string                   // ID of the current node.
	NodeAddr        string                   // Address of the current node.
	PeerMap         map[string]*peer.Peer    // Map of nodes in the cluster.
	HashRing        *hashring.HashRing       // Consistent hashing implementation.
	ConnPool        *pool.GrpcConnectionPool // Connection pool for gRPC clients.
	HashMap         *hashmap.HashMap         // In-memory data store.
	CommandRegistry *command.CommandRegistry // Registry for supported commands.
}

// NewClusterManager creates and initializes a new ClusterManager.
func NewClusterManager(env *environment.Environment, cfg *config.Config) *ClusterManager {
	cmdRegistry := command.NewCommandRegistry()
	hashMap := hashmap.NewHashMap(cmdRegistry, cfg.CleanupInterval)
	peerMap := make(map[string]*peer.Peer)
	hashRing := hashring.New(cfg.VNodeCount, cfg.Replicas, nil)
	connPool := pool.NewGrpcConnectionPool(func(address string) (*grpc.ClientConn, error) {
		var err error
		creds := insecure.NewCredentials()
		if env.TlsCertPath != "" {
			slog.Debug("cluster manager: attempting to start with TLS")
			creds, err = credentials.NewClientTLSFromFile(env.TlsCertPath, "")
			if err != nil {
				slog.Error(fmt.Sprintf("cluster manager: client failed to load TLS credentials: %v", err))
				os.Exit(1)
			}
		}
		return grpc.NewClient(address,
			grpc.WithTransportCredentials(creds),
			grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: cfg.MessageTimeout}),
		)
	})

	cm := &ClusterManager{
		NodeID:          env.NodeID,
		NodeAddr:        env.Host + ":" + env.Port,
		PeerMap:         peerMap,
		HashRing:        hashRing,
		ConnPool:        connPool,
		HashMap:         hashMap,
		CommandRegistry: cmdRegistry,
	}

	cm.HashRing.Add(cm.NodeID)
	slog.Debug(fmt.Sprintf("cluster manager: created cluster manager for node: %s", cm.NodeID))
	return cm
}

// AddNode adds a new node to the cluster.
// It establishes a connection and, if successful, adds the node to the peer list and hash ring.
func (cm *ClusterManager) AddNode(nodeID string, addr string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	// Don't add self.
	if addr == cm.NodeAddr {
		return
	}

	localNode := cm.PeerMap[nodeID]

	if localNode == nil {
		localNode = &peer.Peer{NodeID: nodeID, NodeAddr: addr, Alive: true, LastSeen: time.Now()}
	}

	_, err := cm.ConnPool.Get(addr)

	if err != nil {
		slog.Warn(fmt.Sprintf("cluster manager: failed to connect to node %s: %v", nodeID, err))
		localNode.Alive = false
	} else {
		slog.Info(fmt.Sprintf("cluster manager: added new peer to cluster: %s", nodeID))
		localNode.Alive = true
		cm.HashRing.Add(nodeID)
	}

	cm.PeerMap[nodeID] = localNode
}

// RemoveNode removes a node from the cluster.
// It closes the connection, removes the node from the hash ring, and marks it as not alive.
func (cm *ClusterManager) RemoveNode(nodeID string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if peer, ok := cm.PeerMap[nodeID]; ok {
		cm.ConnPool.Close(peer.NodeAddr)
		cm.PeerMap[nodeID].Alive = false
		cm.PeerMap[nodeID].LastSeen = time.Now()
		cm.HashRing.Remove(nodeID)
		slog.Warn(fmt.Sprintf("cluster manager: removed peer from cluster: %s", nodeID))
	}
}

// GetPeer returns a peer by its node ID.
func (cm *ClusterManager) GetPeer(nodeID string) (*peer.Peer, bool) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	peer, ok := cm.PeerMap[nodeID]
	return peer, ok
}

// GetPeerClient returns a gRPC client for a given peer ID.
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

// AlivePeers returns the number of alive peers in the cluster.
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

// GetRandomAlivePeers returns a slice of n random alive peers.
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

// StartHeartbeat starts the heartbeat process to periodically send heartbeats to other nodes.
func (cm *ClusterManager) StartHeartbeat(cfg *config.Config) {
	ticker := time.NewTicker(cfg.HeartbeatInterval) // Heartbeat interval.
	defer ticker.Stop()

	allPeers := cm.GetRandomAlivePeers(cm.AlivePeers())
	cm.Heartbeat(allPeers...)

	for range ticker.C {
		gossipTargets := cm.GetRandomAlivePeers(cfg.GossipPeerCount) // Number of peers to gossip with.
		old := cm.HashRing.Copy()
		cm.Heartbeat(gossipTargets...)
		new := cm.HashRing
		if new.GetVersion() != old.GetVersion() {
			go cm.Rebalance(old, new)
		}
	}
}

// Heartbeat sends a heartbeat to a list of peers to sync cluster state.
func (cm *ClusterManager) Heartbeat(peerList ...*peer.Peer) {
	if len(peerList) == 0 {
		slog.Debug("cluster manager: skipping heartbeat, no peers found")
		return
	}

	slog.Debug(fmt.Sprintf("cluster manager: sending heartbeat to %d peers", len(peerList)))
	for _, peerToCheck := range peerList {
		client, ok := cm.GetPeerClient(peerToCheck.NodeID)
		if !ok {
			slog.Warn(fmt.Sprintf("cluster manager: heartbeat failed for peer %s, client not found, removing from cluster", peerToCheck.NodeID))
			cm.RemoveNode(peerToCheck.NodeID)
			continue
		}

		cm.Mu.RLock()
		self := &clusterpb.Node{NodeId: cm.NodeID, NodeAddr: cm.NodeAddr, Alive: true, LastSeen: time.Now().Unix()}
		peerspb := make([]*clusterpb.Node, 0, len(cm.PeerMap)+1)
		peerspb = append(peerspb, self)
		for _, peerToAdd := range cm.PeerMap {
			peerspb = append(peerspb, peer.ToProto(*peerToAdd))
		}
		cm.Mu.RUnlock()

		req := &clusterpb.HeartbeatRequest{Peers: peerspb}

		res, err := client.Heartbeat(context.Background(), req)
		if err != nil {
			slog.Warn(fmt.Sprintf("cluster manager: heartbeat failed for peer %s, removing from cluster: %v", peerToCheck.NodeID, err))
			cm.RemoveNode(peerToCheck.NodeID)
		} else {
			slog.Debug(fmt.Sprintf("cluster manager: heartbeat check successful for peer %s", peerToCheck.NodeID))
			cm.MergeState(res.Peers)
		}
	}
}

// MergeState merges the state of a slice of peers with the cluster state.
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

func (cm *ClusterManager) getRebalanceTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs []string) []string {
	oldSet := make(map[string]struct{}, len(oldResponsibleNodeIDs))
	for _, id := range oldResponsibleNodeIDs {
		oldSet[id] = struct{}{}
	}

	// Collect IDs that are in newIDs but not in oldSet
	var diff []string
	for _, id := range newResponsibleNodeIDs {
		if _, exists := oldSet[id]; !exists {
			diff = append(diff, id)
		}
	}

	return diff
}

func (cm *ClusterManager) findRebalanceLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs []string) string {
	// The original leader is the preferred migrator.
	potentialLeader := oldResponsibleNodeIDs[0]
	if potentialLeader == cm.NodeID {
		return potentialLeader
	}

	// If the original leader is not alive, find the first alive replica from the old set.
	for _, replicaID := range oldResponsibleNodeIDs[1:] {
		if replica, ok := cm.GetPeer(replicaID); ok && replica.Alive {
			return replica.NodeID
		}
	}

	// If no old nodes are available, the new leader will have to handle it (though it won't have the data).
	return newResponsibleNodeIDs[0]
}

func (cm *ClusterManager) createMigrationCommands(hash string, he *hashmap.HashEntry, targetIDs []string) map[string][]*clusterpb.CommandRequest {
	commandsByNode := make(map[string][]*clusterpb.CommandRequest)
	he.Mu.RLock()
	defer he.Mu.RUnlock()

	for key, entry := range he.Items {
		ttl := int64(0)
		if entry.ExpiresAt > 0 {
			ttl = entry.ExpiresAt - time.Now().Unix()
		}

		args := make([][]byte, 0)
		args = append(args, []byte(key))
		args = append(args, entry.Data)
		args = append(args, fmt.Appendf(nil, "%d", ttl))

		req := &clusterpb.CommandRequest{
			Command: "HSET",
			Key:     hash,
			Args:    args,
		}

		for _, nodeID := range targetIDs {
			if nodeID != cm.NodeID {
				commandsByNode[nodeID] = append(commandsByNode[nodeID], req)
			}
		}
	}

	return commandsByNode
}

func (cm *ClusterManager) streamMigrationCommands(nodeID string, commands []*clusterpb.CommandRequest) {
	slog.Info(fmt.Sprintf("cluster manager: rebalancing %d keys to node %s", len(commands), nodeID))
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, client not found", nodeID))
		return
	}

	md := metadata.Pairs("replicate", "false")
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	stream, err := client.StreamCommand(ctx)
	if err != nil {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, stream error: %v", nodeID, err))
		return
	}

	for _, command := range commands {
		slog.Debug(fmt.Sprintf("cluster manager: rebalancing %v to node %s", command.Key, nodeID))
		if err := stream.Send(command); err != nil {
			slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, send error: %v", nodeID, err))
			continue
		}
	}

	if err := stream.CloseSend(); err != nil {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance failed for peer %s, close error: %v", nodeID, err))
	}
}

// Rebalance redistributes keys across the cluster when a new node is added.
func (cm *ClusterManager) Rebalance(oldRing, newRing *hashring.HashRing) {
	slog.Debug("cluster manager: rebalancing cluster")

	commandsByNode := make(map[string][]*clusterpb.CommandRequest)
	deleteList := make([]string, 0)

	cm.HashMap.ScanHash(func(hash string, he *hashmap.HashEntry) {
		oldResponsibleNodeIDs := oldRing.Get(hash)
		newResponsibleNodeIDs := newRing.Get(hash)

		targetIDs := cm.getRebalanceTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs)
		if len(targetIDs) == 0 {
			return
		}

		rebalanceLeader := cm.findRebalanceLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs)

		if rebalanceLeader == cm.NodeID {
			migrationCommands := cm.createMigrationCommands(hash, he, targetIDs)
			for nodeID, commands := range migrationCommands {
				commandsByNode[nodeID] = append(commandsByNode[nodeID], commands...)
			}
		}

		if !slices.Contains(newResponsibleNodeIDs, cm.NodeID) {
			deleteList = append(deleteList, hash)
		}
	})

	for nodeID, commands := range commandsByNode {
		cm.streamMigrationCommands(nodeID, commands)
	}

	for _, hash := range deleteList {
		cm.HashMap.HDel(hash)
	}

	slog.Debug("cluster manager: rebalancing finished")
}

// RunCommand executes a command, either locally (then replicate if applicable) or by forwarding it to a responsible node.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *clusterpb.CommandRequest, replicate bool) (any, error) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	slog.Debug(fmt.Sprintf("cluster manager: run command responsible nodes: %v", responsibleNodeIDs))
	isPrimary := responsibleNodeIDs[0] == cm.NodeID
	isReplicaReq := !replicate && slices.Contains(responsibleNodeIDs[1:], cm.NodeID)
	if isPrimary || isReplicaReq {
		// Execute the command locally.
		cmd, ok := cm.CommandRegistry.Get(req.Command)
		if !ok {
			return nil, fmt.Errorf("cluster manager: unknown command: %s", req.Command)
		}
		slog.Debug(fmt.Sprintf("cluster manager: run command %s locally for key: %v", req.Command, req.Key))
		res, err := cmd.Run(req.Key, req.Args...)
		if !isReplicaReq && cmd.Level == command.Replica {
			go cm.ReplicateCommand(req)
		}
		return res, err
	} else {
		// Forward the command to a responsible node.
		responsibleNodeID := responsibleNodeIDs[0]
		client, ok := cm.GetPeerClient(responsibleNodeID)
		if !ok {
			slog.Warn(fmt.Sprintf("cluster manager: failed to get client for node %s", responsibleNodeID))
			cm.RemoveNode(responsibleNodeID)
			slog.Debug("cluster manager: attempting to run command again")
			return cm.RunCommand(ctx, req, replicate)
		}
		slog.Debug(fmt.Sprintf("cluster manager: forwarding command %s to node %v for key: %v", req.Command, responsibleNodeID, req.Key))
		res, err := client.RunCommand(ctx, req)
		if err != nil {
			slog.Warn(fmt.Sprintf("cluster manager: failed to forward command to node %s: %v", responsibleNodeID, err))
			cm.RemoveNode(responsibleNodeID)
			slog.Debug("cluster manager: attempting to run command again")
			return cm.RunCommand(ctx, req, replicate)
		}
		return response.Unmarshal(res)
	}
}

// Replicate command to replicas
func (cm *ClusterManager) ReplicateCommand(req *clusterpb.CommandRequest) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	for _, nodeID := range responsibleNodeIDs[1:] {
		client, ok := cm.GetPeerClient(nodeID)
		if !ok {
			slog.Warn(fmt.Sprintf("cluster manager: error replicating command to node %s: no client found", nodeID))
			cm.RemoveNode(nodeID)
			continue
		}
		md := metadata.Pairs("replicate", "false")
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		slog.Debug(fmt.Sprintf("cluster manager: command %s for key %s replicated to node %s", req.Command, req.Key, nodeID))
		_, err := client.RunCommand(ctx, req)
		if err != nil {
			slog.Warn(fmt.Sprintf("cluster manager: error replicating command to node %s: %v", nodeID, err))
			cm.RemoveNode(nodeID)
			continue
		}
	}
}
