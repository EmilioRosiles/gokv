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
	"gokv/internal/tls"
	"gokv/proto/commonpb"
	"gokv/proto/internalpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterManager manages the cluster state, including node information, peer list, data store, and hash ring.
type ClusterManager struct {
	Mu                 sync.RWMutex
	NodeID             string                   // ID of the current node.
	NodeInternalAddr   string                   // Address of the current node.
	NodeExternalAddr   string                   //
	PeerMap            map[string]*peer.Peer    // Map of nodes in the cluster.
	HashRing           *hashring.HashRing       // Consistent hashing implementation.
	ConnPool           *pool.GrpcConnectionPool // Connection pool for gRPC clients.
	HashMap            *hashmap.HashMap         // In-memory data store.
	CommandRegistry    *command.CommandRegistry // Registry for supported commands.
	LastRebalancedRing *hashring.HashRing       // Ring used for the last rebalance (avoids multiple reblance calls)
}

// NewClusterManager creates and initializes a new ClusterManager.
func NewClusterManager(env *environment.Environment, cfg *config.Config) *ClusterManager {
	cmdRegistry := command.NewCommandRegistry()
	hashMap := hashmap.NewHashMap(cmdRegistry, cfg.CleanupInterval)
	peerMap := make(map[string]*peer.Peer)
	hashRing := hashring.New(cfg.VNodeCount, cfg.Replicas, nil)
	connPool := pool.NewGrpcConnectionPool(func(address string) (*grpc.ClientConn, error) {
		creds := insecure.NewCredentials()
		if env.InternalTlsClientCertPath != "" || env.InternalTlsClientKeyPath != "" {
			slog.Debug("cluster manager: attempting to start client with TLS")
			tlsCfg, err := tls.BuildClientTLSConfig(
				env.InternalTlsCAPath,
				env.InternalTlsClientCertPath,
				env.InternalTlsClientKeyPath,
				env.AdvertiseAddr,
			)
			if err != nil {
				slog.Error(fmt.Sprintf("cluster manager: client failed to load TLS credentials: %v", err))
				os.Exit(1)
			}
			creds = credentials.NewTLS(tlsCfg)
		}

		return grpc.NewClient(address,
			grpc.WithTransportCredentials(creds),
			grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: cfg.MessageTimeout}),
		)
	})

	cm := &ClusterManager{
		NodeID:           env.NodeID,
		NodeInternalAddr: env.AdvertiseAddr,
		NodeExternalAddr: env.ExternalGrpcAdvertiseAddr,
		PeerMap:          peerMap,
		HashRing:         hashRing,
		ConnPool:         connPool,
		HashMap:          hashMap,
		CommandRegistry:  cmdRegistry,
	}

	cm.HashRing.Add(cm.NodeID)
	cm.LastRebalancedRing = cm.HashRing.Copy()
	slog.Debug(fmt.Sprintf("cluster manager: created cluster manager for node: %s", cm.NodeID))
	return cm
}

// AddNode adds a new node to the cluster.
// It establishes a connection and, if successful, adds the node to the peer list and hash ring.
func (cm *ClusterManager) AddNode(nodeID, internalAddr, externalAddr string) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	// Don't add self.
	if internalAddr == cm.NodeInternalAddr {
		return
	}

	localNode := cm.PeerMap[nodeID]

	if localNode == nil {
		localNode = &peer.Peer{
			NodeID:           nodeID,
			NodeInternalAddr: internalAddr,
			NodeExternalAddr: externalAddr,
			Alive:            true,
			LastSeen:         time.Now(),
		}
	}

	_, err := cm.ConnPool.Get(internalAddr)

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
		cm.ConnPool.Close(peer.NodeInternalAddr)
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
func (cm *ClusterManager) GetPeerClient(nodeID string) (internalpb.InternalServerClient, bool) {
	peer, ok := cm.GetPeer(nodeID)
	if !ok {
		return nil, false
	}

	conn, err := cm.ConnPool.Get(peer.NodeInternalAddr)
	if err != nil {
		return nil, false
	}

	return internalpb.NewInternalServerClient(conn), true
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

	for range ticker.C {
		gossipTargets := cm.GetRandomAlivePeers(cfg.GossipPeerCount) // Number of peers to gossip with.
		cm.Heartbeat(gossipTargets...)
		if cm.LastRebalancedRing.GetVersion() != cm.HashRing.GetVersion() {
			go cm.Rebalance(cm.LastRebalancedRing, cm.HashRing)
			cm.Mu.Lock()
			cm.LastRebalancedRing = cm.HashRing.Copy()
			cm.Mu.Unlock()
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
		self := &internalpb.HeartbeatNode{
			NodeId:           cm.NodeID,
			NodeInternalAddr: cm.NodeInternalAddr,
			NodeExternalAddr: cm.NodeExternalAddr,
			Alive:            true,
			LastSeen:         time.Now().Unix(),
		}
		peerspb := make([]*internalpb.HeartbeatNode, 0, len(cm.PeerMap)+1)
		peerspb = append(peerspb, self)
		for _, peerToAdd := range cm.PeerMap {
			peerspb = append(peerspb, peer.ToHeartbeatNodeProto(*peerToAdd))
		}
		cm.Mu.RUnlock()

		req := &internalpb.HeartbeatRequest{Peers: peerspb}

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
func (cm *ClusterManager) MergeState(nodes []*internalpb.HeartbeatNode) {
	for _, remoteNode := range nodes {

		if remoteNode.NodeId == cm.NodeID {
			continue
		}

		localNode, exists := cm.GetPeer(remoteNode.NodeId)

		if !exists {
			cm.AddNode(remoteNode.NodeId, remoteNode.NodeInternalAddr, remoteNode.NodeExternalAddr)
			if peer, ok := cm.GetPeer(remoteNode.NodeId); ok {
				cm.Mu.Lock()
				peer.Alive = remoteNode.Alive
				peer.LastSeen = time.Unix(remoteNode.LastSeen, 0)
				cm.Mu.Unlock()
			}
			continue
		}

		if remoteNode.LastSeen > localNode.LastSeen.Unix() {
			cm.Mu.Lock()
			localNode.LastSeen = time.Unix(remoteNode.LastSeen, 0)
			localNode.NodeExternalAddr = remoteNode.NodeExternalAddr
			localNode.NodeInternalAddr = remoteNode.NodeInternalAddr
			cm.Mu.Unlock()

			if !remoteNode.Alive && localNode.Alive {
				cm.RemoveNode(localNode.NodeID)
			}

			if remoteNode.Alive && !localNode.Alive {
				cm.AddNode(localNode.NodeID, localNode.NodeInternalAddr, localNode.NodeExternalAddr)
			}
		}
	}
}

// Finds migration targets by comparing the old and new responsible nodes
func (cm *ClusterManager) getMigrationTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs []string) []string {
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

// Finds migration leader to avoid unnecessary network traffic
func (cm *ClusterManager) findMigrationLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs []string) string {
	// The original leader is the preferred migrator.
	oldLeader := oldResponsibleNodeIDs[0]
	if leader, ok := cm.GetPeer(oldLeader); oldLeader == cm.NodeID || (ok && leader.Alive) {
		return oldLeader
	}

	// If the original leader is not alive, find the first alive replica from the old set.
	for _, replicaID := range oldResponsibleNodeIDs[1:] {
		if replica, ok := cm.GetPeer(replicaID); replicaID == cm.NodeID || (ok && replica.Alive) {
			return replicaID
		}
	}

	// If no old nodes are available, the new leader will have to handle it (though it won't have the data).
	return newResponsibleNodeIDs[0]
}

// Creates migration commands for HSET and groups them by target node.
func (cm *ClusterManager) createMigrationCommands(hash string, he *hashmap.HashEntry, targetIDs []string) map[string][]*commonpb.CommandRequest {
	commandsByNode := make(map[string][]*commonpb.CommandRequest)
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

		req := &commonpb.CommandRequest{
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

// Batch streams commands to new owners
func (cm *ClusterManager) streamMigrationCommands(nodeID string, commands []*commonpb.CommandRequest) {
	slog.Info(fmt.Sprintf("cluster manager: rebalancing %d keys to node %s", len(commands), nodeID))
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, client not found", nodeID))
		return
	}

	ctx := context.Background()
	stream, err := client.Rebalance(ctx)
	if err != nil {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, stream error: %v", nodeID, err))
		return
	}

	batchSize := 100
	for i := 0; i < len(commands); i += batchSize {
		end := min(i+batchSize, len(commands))
		batch := &internalpb.RebalanceRequest{Commands: commands[i:end]}
		if err := stream.Send(batch); err != nil {
			slog.Warn(fmt.Sprintf("cluster manager: rebalance command failed for peer %s, send error: %v", nodeID, err))
			continue
		}
	}

	if err := stream.CloseSend(); err != nil {
		slog.Warn(fmt.Sprintf("cluster manager: rebalance failed for peer %s, close error: %v", nodeID, err))
	}
}

// Rebalance redistributes keys across the cluster when the state changes.
func (cm *ClusterManager) Rebalance(oldRing, newRing *hashring.HashRing) {
	slog.Debug("cluster manager: rebalancing cluster")

	commandsByNode := make(map[string][]*commonpb.CommandRequest)
	deleteList := make([]string, 0)

	cm.HashMap.ScanHash(func(hash string, he *hashmap.HashEntry) {
		oldResponsibleNodeIDs := oldRing.Get(hash)
		newResponsibleNodeIDs := newRing.Get(hash)

		targetIDs := cm.getMigrationTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs)
		if len(targetIDs) == 0 {
			return
		}

		rebalanceLeader := cm.findMigrationLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs)

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
		if peer, ok := cm.GetPeer(nodeID); ok {
			cm.Heartbeat(peer)
			cm.streamMigrationCommands(nodeID, commands)
		}
	}

	for _, hash := range deleteList {
		cm.HashMap.HDel(hash)
	}

	slog.Debug("cluster manager: rebalancing finished")
}

// Runs a command locally, does not check for responsibility.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (any, error) {
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", req.Command)
	}
	slog.Debug(fmt.Sprintf("cluster manager: running local command: %s %s", req.Command, req.Key))
	return cmd.Run(req.Key, req.Args...)
}

// Forwards command to a replica, forwarded comands only attempt to run locally and do not trigger replication.
func (cm *ClusterManager) ForwardCommand(ctx context.Context, req *commonpb.CommandRequest, nodeID string) (any, error) {
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		return nil, fmt.Errorf("cluster manager: failed to get client for node %s", nodeID)
	}
	slog.Debug(fmt.Sprintf("cluster manager: forwarding to node %s command: %s %s", nodeID, req.Command, req.Key))
	res, err := client.ForwardCommand(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cluster manager: failed to forwarding command to node %s: %v", nodeID, err)
	}
	return response.Unmarshal(res)
}

// Sync command with replicas using forward command.
func (cm *ClusterManager) ReplicateCommand(req *commonpb.CommandRequest) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	ctx := context.Background()
	slog.Debug(fmt.Sprintf("cluster manager: replicating to nodes %v commnad: %s %s", responsibleNodeIDs, req.Command, req.Key))
	for _, replicaID := range responsibleNodeIDs[1:] {
		if replicaID == cm.NodeID {
			cm.RunCommand(ctx, req)
		} else {
			slog.Debug(fmt.Sprintf("cluster manager: replicating to node %s commnad: %s %s", replicaID, req.Command, req.Key))
			_, err := cm.ForwardCommand(ctx, req, replicaID)
			if err != nil {
				slog.Warn(fmt.Sprintf("cluster manager: error replicating to node %v commnad: %s %s", replicaID, req.Command, req.Key))
				continue
			}
		}
	}
}
