package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"gokv/internal/context/config"
	"gokv/internal/context/environment"
	"gokv/internal/hashring"
	"gokv/internal/models/peer"
	"gokv/internal/pool"
	"gokv/internal/registry"
	"gokv/internal/storage"
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
	NodeID             string                    // ID of the current node.
	NodeInternalAddr   string                    // Address of the current node.
	NodeExternalAddr   string                    //
	PeerMap            map[string]*peer.Peer     // Map of nodes in the cluster.
	HashRing           *hashring.HashRing        // Consistent hashing implementation.
	ConnPool           *pool.GrpcConnectionPool  // Connection pool for gRPC clients.
	DataStore          *storage.DataStore        // In-memory data store.
	CommandRegistry    *registry.CommandRegistry // Registry for supported commands.
	LastRebalancedRing *hashring.HashRing        // Ring used for the last rebalance (avoids multiple reblance calls)
}

// NewClusterManager creates and initializes a new ClusterManager.
func NewClusterManager(env *environment.Environment, cfg *config.Config) *ClusterManager {
	cr := registry.NewCommandRegistry()
	ds := storage.NewDataStore(cfg.Shards, cfg.ShardsPerCursor, cfg.CleanupInterval)
	peerMap := make(map[string]*peer.Peer)
	hashRing := hashring.New(cfg.VNodeCount, cfg.Replicas)
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
		DataStore:        ds,
		CommandRegistry:  cr,
	}

	cm.HashRing.Add(cm.NodeID)
	cm.LastRebalancedRing = cm.HashRing.Copy()

	// Register HashMap commands.
	cr.Register("HGET", registry.Command{Run: cm.HGet})
	cr.Register("HSET", registry.Command{Run: cm.HSet, Replicate: true})
	cr.Register("HDEL", registry.Command{Run: cm.HDel, Replicate: true})
	cr.Register("HSCAN", registry.Command{Run: cm.HScan, ResponsibleFunc: cm.findCursorNode})

	// Register ListMap commands.
	cr.Register("LPUSH", registry.Command{Run: cm.LPush, Replicate: true})
	cr.Register("LPOP", registry.Command{Run: cm.LPop, Replicate: true})
	cr.Register("RPUSH", registry.Command{Run: cm.RPush, Replicate: true})
	cr.Register("RPOP", registry.Command{Run: cm.RPop, Replicate: true})
	cr.Register("LLEN", registry.Command{Run: cm.LLen})
	cr.Register("LSCAN", registry.Command{Run: cm.LScan, ResponsibleFunc: cm.findCursorNode})

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

// Runs a command locally, does not check for responsibility.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", req.Command)
	}
	slog.Debug(fmt.Sprintf("cluster manager: running local command: %s %s", req.Command, req.Key))
	return cmd.Run(req.Key, req.Args...)
}

// Forwards command to a replica, forwarded comands only attempt to run locally and do not trigger replication.
func (cm *ClusterManager) ForwardCommand(ctx context.Context, req *commonpb.CommandRequest, nodeID string) (*commonpb.CommandResponse, error) {
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		return nil, fmt.Errorf("cluster manager: failed to get client for node %s", nodeID)
	}
	slog.Debug(fmt.Sprintf("cluster manager: forwarding to node %s command: %s %s", nodeID, req.Command, req.Key))
	res, err := client.ForwardCommand(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cluster manager: failed to forwarding command to node %s: %v", nodeID, err)
	}
	return res, nil
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
