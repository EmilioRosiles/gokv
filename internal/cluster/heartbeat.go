package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gokv/internal/context/config"
	"gokv/internal/models/peer"
	"gokv/proto/internalpb"
)

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
