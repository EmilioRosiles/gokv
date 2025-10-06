package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"gokv/internal/context/config"
	"gokv/proto/internalpb"
)

// StartHeartbeat starts the heartbeat process to periodically send heartbeats to other nodes.
func (cm *ClusterManager) StartHeartbeat(cfg *config.Config) {
	if cfg.HeartbeatInterval == 0 {
		slog.Info("heartbeat: disabled")
		return
	}

	interval := cfg.HeartbeatInterval
	jitterRange := time.Duration(float64(interval) * 0.25)
	for {
		jitter := time.Duration(rand.Int63n(int64(jitterRange)*2)) - jitterRange
		time.Sleep(interval + jitter)

		gossipTargets := cm.GetRandomAlivePeers(cfg.GossipPeerCount)
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
func (cm *ClusterManager) Heartbeat(peerList ...*Peer) {
	if len(peerList) == 0 {
		slog.Debug("heartbeat: skipping, no peers found")
		return
	}

	slog.Debug(fmt.Sprintf("heartbeat: sending to %d peers", len(peerList)))
	for _, peerToCheck := range peerList {
		client, ok := cm.GetPeerClient(peerToCheck.NodeID)
		if !ok {
			slog.Warn(fmt.Sprintf("heartbeat: failed for peer %s, client not found, removing from cluster", peerToCheck.NodeID))
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
			peerspb = append(peerspb, &internalpb.HeartbeatNode{
				NodeId:           peerToAdd.NodeID,
				NodeInternalAddr: peerToAdd.NodeInternalAddr,
				NodeExternalAddr: peerToAdd.NodeExternalAddr,
				Alive:            peerToAdd.Alive,
				LastSeen:         peerToAdd.LastSeen.Unix(),
			})
		}
		cm.Mu.RUnlock()

		req := &internalpb.HeartbeatRequest{Peers: peerspb}

		res, err := client.Heartbeat(context.Background(), req)
		if err != nil {
			slog.Warn(fmt.Sprintf("heartbeat: failed for peer %s, removing from cluster: %v", peerToCheck.NodeID, err))
			cm.RemoveNode(peerToCheck.NodeID)
		} else {
			slog.Debug(fmt.Sprintf("heartbeat: check successful for peer %s", peerToCheck.NodeID))
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
			if remoteNode.Alive {
				cm.AddNode(remoteNode.NodeId, remoteNode.NodeInternalAddr, remoteNode.NodeExternalAddr)
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
