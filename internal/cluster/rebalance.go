package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"gokv/internal/hashring"
	"gokv/proto/commonpb"
	"gokv/proto/internalpb"
)

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

// // Creates migration commands for HSET and groups them by target node.
// func (cm *ClusterManager) createMigrationCommands(hash string, he *hashmap.HashEntry, targetIDs []string) map[string][]*commonpb.CommandRequest {
// 	commandsByNode := make(map[string][]*commonpb.CommandRequest)
// 	he.Mu.RLock()
// 	defer he.Mu.RUnlock()

// 	for key, entry := range he.Items {
// 		ttl := int64(0)
// 		if entry.ExpiresAt > 0 {
// 			ttl = entry.ExpiresAt - time.Now().Unix()
// 		}

// 		args := make([][]byte, 0)
// 		args = append(args, []byte(key))
// 		args = append(args, entry.Data)
// 		args = append(args, fmt.Appendf(nil, "%d", ttl))

// 		req := &commonpb.CommandRequest{
// 			Command: "HSET",
// 			Key:     hash,
// 			Args:    args,
// 		}

// 		for _, nodeID := range targetIDs {
// 			if nodeID != cm.NodeID {
// 				commandsByNode[nodeID] = append(commandsByNode[nodeID], req)
// 			}
// 		}
// 	}

// 	return commandsByNode
// }

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

	// cm.HashMap.Scan(-1, func(hash string, he *hashmap.HashEntry) {
	// 	oldResponsibleNodeIDs := oldRing.Get(hash)
	// 	newResponsibleNodeIDs := newRing.Get(hash)

	// 	targetIDs := cm.getMigrationTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs)
	// 	if len(targetIDs) == 0 {
	// 		return
	// 	}

	// 	rebalanceLeader := cm.findMigrationLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs)

	// 	if rebalanceLeader == cm.NodeID {
	// 		migrationCommands := cm.createMigrationCommands(hash, he, targetIDs)
	// 		for nodeID, commands := range migrationCommands {
	// 			commandsByNode[nodeID] = append(commandsByNode[nodeID], commands...)
	// 		}
	// 	}

	// 	if !slices.Contains(newResponsibleNodeIDs, cm.NodeID) {
	// 		deleteList = append(deleteList, hash)
	// 	}
	// })

	for nodeID, commands := range commandsByNode {
		if peer, ok := cm.GetPeer(nodeID); ok {
			cm.Heartbeat(peer)
			cm.streamMigrationCommands(nodeID, commands)
		}
	}

	for _, hash := range deleteList {
		cm.HDel(hash)
	}

	slog.Debug("cluster manager: rebalancing finished")
}
