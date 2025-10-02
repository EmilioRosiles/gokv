package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"gokv/internal/hashring"
	"gokv/internal/storage"
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

func (cm *ClusterManager) createHashCommand(hash string, store *storage.HashMap) *commonpb.CommandRequest {
	args := make([][]byte, store.Len()*2)

	i := 0
	for key, entry := range store.Items {
		args[i] = []byte(key)
		args[i+1] = entry.Data
		i += 2
	}

	return &commonpb.CommandRequest{
		Command: "HSET",
		Key:     hash,
		Args:    args,
	}
}

func (cm *ClusterManager) createListCommand(listName string, store *storage.ListMap) *commonpb.CommandRequest {
	args := make([][]byte, store.Len())

	i := 0
	for e := store.Items.Front(); e != nil; e = e.Next() {
		args[i] = e.Value.([]byte)
		i++
	}

	return &commonpb.CommandRequest{
		Command: "RPUSH",
		Key:     listName,
		Args:    args,
	}
}

// Creates migration commands for DataStore and groups them by target node.
func (cm *ClusterManager) createMigrationCommands(key string, store storage.Storable, targetIDs []string) map[string][]*commonpb.CommandRequest {
	commandsByNode := make(map[string][]*commonpb.CommandRequest)
	now := time.Now().Unix()
	expiresAt := store.ExpiresAt()

	store.Mu().RLock()
	defer store.Mu().RUnlock()

	var cmd *commonpb.CommandRequest
	switch s := store.(type) {
	case *storage.HashMap:
		cmd = cm.createHashCommand(key, s)
	case *storage.ListMap:
		cmd = cm.createListCommand(key, s)
	default:
		return commandsByNode
	}

	var expCmd *commonpb.CommandRequest
	if expiresAt != 0 && now < expiresAt {
		ttl := time.Until(time.Unix(expiresAt, 0))
		expCmd = &commonpb.CommandRequest{
			Command: "EXPIRE",
			Key:     key,
			Args:    [][]byte{[]byte(ttl.String())},
		}
	}

	for _, nodeID := range targetIDs {
		if nodeID != cm.NodeID {
			commandsByNode[nodeID] = append(commandsByNode[nodeID], cmd)

			if expCmd != nil {
				commandsByNode[nodeID] = append(commandsByNode[nodeID], expCmd)
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

	cm.DataStore.Scan(-1, func(key string, store storage.Storable) {
		oldResponsibleNodeIDs := oldRing.Get(key)
		newResponsibleNodeIDs := newRing.Get(key)

		targetIDs := cm.getMigrationTargets(oldResponsibleNodeIDs, newResponsibleNodeIDs)
		if len(targetIDs) == 0 {
			return
		}

		rebalanceLeader := cm.findMigrationLeader(oldResponsibleNodeIDs, newResponsibleNodeIDs)

		if rebalanceLeader == cm.NodeID {
			migrationCommands := cm.createMigrationCommands(key, store, targetIDs)
			for nodeID, commands := range migrationCommands {
				commandsByNode[nodeID] = append(commandsByNode[nodeID], commands...)
			}
		}

		if !slices.Contains(newResponsibleNodeIDs, cm.NodeID) {
			deleteList = append(deleteList, key)
		}
	})

	for nodeID, commands := range commandsByNode {
		if peer, ok := cm.GetPeer(nodeID); ok {
			cm.Heartbeat(peer)
			cm.streamMigrationCommands(nodeID, commands)
		}
	}

	for _, key := range deleteList {
		cm.DataStore.Del(key)
	}

	slog.Debug("cluster manager: rebalancing finished")
}
