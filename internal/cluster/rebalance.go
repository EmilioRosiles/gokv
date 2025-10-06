package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"gokv/internal/hashring"
	"gokv/internal/storage"
	"gokv/proto/commonpb"
	"gokv/proto/internalpb"
)

// Batch streams commands to new owners
func (cm *ClusterManager) rebalanceCommands(nodeID string, commands []*commonpb.CommandRequest) {
	slog.Info(fmt.Sprintf("rebalance: %d keys to node %s", len(commands), nodeID))
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		slog.Warn(fmt.Sprintf("rebalance: command failed for peer %s, client not found", nodeID))
		return
	}

	ctx := context.Background()
	stream, err := client.Rebalance(ctx)
	if err != nil {
		slog.Warn(fmt.Sprintf("rebalance: command failed for peer %s, stream error: %v", nodeID, err))
		return
	}

	batchSize := 100
	for i := 0; i < len(commands); i += batchSize {
		end := min(i+batchSize, len(commands))
		batch := &internalpb.RebalanceRequest{Commands: commands[i:end]}
		if err := stream.Send(batch); err != nil {
			slog.Warn(fmt.Sprintf("rebalance: command failed for peer %s, send error: %v", nodeID, err))
			continue
		}
	}

	if err := stream.CloseSend(); err != nil {
		slog.Warn(fmt.Sprintf("rebalance: failed for peer %s, close error: %v", nodeID, err))
	}
}

// Rebalance redistributes keys across the cluster when the state changes.
func (cm *ClusterManager) Rebalance(oldRing, newRing *hashring.HashRing) {
	slog.Debug("rebalance: started")

	commandsByNode := make(map[string][]*commonpb.CommandRequest)
	deleteList := make([]string, 0)

	cm.DataStore.Scan(-1, 0, func(key string, store storage.Storable) {
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
			cm.rebalanceCommands(nodeID, commands)
		}
	}

	for _, key := range deleteList {
		cm.DataStore.Del(key)
	}

	slog.Debug("rebalance: finished")
}
