package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"gokv/proto/commonpb"
)

// Runs a command in cluster. Handlers retries, redirection, and replication.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return &commonpb.CommandResponse{Error: fmt.Sprintf("unknown command: %s", req.Command)}, nil
	}

	if cmd.ResponsibleFunc != nil {
		nodeIDs, err := cmd.ResponsibleFunc(req.Key)
		if err != nil {
			return &commonpb.CommandResponse{Error: err.Error()}, nil
		}
		responsibleNodeIDs = nodeIDs
	}

	var res *commonpb.CommandResponse
	var err error
	if responsibleNodeIDs[0] == cm.NodeID {
		res, err = cm.RunLocalCommand(ctx, req)
	} else {
		res, err = cm.ForwardCommand(ctx, req, responsibleNodeIDs[0])
		if err != nil {
			cm.RemoveNode(responsibleNodeIDs[0])
			slog.Debug(fmt.Sprintf("cluster manager: fail to forward command %s %s trying again...", req.Command, req.Key))
			return cm.RunCommand(ctx, req)
		}
	}

	if err != nil {
		return &commonpb.CommandResponse{Error: err.Error()}, nil
	}

	if cmd.Replicate && cm.HashRing.Replicas > 1 {
		go cm.ReplicateCommand(req)
	}

	return res, nil
}

// Runs a command locally.
func (cm *ClusterManager) RunLocalCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", req.Command)
	}
	slog.Debug(fmt.Sprintf("cluster manager: running local command: %s %s", req.Command, req.Key))
	return cmd.Run(req.Key, req.Args...)
}

// Forwards command, forwarded comands only attempt to run locally and do not trigger replication.
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

// Replicate command to other responsible nodes, uses forward command.
func (cm *ClusterManager) ReplicateCommand(req *commonpb.CommandRequest) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	ctx := context.Background()
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
