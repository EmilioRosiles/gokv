package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"gokv/proto/commonpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Runs a command in cluster. Handlers retries, redirection, and replication.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown command: %s", req.Command)
	}

	if cmd.ResponsibleFunc != nil {
		nodeIDs, err := cmd.ResponsibleFunc(req.Key)
		if err != nil {
			return nil, err
		}
		responsibleNodeIDs = nodeIDs
	}

	var res *commonpb.CommandResponse
	var err error
	if responsibleNodeIDs[0] == cm.NodeID {
		res, err = cm.RunLocalCommand(req)
		if err != nil {
			return nil, err
		}
	} else {
		res, err = cm.ForwardCommand(ctx, req, responsibleNodeIDs[0])
		if err != nil {
			cm.RemoveNode(responsibleNodeIDs[0])
			slog.Debug(fmt.Sprintf("handler: fail to forward command %s %s trying again...", req.Command, req.Key))
			return cm.RunCommand(ctx, req)
		}
	}

	if cmd.Replicate && cm.HashRing.Replicas > 0 {
		go cm.ReplicateCommand(req)
	}

	return res, nil
}

// Runs a command locally.
func (cm *ClusterManager) RunLocalCommand(req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown command: %s", req.Command)
	}
	slog.Debug(fmt.Sprintf("handler: running local command: %s %s", req.Command, req.Key))
	return cmd.Run(req.Key, req.Args...)
}

// Forwards command, forwarded comands only attempt to run locally and do not trigger replication.
func (cm *ClusterManager) ForwardCommand(ctx context.Context, req *commonpb.CommandRequest, nodeID string) (*commonpb.CommandResponse, error) {
	client, ok := cm.GetPeerClient(nodeID)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to get client for node %s", nodeID)
	}
	slog.Debug(fmt.Sprintf("handler: forwarding to node %s command: %s %s", nodeID, req.Command, req.Key))
	res, err := client.ForwardCommand(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Replicate command to other responsible nodes, uses forward command.
func (cm *ClusterManager) ReplicateCommand(req *commonpb.CommandRequest) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	ctx := context.Background()
	for _, replicaID := range responsibleNodeIDs[1:] {
		if replicaID == cm.NodeID {
			cm.RunLocalCommand(req)
		} else {
			slog.Debug(fmt.Sprintf("handler: replicating to node %s commnad: %s %s", replicaID, req.Command, req.Key))
			_, err := cm.ForwardCommand(ctx, req, replicaID)
			if err != nil {
				slog.Warn(fmt.Sprintf("handler: error replicating to node %v commnad: %s %s", replicaID, req.Command, req.Key))
			}
		}
	}
}
