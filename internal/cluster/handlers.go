package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"gokv/internal/models/peer"
	"gokv/proto/commonpb"
	"gokv/proto/externalpb"
)

// Runs a command in cluster. Handlers retries, redirection, and replication.
func (cm *ClusterManager) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	responsibleNodeIDs := cm.HashRing.Get(req.Key)
	cmd, ok := cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", req.Command)
	}

	if cmd.ResponsibleFunc != nil {
		if nodeIDs, err := cmd.ResponsibleFunc(req.Key); err == nil {
			responsibleNodeIDs = nodeIDs
		}
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

// Returns cluster health information.
func (cm *ClusterManager) GetHealth(ctx context.Context, req *externalpb.HealthcheckRequest) *externalpb.HealthcheckResponse {
	cm.Mu.RLock()
	self := &externalpb.HealthcheckNode{
		NodeId:   cm.NodeID,
		NodeAddr: cm.NodeExternalAddr,
		Alive:    true,
	}
	peerspb := make([]*externalpb.HealthcheckNode, 0, len(cm.PeerMap)+1)
	peerspb = append(peerspb, self)
	for _, peerToAdd := range cm.PeerMap {
		peerspb = append(peerspb, peer.ToHealthcheckNodeProto(*peerToAdd))
	}
	cm.Mu.RUnlock()
	return &externalpb.HealthcheckResponse{Peers: peerspb}
}
