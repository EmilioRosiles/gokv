package grpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"time"

	"gokv/internal/cluster"
	"gokv/internal/context/environment"
	"gokv/internal/models/peer"
	"gokv/internal/tls"
	"gokv/proto/commonpb"
	"gokv/proto/internalpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// clusterNodeServer is the implementation of the ClusterNode gRPC server.
// It holds a reference to the ClusterManager to access cluster-related functionality.
type internalServer struct {
	internalpb.UnimplementedInternalServerServer
	cm *cluster.ClusterManager
}

// StartGrpcServer starts the gRPC server on the specified host and port.
// It registers the clusterNodeServer implementation with the gRPC server.
func StartInternalServer(env *environment.Environment, cm *cluster.ClusterManager) {
	lis, err := net.Listen("tcp", env.BindAddr)

	if err != nil {
		slog.Error(fmt.Sprintf("gRPC internal: could not start listening: %v", err))
		os.Exit(1)
	}

	creds := insecure.NewCredentials()
	if env.InternalTlsServerCertPath != "" || env.InternalTlsServerKeyPath != "" {
		slog.Debug("gRPC internal: attempting to start with TLS")
		internalTLS, err := tls.BuildServerTLSConfig(
			env.InternalTlsServerCertPath,
			env.InternalTlsServerKeyPath,
			env.InternalTlsCAPath,
			true,
		)
		if err != nil {
			slog.Error(fmt.Sprintf("gRPC internal: failed to load TLS credentials: %v", err))
			os.Exit(1)
		}
		creds = credentials.NewTLS(internalTLS)
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	serverImplementation := &internalServer{cm: cm}
	internalpb.RegisterInternalServerServer(grpcServer, serverImplementation)
	slog.Info(fmt.Sprintf("gRPC internal: starting on %s", env.BindAddr))
	grpcServer.Serve(lis)
}

// Heartbeat handles incoming heartbeat requests from other nodes in the cluster.
// It merges the state of the incoming node and its peers with the current node's state.
func (s *internalServer) Heartbeat(ctx context.Context, req *internalpb.HeartbeatRequest) (*internalpb.HeartbeatResponse, error) {
	slog.Debug("gRPC internal: received heartbeat")
	s.cm.MergeState(req.Peers)
	if s.cm.LastRebalancedRing.GetVersion() != s.cm.HashRing.GetVersion() {
		go s.cm.Rebalance(s.cm.LastRebalancedRing, s.cm.HashRing)
		s.cm.Mu.Lock()
		s.cm.LastRebalancedRing = s.cm.HashRing.Copy()
		s.cm.Mu.Unlock()
	}

	s.cm.Mu.RLock()
	self := &internalpb.HeartbeatNode{NodeId: s.cm.NodeID, NodeInternalAddr: s.cm.NodeInternalAddr, NodeExternalAddr: s.cm.NodeExternalAddr, Alive: true, LastSeen: time.Now().Unix()}
	peerspb := make([]*internalpb.HeartbeatNode, 0, len(s.cm.PeerMap)+1)
	peerspb = append(peerspb, self)
	for _, peerToAdd := range s.cm.PeerMap {
		peerspb = append(peerspb, peer.ToHeartbeatNodeProto(*peerToAdd))
	}
	s.cm.Mu.RUnlock()
	return &internalpb.HeartbeatResponse{Peers: peerspb}, nil
}

// ForwardCommand handles forwarded commands, they are only attempted to be run locally.
// If this node is not responsible for the command it just fails.
func (s *internalServer) ForwardCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	slog.Debug(fmt.Sprintf("gRPC internal: forwarded command %s %s", req.Command, req.Key))
	responsibleNodeIDs := s.cm.HashRing.Get(req.Key)
	cmd, ok := s.cm.CommandRegistry.Get(req.Command)
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", req.Command)
	}

	if cmd.ResponsibleFunc != nil {
		if nodeIDs, err := cmd.ResponsibleFunc(req); err == nil {
			responsibleNodeIDs = nodeIDs
		}
	}

	var res *commonpb.CommandResponse
	var err error
	if slices.Contains(responsibleNodeIDs, s.cm.NodeID) {
		res, err = s.cm.RunCommand(ctx, req)
	} else {
		err = fmt.Errorf("node %s not responsible for forwarded command %s %s", req.Command, s.cm.NodeID, req.Key)
	}

	if err != nil {
		return &commonpb.CommandResponse{Error: err.Error()}, nil
	}

	return res, nil
}

// Rebalance handles key migration to rebalance the cluster when the state changes
func (s *internalServer) Rebalance(stream internalpb.InternalServer_RebalanceServer) error {
	slog.Debug("gRPC internal: received cluster rebalance")
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC internal: error from rebalance stream: %v", err))
			return err
		}

		slog.Debug(fmt.Sprintf("gRPC internal: processing rebalance message for %v commands", len(req.Commands)))
		for _, command := range req.Commands {
			responsibleNodeIDs := s.cm.HashRing.Get(command.Key)
			if slices.Contains(responsibleNodeIDs, s.cm.NodeID) {
				_, err = s.cm.RunCommand(ctx, command)
			} else {
				err = fmt.Errorf("node %v not responsible for command %s %s", s.cm.NodeID, command.Command, command.Key)
			}
			if err != nil {
				slog.Warn(fmt.Sprintf("gRPC internal: error rebalancing command: %v", err))
			}
		}
	}

	err := stream.SendAndClose(&internalpb.RebalanceResponse{Success: true})
	if err != nil {
		slog.Warn(fmt.Sprintf("gRPC internal: error sending to rebalance stream: %v", err))
		return err
	}

	return nil
}
