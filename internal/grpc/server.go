package grpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"gokv/internal/cluster"
	"gokv/internal/context/environment"
	"gokv/internal/models/peer"
	"gokv/internal/response"
	"gokv/proto/clusterpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// clusterNodeServer is the implementation of the ClusterNode gRPC server.
// It holds a reference to the ClusterManager to access cluster-related functionality.
type clusterNodeServer struct {
	clusterpb.UnimplementedClusterNodeServer
	cm *cluster.ClusterManager
}

// StartGrpcServer starts the gRPC server on the specified host and port.
// It registers the clusterNodeServer implementation with the gRPC server.
func StartGrpcServer(env *environment.Environment, cm *cluster.ClusterManager) {
	lis, err := net.Listen("tcp", env.Host+":"+env.Port)
	if err != nil {
		slog.Error(fmt.Sprintf("gRPC server: could not start listening: %v", err))
		os.Exit(1)
	}

	creds := insecure.NewCredentials()
	if env.TlsCertPath != "" || env.TlsKeyPath != "" {
		slog.Debug("gRPC server: attempting to start with TLS")
		creds, err = credentials.NewServerTLSFromFile(env.TlsCertPath, env.TlsKeyPath)
		if err != nil {
			slog.Error(fmt.Sprintf("gRPC server: failed to load TLS credentials: %v", err))
			os.Exit(1)
		}
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	serverImplementation := &clusterNodeServer{cm: cm}
	clusterpb.RegisterClusterNodeServer(grpcServer, serverImplementation)
	slog.Info(fmt.Sprintf("gRPC server: starting on %s:%s", env.Host, env.Port))
	grpcServer.Serve(lis)
}

// Heartbeat handles incoming heartbeat requests from other nodes in the cluster.
// It merges the state of the incoming node and its peers with the current node's state.
func (s *clusterNodeServer) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	slog.Debug("gRPC server: received heartbeat")
	prev, _ := s.cm.HashRing.GetVersion()
	s.cm.MergeState(req.Peers)
	next, _ := s.cm.HashRing.GetVersion()
	if next != prev {
		go s.cm.Rebalance()
	}

	s.cm.Mu.RLock()
	self := &clusterpb.Node{NodeId: s.cm.NodeID, NodeAddr: s.cm.NodeAddr, Alive: true, LastSeen: time.Now().Unix()}
	peerspb := make([]*clusterpb.Node, 0)
	peerspb = append(peerspb, self)
	for _, peerToAdd := range s.cm.PeerMap {
		peerspb = append(peerspb, peer.ToProto(*peerToAdd))
	}
	s.cm.Mu.RUnlock()
	return &clusterpb.HeartbeatResponse{Peers: peerspb}, nil
}

// RunCommand handles incoming command requests from other nodes in the cluster.
// It executes the command using the ClusterManager and returns the result.
func (s *clusterNodeServer) RunCommand(ctx context.Context, req *clusterpb.CommandRequest) (*clusterpb.CommandResponse, error) {
	slog.Debug(fmt.Sprintf("gRPC server: received command '%s' for key '%s'", req.Command, req.Key))
	data, err := s.cm.RunCommand(ctx, req)
	if err != nil {
		return &clusterpb.CommandResponse{Error: err.Error()}, nil
	}

	if s.cm.HashRing.Replicas > 1 {
		go s.cm.ReplicateCommand(req)
	}

	resp, err := response.Marshal(data)
	if err != nil {
		return &clusterpb.CommandResponse{Error: err.Error()}, nil
	}

	return resp, nil
}

// StreamCommand handles incoming streaming command requests from other nodes in the cluster.
// It receives a stream of commands, executes them, and sends back a stream of responses.
func (s *clusterNodeServer) StreamCommand(stream clusterpb.ClusterNode_StreamCommandServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC server: error receiving from command stream: %v", err))
			return err
		}

		slog.Debug(fmt.Sprintf("gRPC server: received streamed command '%s' for key '%s'", req.Command, req.Key))
		data, err := s.cm.RunCommand(stream.Context(), req)
		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC server: error running streamed command: %v", err))
			return err
		}

		resp, err := response.Marshal(data)
		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC server: error marshalling streamed command response: %v", err))
			return err
		}

		if err := stream.Send(resp); err != nil {
			slog.Warn(fmt.Sprintf("gRPC server: error sending to command stream: %v", err))
			return err
		}
	}
}

// Rebalance handles batches of commands for rebalancing cluster.
func (s *clusterNodeServer) Rebalance(stream clusterpb.ClusterNode_RebalanceServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&clusterpb.RebalanceResponse{Success: true})
		}

		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC server: error receiving from rebalance stream: %v", err))
			return err
		}

		slog.Debug(fmt.Sprintf("gRPC server: received rebalance commands %v", len(req.Commands)))
		for _, command := range req.Commands {
			s.cm.RunCommand(stream.Context(), command)
		}
	}
}
