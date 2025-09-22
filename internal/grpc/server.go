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
	"google.golang.org/grpc/metadata"
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
	s.cm.MergeState(req.Peers)
	next := s.cm.HashRing.GetVersion()
	prev := s.cm.HashRing.GetLastVersion()
	if next != prev {
		s.cm.HashRing.CommitVersion()
		go s.cm.Rebalance()
	}

	s.cm.Mu.RLock()
	self := &clusterpb.Node{NodeId: s.cm.NodeID, NodeAddr: s.cm.NodeAddr, Alive: true, LastSeen: time.Now().Unix()}
	peerspb := make([]*clusterpb.Node, 0, len(s.cm.PeerMap)+1)
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
	replicate := true
	md, ok := metadata.FromIncomingContext(ctx)
	if vals := md.Get("replicate"); ok && len(vals) > 0 {
		if vals[0] == "false" {
			replicate = false
		}
	}

	data, err := s.cm.RunCommand(ctx, req, replicate)
	if err != nil {
		return &clusterpb.CommandResponse{Error: err.Error()}, nil
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
	replicate := true
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if vals := md.Get("replicate"); ok && len(vals) > 0 {
		if vals[0] == "false" {
			replicate = false
		}
	}

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
		data, err := s.cm.RunCommand(ctx, req, replicate)
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
