package grpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"gokv/internal/cluster"
	"gokv/internal/context/environment"
	"gokv/internal/models/peer"
	"gokv/internal/tls"
	"gokv/proto/commonpb"
	"gokv/proto/externalpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// externalServer is gRPC server intended to be used by client applications.
// It holds a reference to the ClusterManager to access cluster-related functionality.
type externalServer struct {
	externalpb.UnimplementedExternalServerServer
	cm *cluster.ClusterManager
}

// Start gRPC external server on the specified host and port.
// It registers the clusterNodeServer implementation with the gRPC server.
func StartExternalServer(env *environment.Environment, cm *cluster.ClusterManager) {
	lis, err := net.Listen("tcp", env.ExternalGrpcBindAddr)

	if err != nil {
		slog.Error(fmt.Sprintf("gRPC external: could not start listening: %v", err))
		os.Exit(1)
	}

	creds := insecure.NewCredentials()
	if env.ExternalTlsServerCertPath != "" || env.ExternalTlsServerKeyPath != "" {
		slog.Debug("gRPC external: attempting to start with TLS")
		externalTLS, err := tls.BuildServerTLSConfig(
			env.ExternalTlsServerCertPath,
			env.ExternalTlsServerKeyPath,
			env.ExternalTlsCAPath,
			false,
		)
		if err != nil {
			slog.Error(fmt.Sprintf("gRPC external: failed to load TLS credentials: %v", err))
			os.Exit(1)
		}
		creds = credentials.NewTLS(externalTLS)
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	serverImplementation := &externalServer{cm: cm}
	externalpb.RegisterExternalServerServer(grpcServer, serverImplementation)
	slog.Info(fmt.Sprintf("gRPC external: starting on %s", env.ExternalGrpcBindAddr))
	grpcServer.Serve(lis)
}

// Healthcheck request returns the cluster status.
func (s *externalServer) Healthcheck(ctx context.Context, req *externalpb.HealthcheckRequest) (*externalpb.HealthcheckResponse, error) {
	slog.Debug("gRPC external: received healthcheck")
	s.cm.Mu.RLock()
	self := &externalpb.HealthcheckNode{
		NodeId:   s.cm.NodeID,
		NodeAddr: s.cm.NodeExternalAddr,
		Alive:    true,
	}
	peerspb := make([]*externalpb.HealthcheckNode, 0, len(s.cm.PeerMap)+1)
	peerspb = append(peerspb, self)
	for _, peerToAdd := range s.cm.PeerMap {
		peerspb = append(peerspb, peer.ToHealthcheckNodeProto(*peerToAdd))
	}
	s.cm.Mu.RUnlock()
	return &externalpb.HealthcheckResponse{Peers: peerspb}, nil
}

// RunCommand handles incoming command requests from clients.
// It executes the command in the leader of the replica set (locally or redirects it).
// Replicates command if there are replicas configured and the command level is Replica
func (s *externalServer) RunCommand(ctx context.Context, req *commonpb.CommandRequest) (*commonpb.CommandResponse, error) {
	slog.Debug(fmt.Sprintf("gRPC external: received command %s %s", req.Command, req.Key))
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
	if responsibleNodeIDs[0] == s.cm.NodeID {
		res, err = s.cm.RunCommand(ctx, req)
	} else {
		res, err = s.cm.ForwardCommand(ctx, req, responsibleNodeIDs[0])
		if err != nil {
			s.cm.RemoveNode(responsibleNodeIDs[0])
			slog.Debug(fmt.Sprintf("gRPC external: fail to forward command %s %s trying again...", req.Command, req.Key))
			return s.RunCommand(ctx, req)
		}
	}

	if err != nil {
		return &commonpb.CommandResponse{Error: err.Error()}, nil
	}

	if cmd.Replicate && s.cm.HashRing.Replicas > 1 {
		go s.cm.ReplicateCommand(req)
	}

	return res, nil
}

// StreamCommand handles incoming streaming command requests from other nodes in the cluster.
// It receives a stream of commands, executes them, and sends back a stream of responses.
func (s *externalServer) StreamCommand(stream externalpb.ExternalServer_StreamCommandServer) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC external: error receiving from command stream: %v", err))
			return err
		}

		slog.Debug(fmt.Sprintf("gRPC external: received streamed command %s %s", req.Command, req.Key))
		data, err := s.RunCommand(ctx, req)
		if err != nil {
			slog.Warn(fmt.Sprintf("gRPC external: error running streamed command: %v", err))
			return err
		}

		if err := stream.Send(data); err != nil {
			slog.Warn(fmt.Sprintf("gRPC external: error sending to command stream: %v", err))
			return err
		}
	}
}
