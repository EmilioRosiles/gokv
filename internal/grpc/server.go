package grpc

import (
	"context"
	"log"
	"net"

	"gokv/internal/cluster"
	"gokv/internal/models/peer"
	"gokv/internal/response"
	"gokv/proto/clusterpb"

	"google.golang.org/grpc"
)

// clusterNodeServer is the implementation of the ClusterNode gRPC server.
// It holds a reference to the ClusterManager to access cluster-related functionality.
type clusterNodeServer struct {
	clusterpb.UnimplementedClusterNodeServer
	cm *cluster.ClusterManager
}

// StartGrpcServer starts the gRPC server on the specified host and port.
// It registers the clusterNodeServer implementation with the gRPC server.
func StartGrpcServer(host string, port string, cm *cluster.ClusterManager) {
	lis, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalf("gRPC server: could not start listening: %v", err)
		return
	}

	grpcServer := grpc.NewServer()
	serverImplementation := &clusterNodeServer{cm: cm}
	clusterpb.RegisterClusterNodeServer(grpcServer, serverImplementation)
	log.Printf("gRPC server: starting on %s:%s", host, port)
	grpcServer.Serve(lis)
}

// Heartbeat handles incoming heartbeat requests from other nodes in the cluster.
// It merges the state of the incoming node and its peers with the current node's state.
func (s *clusterNodeServer) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	log.Printf("gRPC server: received heartbeat from node %s", req.Self.NodeId)
	s.cm.MergeState(append(req.Peers, req.Self))

	s.cm.Mu.RLock()
	peerspb := make([]*clusterpb.Node, 0, len(s.cm.PeerMap))
	for _, peerToAdd := range s.cm.PeerMap {
		peerspb = append(peerspb, peer.ToProto(*peerToAdd))
	}
	s.cm.Mu.RUnlock()
	return &clusterpb.HeartbeatResponse{Peers: peerspb}, nil
}

// RunCommand handles incoming command requests from other nodes in the cluster.
// It executes the command using the ClusterManager and returns the result.
func (s *clusterNodeServer) RunCommand(ctx context.Context, req *clusterpb.CommandRequest) (*clusterpb.CommandResponse, error) {
	log.Printf("gRPC server: received command '%s' for key '%s'", req.Command, req.Key)
	data, err := s.cm.RunCommand(req.Command, req.Key, req.Args...)
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
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("gRPC server: error receiving from stream: %v", err)
			return err
		}

		log.Printf("gRPC server: received stream command '%s' for key '%s'", req.Command, req.Key)
		data, err := s.cm.RunCommand(req.Command, req.Key, req.Args...)
		if err != nil {
			log.Printf("gRPC server: error running stream command: %v", err)
			return err
		}

		resp, err := response.Marshal(data)
		if err != nil {
			log.Printf("gRPC server: error marshalling stream response: %v", err)
			return err
		}

		if err := stream.Send(resp); err != nil {
			log.Printf("gRPC server: error sending to stream: %v", err)
			return err
		}
	}
}
