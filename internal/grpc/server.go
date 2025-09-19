package grpc

import (
	"context"
	"log"
	"net"

	"gokv/internal/cluster"
	"gokv/internal/models/peer"
	clusterpb "gokv/proto"

	"google.golang.org/grpc"
)

type clusterNodeServer struct {
	clusterpb.UnimplementedClusterNodeServer
	cm *cluster.ClusterManager
}

func StartGrpcServer(host string, port string, cm *cluster.ClusterManager) {
	lis, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalf("could not start gRPC server: %v", err)
		return
	}

	grpcServer := grpc.NewServer()
	serverImplementation := &clusterNodeServer{cm: cm}
	clusterpb.RegisterClusterNodeServer(grpcServer, serverImplementation)
	log.Printf("Starting gRPC server on: %s\n", host+":"+port)
	grpcServer.Serve(lis)
}

// Heartbeat gRPC message handler
func (s *clusterNodeServer) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	log.Printf("Received heartbeat from node %s", req.Self.NodeId)
	s.cm.MergeState(append(req.Peers, req.Self))

	s.cm.Mu.RLock()
	peerspb := make([]*clusterpb.Node, 0, len(s.cm.PeerMap))
	for _, peerToAdd := range s.cm.PeerMap {
		peerspb = append(peerspb, peer.ToProto(*peerToAdd))
	}
	s.cm.Mu.RUnlock()
	return &clusterpb.HeartbeatResponse{Peers: peerspb}, nil
}

// RunCommand gRPC message handler
func (s *clusterNodeServer) RunCommand(ctx context.Context, req *clusterpb.CommandRequest) (*clusterpb.CommandResponse, error) {
	log.Printf("Received command %s for key %s", req.Command, req.Key)
	data, err := s.cm.RunCommand(req.Command, req.Key, req.Args...)
	if err != nil {
		return &clusterpb.CommandResponse{Error: err.Error()}, nil
	}
	return &clusterpb.CommandResponse{Data: data}, nil
}
