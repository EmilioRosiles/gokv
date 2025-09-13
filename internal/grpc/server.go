package grpc

import (
	"context"
	"log"
	"net"

	"gokv/internal/cluster"
	"gokv/internal/cluster/peer"
	clusterpb "gokv/proto"

	"google.golang.org/grpc"
)

type cacheNodeServer struct {
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
	serverImplementation := &cacheNodeServer{cm: cm}
	clusterpb.RegisterClusterNodeServer(grpcServer, serverImplementation)
	log.Printf("Starting gRPC server on: %s\n", host+":"+port)
	grpcServer.Serve(lis)
}

// Heartbeat gRPC message handler
func (s *cacheNodeServer) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
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

func (s *cacheNodeServer) RunCommand() {

}
