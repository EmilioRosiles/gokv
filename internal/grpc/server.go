package grpc

import (
	"context"
	"errors"
	"log"
	"net"

	"gokv/internal/cluster"
	"gokv/internal/cluster/peer"
	position "gokv/internal/models"
	clusterpb "gokv/proto"

	"github.com/golang/geo/s2"
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

// Get gRPC message handler
func (s *clusterNodeServer) Get(ctx context.Context, req *clusterpb.GetRequest) (*clusterpb.GetResponse, error) {
	log.Printf("Received get from node %s for cell %v", req.NodeId, req.CellId)
	cellID := s2.CellID(req.CellId)
	if !cellID.IsValid() {
		return nil, errors.New("invalid cell ID")
	}

	positions := s.cm.GetPositions(cellID)
	return &clusterpb.GetResponse{Positions: position.ToProtoList(positions)}, nil
}

// Set gRPC message handler
func (s *clusterNodeServer) Set(ctx context.Context, req *clusterpb.SetRequest) (*clusterpb.SetResponse, error) {
	log.Printf("Received set from node %s", req.NodeId)
	positions := position.FromProtoList(req.Positions)
	s.cm.SetPositions(positions)
	return &clusterpb.SetResponse{Success: true}, nil
}
