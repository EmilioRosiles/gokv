package grpc

import (
	"context"
	"io"
	"log"
	"net"
	"time"

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

// Get gRPC message handler
func (s *clusterNodeServer) Get(ctx context.Context, req *clusterpb.GetRequest) (*clusterpb.GetResponse, error) {
	log.Printf("Received get for hash %s", req.Hash)
	data := s.cm.Get(req.Hash, req.Key)
	return &clusterpb.GetResponse{Data: data}, nil
}

// Set gRPC message handler
func (s *clusterNodeServer) Set(ctx context.Context, req *clusterpb.SetRequest) (*clusterpb.SetResponse, error) {
	log.Printf("Received set for hash %s", req.Hash)
	s.cm.Set(req.Hash, req.Key, req.Data, time.Duration(req.Ttl))
	return &clusterpb.SetResponse{Success: true}, nil
}

// Delete gRPC message handler
func (s *clusterNodeServer) Delete(ctx context.Context, req *clusterpb.DeleteRequest) (*clusterpb.DeleteResponse, error) {
	log.Printf("Received delete for hash %s", req.Hash)
	s.cm.Delete(req.Hash, req.Key)
	return &clusterpb.DeleteResponse{Success: true}, nil
}

// Stream Get gRPC message handler
func (s *clusterNodeServer) StreamGet(stream clusterpb.ClusterNode_StreamGetServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("Received set for hash %s", req.Hash)
		data := s.cm.Get(req.Hash, req.Key)

		res := &clusterpb.GetResponse{Data: data}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

// Stream Set gRPC message handler
func (s *clusterNodeServer) StreamSet(stream clusterpb.ClusterNode_StreamSetServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("Received set for hash %s", req.Hash)
		s.cm.Set(req.Hash, req.Key, req.Data, time.Duration(req.Ttl))

		res := &clusterpb.SetResponse{Success: true}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

// Stream Get gRPC message handler
func (s *clusterNodeServer) StreamDelete(stream clusterpb.ClusterNode_StreamDeleteServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("Received set for hash %s", req.Hash)
		s.cm.Delete(req.Hash, req.Key)

		res := &clusterpb.DeleteResponse{Success: true}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}
