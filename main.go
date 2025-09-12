package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"gokv/internal/cluster"
	"gokv/internal/grpc"
)

func main() {
	NODE_ID := os.Getenv("NODE_ID")
	HOST := os.Getenv("HOST")
	GRPC_PORT := os.Getenv("GRPC_PORT")
	SEED_NODE_ID := os.Getenv("SEED_NODE_ID")
	SEED_NODE_ADDR := os.Getenv("SEED_NODE_ADDR")

	cm := cluster.NewClusterManager(NODE_ID, HOST+":"+GRPC_PORT, 3)

	go grpc.StartGrpcServer(HOST, GRPC_PORT, cm)

	if SEED_NODE_ID != "" && SEED_NODE_ADDR != "" {
		cm.AddNode(SEED_NODE_ID, SEED_NODE_ADDR)
		if seedNode, ok := cm.GetPeer(SEED_NODE_ID); ok {
			cm.Heartbeat(seedNode)
		}
	}

	cm.StartHeartbeat()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	log.Println("Shutting down servers...")
}
