package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gokv/internal/cluster"
	"gokv/internal/grpc"

	"github.com/joho/godotenv"
)

// main is the entry point of the gokv application.
// It initializes the cluster manager, starts the gRPC server, and handles graceful shutdown.
func main() {
	godotenv.Load()

	// Load environment variables.
	NODE_ID := os.Getenv("NODE_ID")
	HOST := os.Getenv("HOST")
	GRPC_PORT := os.Getenv("GRPC_PORT")
	SEED_NODE_ID := os.Getenv("SEED_NODE_ID")       // The ID of the seed node to connect to.
	SEED_NODE_ADDR := os.Getenv("SEED_NODE_ADDR") // The address of the seed node to connect to.

	log.Printf("main: starting node %s", NODE_ID)

	// Create a new cluster manager.
	cm := cluster.NewClusterManager(NODE_ID, HOST+":"+GRPC_PORT, 3, 10*time.Second)

	// Start the gRPC server in a new goroutine.
	go grpc.StartGrpcServer(HOST, GRPC_PORT, cm)

	// If a seed node is provided, add it to the cluster and send a heartbeat.
	if SEED_NODE_ID != "" && SEED_NODE_ADDR != "" {
		cm.AddNode(SEED_NODE_ID, SEED_NODE_ADDR)
		if seedNode, ok := cm.GetPeer(SEED_NODE_ID); ok {
			cm.Heartbeat(seedNode)
		}
	}

	// Start the heartbeat process in a new goroutine.
	go cm.StartHeartbeat()

	// Wait for a shutdown signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	log.Printf("main: shutting down node %s", NODE_ID)
}
