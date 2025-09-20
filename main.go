package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"gokv/internal/cluster"
	"gokv/internal/context/config"
	"gokv/internal/context/environment"
	"gokv/internal/grpc"
)

// main is the entry point of the gokv application.
// It initializes the cluster manager, starts the gRPC server, and handles graceful shutdown.
func main() {
	env := environment.LoadEnvironment()
	cfg := config.LoadConfig(env)

	log.Printf("main: starting node %s", env.NodeID)

	// Create a new cluster manager.
	cm := cluster.NewClusterManager(env, cfg)

	// Start the gRPC server in a new goroutine.
	go grpc.StartGrpcServer(env, cm)

	// If a seed node is provided, add it to the cluster and send a heartbeat.
	if env.SeedNodeID != "" && env.SeedNodeAddr != "" {
		cm.AddNode(env.SeedNodeID, env.SeedNodeAddr)
		if seedNode, ok := cm.GetPeer(env.SeedNodeID); ok {
			cm.Heartbeat(seedNode)
		}
	}

	// Start the heartbeat process in a new goroutine.
	go cm.StartHeartbeat(cfg)

	// Wait for a shutdown signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	log.Printf("main: shutting down node %s", env.NodeID)
}
