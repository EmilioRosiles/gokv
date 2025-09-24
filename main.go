package main

import (
	"fmt"
	"log/slog"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: env.LogLevel}))
	slog.SetDefault(logger)

	slog.Info(fmt.Sprintf("main: starting node: %s", env.NodeID))
	cfg := config.LoadConfig(env)

	// Create a new cluster manager.
	cm := cluster.NewClusterManager(env, cfg)

	// Start the gRPC internal and external servers.
	go grpc.StartInternalServer(env, cm)
	go grpc.StartExternalServer(env, cm)

	// If seed nodes are provided, add it to the cluster, send heartbeat, and trigger initial rebalance.
	if len(env.ClusterSeeds) > 0 {
		old := cm.HashRing.Copy()
		for nodeID, internalAddr := range env.ClusterSeeds {
			cm.AddNode(nodeID, internalAddr, "")
		}
		allPeers := cm.GetRandomAlivePeers(cm.AlivePeers())
		cm.Heartbeat(allPeers...)
		new := cm.HashRing
		// Initial rebalance currently doesn't do anything, but it will eventually when I add data persistance.
		cm.Rebalance(old, new)
	}

	// Start the heartbeat process in a new goroutine.
	go cm.StartHeartbeat(cfg)

	// Wait for a shutdown signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	slog.Info(fmt.Sprintf("main: shutting down node: %s", env.NodeID))
}
