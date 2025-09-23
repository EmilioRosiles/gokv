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
	slog.Info(fmt.Sprintf("main: shutting down node: %s", env.NodeID))
}
