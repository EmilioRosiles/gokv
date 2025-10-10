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
	"gokv/internal/rest"
)

// main is the entry point of the gokv application.
// It initializes the cluster manager, APIs, gossip and snapshots.
func main() {
	env := environment.LoadEnvironment()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: env.LogLevel}))
	slog.SetDefault(logger)

	slog.Info(fmt.Sprintf("main: starting node: %s", env.NodeID))
	cfg := config.LoadConfig(env)

	cm := cluster.NewClusterManager(env, cfg)

	go grpc.StartInternalServer(env, cm)

	if env.ExternalGrpcBindAddr != "" {
		go grpc.StartExternalServer(env, cm)
	}

	if env.ExternalRestBindAddr != "" {
		go rest.StartRESTServer(env, cm)
	}

	// If seed nodes are provided, add them to the cluster, send heartbeat, and trigger initial rebalance.
	if len(env.ClusterSeeds) > 0 {
		for nodeID, internalAddr := range env.ClusterSeeds {
			cm.AddNode(nodeID, internalAddr, "")
		}
		cm.Heartbeat(cm.GetRandomAlivePeers(cm.AlivePeers())...)
	}

	if cfg.Persistence.RestoreOnStartup == config.Always ||
		(cfg.Persistence.RestoreOnStartup == config.Auto && cfg.Cluster.Replicas == 0) {
		slog.Info("main: restoring from snapshop")
		cm.LoadSnapshot(env)
	}

	go cm.StartHeartbeat(cfg)
	go cm.StartSnapshot(env, cfg)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	slog.Info(fmt.Sprintf("main: shutting down node: %s", env.NodeID))
}
