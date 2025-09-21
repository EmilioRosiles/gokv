package config

import (
	"fmt"
	"gokv/internal/context/environment"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	CleanupInterval   time.Duration `yaml:"cleanup_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	GossipPeerCount   int           `yaml:"gossip_peer_count"`
	VNodeCount        int           `yaml:"v_node_count"`
	MessageTimeout    time.Duration `yaml:"message_timeout"`
}

func Default() *Config {
	return &Config{
		CleanupInterval:   10 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		GossipPeerCount:   2,
		VNodeCount:        3,
		MessageTimeout:    5 * time.Second,
	}
}

func LoadConfig(env *environment.Environment) *Config {
	// If no config path is provided, load the default configuration.
	if env.CfgPath == "" {
		slog.Debug("config: loading default configuration")
		return Default()
	}

	// Read the YAML file.
	yamlFile, err := os.ReadFile(env.CfgPath)
	if err != nil {
		slog.Warn(fmt.Sprintf("config: could not read config file, loading default configuration: %v", err))
		return Default()
	}

	// Unmarshal the YAML file into the Config struct.
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		slog.Error(fmt.Sprintf("config: error unmarshalling YAML: %v", err))
		os.Exit(1)
	}

	slog.Info(fmt.Sprintf("config: loaded configuration from: %s", env.CfgPath))
	return &config
}
