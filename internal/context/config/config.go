package config

import (
	"gokv/internal/context/environment"
	"log"
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
	if env.CfgPath == "" {
		return Default()
	}

	yamlFile, err := os.ReadFile(env.CfgPath)
	if err != nil {
		return Default()
	}

	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling YAML: %v", err)
	}
	return &config
}
