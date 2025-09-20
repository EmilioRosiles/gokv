package config

import (
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

func LoadConfig() *Config {
	yamlFile, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling YAML: %v", err)
	}
	return &config
}
