package config

import (
	"fmt"
	"gokv/internal/context/environment"
	"log/slog"
	"os"
	"reflect"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	CleanupInterval     time.Duration `yaml:"cleanup_interval"`
	HeartbeatInterval   time.Duration `yaml:"heartbeat_interval"`
	GossipPeerCount     int           `yaml:"gossip_peer_count"`
	VNodeCount          int           `yaml:"v_node_count"`
	MessageTimeout      time.Duration `yaml:"message_timeout"`
	Replicas            int           `yaml:"replicas"`
	MessageRetry        int           `yaml:"message_retry"`
	PersistenceInterval time.Duration `yaml:"persistence_interval"`
}

func Default() *Config {
	return &Config{
		CleanupInterval:     10 * time.Second,
		HeartbeatInterval:   3 * time.Second,
		GossipPeerCount:     2,
		VNodeCount:          100,
		MessageTimeout:      1 * time.Second,
		Replicas:            2,
		MessageRetry:        3,
		PersistenceInterval: 10 * time.Second,
	}
}

func LoadConfig(env *environment.Environment) *Config {
	config := Default()

	if env.CfgPath != "" {
		yamlFile, err := os.ReadFile(env.CfgPath)
		if err != nil {
			slog.Warn(fmt.Sprintf("config: could not read config file, using default configuration: %v", err))
		} else {
			var yamlConfig Config
			err = yaml.Unmarshal(yamlFile, &yamlConfig)
			if err != nil {
				slog.Error(fmt.Sprintf("config: error unmarshalling YAML, using default configuration: %v", err))
			} else {
				mergeConfigs(config, &yamlConfig)
				slog.Debug(fmt.Sprintf("config: loaded and merged configuration from: %s", env.CfgPath))
			}
		}
	} else {
		slog.Debug("config: no config path provided, using default configuration")
	}

	return config
}

func mergeConfigs(base *Config, overlay *Config) {
	baseVal := reflect.ValueOf(base).Elem()
	overlayVal := reflect.ValueOf(overlay).Elem()

	for i := 0; i < baseVal.NumField(); i++ {
		baseField := baseVal.Field(i)
		overlayField := overlayVal.Field(i)

		if overlayField.Kind() == reflect.Struct {
			for j := 0; j < overlayField.NumField(); j++ {
				if !overlayField.Field(j).IsZero() {
					baseField.Field(j).Set(overlayField.Field(j))
				}
			}
		} else if !overlayField.IsZero() {
			baseField.Set(overlayField)
		}
	}
}
