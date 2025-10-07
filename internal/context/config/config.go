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

type Cluster struct {
	VNodeCount int `yaml:"v_node_count"`
	Replicas   int `yaml:"replicas"`
}

type Messaging struct {
	GossipPeerCount   int           `yaml:"gossip_peer_count"`
	MessageTimeout    time.Duration `yaml:"message_timeout"`
	MessageRetry      int           `yaml:"message_retry"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

type Rebalance struct {
	RebalanceDebounce time.Duration `yaml:"rebalance_debounce"`
}

type Persistence struct {
	PersistenceInterval time.Duration `yaml:"persistence_interval"`
}

type Maintance struct {
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
}

type Config struct {
	Cluster     Cluster     `yaml:"cluster"`
	Messaging   Messaging   `yaml:"messaging"`
	Rebalance   Rebalance   `yaml:"rebalance"`
	Persistence Persistence `yaml:"persistence"`
	Maintance   Maintance   `yaml:"maintance"`
}

func Default() *Config {
	return &Config{
		Cluster: Cluster{
			VNodeCount: 100,
			Replicas:   2,
		},
		Messaging: Messaging{
			GossipPeerCount:   2,
			MessageTimeout:    1 * time.Second,
			MessageRetry:      3,
			HeartbeatInterval: 3 * time.Second,
		},
		Rebalance: Rebalance{
			RebalanceDebounce: 1 * time.Second,
		},
		Persistence: Persistence{
			PersistenceInterval: 10 * time.Second,
		},
		Maintance: Maintance{
			CleanupInterval: 10 * time.Second,
		},
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
