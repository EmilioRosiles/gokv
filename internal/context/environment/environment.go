package environment

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Environment struct {
	LogLevel                  slog.Level
	CfgPath                   string
	NodeID                    string
	BindAddr                  string
	AdvertiseAddr             string
	ClusterSeeds              map[string]string
	InternalTlsCAPath         string
	InternalTlsServerCertPath string
	InternalTlsServerKeyPath  string
	InternalTlsClientCertPath string
	InternalTlsClientKeyPath  string
	ExternalTlsCAPath         string
	ExternalTlsServerCertPath string
	ExternalTlsServerKeyPath  string
	ExternalGrpcBindAddr      string
	ExternalGrpcAdvertiseAddr string
	ExternalRestBindAddr      string
	ExternalRestAdvertiseAddr string
	PersistencePath           string
}

// LoadEnvironment loads the environment variables from the .env file and the system.
func LoadEnvironment() *Environment {
	godotenv.Load()

	return &Environment{
		LogLevel:                  logLevel(os.Getenv("GOKV_LOG_LEVEL")),
		CfgPath:                   os.Getenv("GOKV_CONFIG_PATH"),
		NodeID:                    os.Getenv("GOKV_CLUSTER_NODE_ID"),
		BindAddr:                  os.Getenv("GOKV_CLUSTER_BIND_ADDR"),
		AdvertiseAddr:             os.Getenv("GOKV_CLUSTER_ADVERTISE_ADDR"),
		ClusterSeeds:              getClusterSeeds(os.Getenv("GOKV_CLUSTER_SEEDS")),
		InternalTlsCAPath:         os.Getenv("GOKV_INTERNAL_TLS_CA_PATH"),
		InternalTlsServerCertPath: os.Getenv("GOKV_INTERNAL_TLS_SERVER_CERT_PATH"),
		InternalTlsServerKeyPath:  os.Getenv("GOKV_INTERNAL_TLS_SERVER_KEY_PATH"),
		InternalTlsClientCertPath: os.Getenv("GOKV_INTERNAL_TLS_CLIENT_CERT_PATH"),
		InternalTlsClientKeyPath:  os.Getenv("GOKV_INTERNAL_TLS_CLIENT_KEY_PATH"),
		ExternalTlsCAPath:         os.Getenv("GOKV_EXTERNAL_TLS_CA_PATH"),
		ExternalTlsServerCertPath: os.Getenv("GOKV_EXTERNAL_TLS_SERVER_CERT_PATH"),
		ExternalTlsServerKeyPath:  os.Getenv("GOKV_EXTERNAL_TLS_SERVER_KEY_PATH"),
		ExternalGrpcBindAddr:      os.Getenv("GOKV_EXTERNAL_GRPC_BIND_ADDR"),
		ExternalGrpcAdvertiseAddr: os.Getenv("GOKV_EXTERNAL_GRPC_ADVERTISE_ADDR"),
		ExternalRestBindAddr:      os.Getenv("GOKV_EXTERNAL_REST_BIND_ADDR"),
		ExternalRestAdvertiseAddr: os.Getenv("GOKV_EXTERNAL_REST_ADVERTISE_ADDR"),
		PersistencePath:           os.Getenv("GOKV_PERSISTENCE_PATH"),
	}
}

func getClusterSeeds(str string) map[string]string {
	seedMap := make(map[string]string, 0)
	if str == "" {
		return seedMap
	}
	clusterSeeds := strings.Split(str, ",")
	for _, entry := range clusterSeeds {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			slog.Warn(fmt.Sprintf("environment: invalid seed entry: %q", entry))
			continue
		}
		seedMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return seedMap
}

func logLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
