package environment

import (
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Environment struct {
	NodeID       string
	Host         string
	Port         string
	SeedNodeID   string
	SeedNodeAddr string
	CfgPath      string
	TlsCertPath  string
	TlsKeyPath   string
	LogLevel     slog.Level
}

// LoadEnvironment loads the environment variables from the .env file and the system.
func LoadEnvironment() *Environment {
	godotenv.Load()
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node1"
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}

	return &Environment{
		NodeID:       nodeID,
		Host:         host,
		Port:         port,
		SeedNodeID:   os.Getenv("SEED_NODE_ID"),
		SeedNodeAddr: os.Getenv("SEED_NODE_ADDR"),
		CfgPath:      os.Getenv("CONFIG_PATH"),
		TlsCertPath:  os.Getenv("TLS_CERT_PATH"),
		TlsKeyPath:   os.Getenv("TLS_KEY_PATH"),
		LogLevel:     logLevel(os.Getenv("LOG_LEVEL")),
	}
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
		return slog.LevelDebug
	}
}
