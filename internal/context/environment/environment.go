package environment

import (
	"fmt"
	"log/slog"
	"os"

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
}

// LoadEnvironment loads the environment variables from the .env file and the system.
func LoadEnvironment() *Environment {
	err := godotenv.Load()
	if err != nil {
		slog.Warn(fmt.Sprintf("environment: error loading .env file: %v", err))
	}

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
	}
}
