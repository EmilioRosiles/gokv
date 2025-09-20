package environment

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Environment struct {
	NodeID       string
	Host         string
	Port         string
	SeedNodeID   string
	SeedNodeAddr string
}

func LoadEnvironment() *Environment {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Error loading .env file, using environment variables: %v", err)
	}

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")

	if host == "" || port == "" {
		log.Fatalf("Error: HOST, and PORT must be set.")
	}

	return &Environment{
		NodeID:       os.Getenv("NODE_ID"),
		Host:         host,
		Port:         port,
		SeedNodeID:   os.Getenv("SEED_NODE_ID"),
		SeedNodeAddr: os.Getenv("SEED_NODE_ADDR"),
	}
}
