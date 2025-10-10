package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"gokv/internal/tls"
	"gokv/proto/commonpb"
	"gokv/proto/externalpb"

	"encoding/json"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	uri string
	ca  string
)

var rootCmd = &cobra.Command{
	Use:   "gokv-cli",
	Short: "A CLI for interacting with a gokv cluster",
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of the cluster",
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := createConnection()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		client := externalpb.NewExternalServerClient(conn)

		req := &externalpb.HealthcheckRequest{}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		res, err := client.Healthcheck(ctx, req)
		if err != nil {
			log.Fatalf("Failed to get status: %v", err)
		}

		fmt.Println("Cluster Status:")
		for _, peer := range res.Peers {
			fmt.Printf("  - Node ID: %s, Address: %s, Alive: %t\n", peer.NodeId, peer.NodeAddr, peer.Alive)
		}
	},
}

var runCmd = &cobra.Command{
	Use:   "run [command] [key] [args...]",
	Short: "Run a command on the cluster",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := createConnection()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		client := externalpb.NewExternalServerClient(conn)

		command := args[0]
		key := args[1]
		var cmdArgs [][]byte
		for _, arg := range args[2:] {
			cmdArgs = append(cmdArgs, []byte(arg))
		}

		req := &commonpb.CommandRequest{
			Command: command,
			Key:     key,
			Args:    cmdArgs,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		res, err := client.RunCommand(ctx, req)
		if err != nil {
			log.Fatalf("Failed to run command: %v", err)
		}

		printValue(res.Response)
		fmt.Print("\n")
	},
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Error loading .env file: %v", err)
	}

	uri = os.Getenv("GOKV_CLIENT_URI")
	if uri == "" {
		uri = "localhost:50051"
	}

	ca = os.Getenv("GOKV_CLIENT_CA")

	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(runCmd)
}

func createConnection() (*grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	if ca != "" {
		tlsCfg, err := tls.BuildClientTLSConfig(ca, "", "", uri)
		if err != nil {
			return nil, err
		}
		creds = credentials.NewTLS(tlsCfg)
	}

	return grpc.NewClient(uri,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: 5 * time.Second}),
	)
}

func printValue(v *commonpb.Value) {
	value, err := commonpb.ValueToInterface(v)
	if err != nil {
		log.Fatalf("Failed to convert value to interface: %v", err)
	}

	jsonValue, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal json: %v", err)
	}

	fmt.Print(string(jsonValue))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
