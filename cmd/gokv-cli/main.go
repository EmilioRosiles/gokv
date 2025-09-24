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

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

		if res.Error != "" {
			log.Fatalf("Error: %s", res.Error)
		}

		switch r := res.Response.(type) {
		case *commonpb.CommandResponse_Value:
			fmt.Printf("Result: %s\n", string(r.Value))
		case *commonpb.CommandResponse_Success:
			fmt.Printf("Success: %t\n", r.Success)
		case *commonpb.CommandResponse_Count:
			fmt.Printf("Count: %d\n", r.Count)
		case *commonpb.CommandResponse_List:
			fmt.Printf("List: \n")
			for _, kv := range r.List.List {
				fmt.Printf("  - %s: %s\n", kv.Key, string(kv.Value))
			}
		case *commonpb.CommandResponse_Map:
			fmt.Printf("Map: \n")
			for key, kvList := range r.Map.Map {
				fmt.Printf("  - %s:\n", key)
				for _, kv := range kvList.List {
					fmt.Printf("    - %s: %s\n", kv.Key, string(kv.Value))
				}
			}
		default:
			fmt.Println("OK")
		}
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
	tlsCfg, err := tls.BuildClientTLSConfig(ca, "", "", uri)
	if err != nil {
		return nil, err
	}
	creds := credentials.NewTLS(tlsCfg)

	return grpc.NewClient(uri,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: 5 * time.Second}),
	)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
