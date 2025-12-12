package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
	"unicode/utf8"

	"gokv/internal/tls"
	"gokv/proto/commonpb"
	"gokv/proto/externalpb"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/vmihailenco/msgpack/v5"
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

		var result any
		if len(res.Response) > 0 {
			err = msgpack.Unmarshal(res.Response, &result)
			if err != nil {
				log.Fatalf("Failed to unmarshal response data: %v", err)
			}
		} else {
			result = nil
		}

		result = convertBytesToStrings(result)

		jsonValue, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal json for printing: %v", err)
		}

		fmt.Print(string(jsonValue))
		fmt.Print("\n")
	},
}

func convertBytesToStrings(data any) any {
	switch v := data.(type) {
	case map[string]any:
		newMap := make(map[string]any, len(v))
		for key, val := range v {
			newMap[key] = convertBytesToStrings(val)
		}
		return newMap
	case []any:
		newSlice := make([]any, len(v))
		for i, val := range v {
			newSlice[i] = convertBytesToStrings(val)
		}
		return newSlice
	case []byte:
		if utf8.ValidString(string(v)) {
			return string(v)
		}
		return v
	default:
		return v
	}
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

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
