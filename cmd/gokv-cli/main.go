package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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

		printValue(res.Response, 0)
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

func printValue(v *commonpb.Value, indent int) {
	pad := func() { fmt.Print(strings.Repeat(" ", indent)) }

	switch x := v.Kind.(type) {
	case *commonpb.Value_Str:
		fmt.Printf("\"%q\"", x.Str)

	case *commonpb.Value_Int:
		fmt.Printf("%d", x.Int)

	case *commonpb.Value_Bool:
		fmt.Printf("%t", x.Bool)

	case *commonpb.Value_Bytes:
		fmt.Printf("\"%s\"", string(x.Bytes))

	case *commonpb.Value_List:
		fmt.Println("[")
		for i, elem := range x.List.Values {
			pad()
			printValue(elem, indent+2)
			if i > 0 && i < len(x.List.Values)-1 {
				fmt.Print(",")
			}
			fmt.Println()
		}
		fmt.Print("]")

	case *commonpb.Value_Map:
		fmt.Println("{")
		n := len(x.Map.Values)
		i := 0
		for k, elem := range x.Map.Values {
			pad()
			fmt.Printf("  %q: ", k)
			printValue(elem, indent+2)
			i++
			if i < n {
				fmt.Print(",")
			}
			fmt.Println()
		}
		pad()
		fmt.Print("}")

	default:
		fmt.Print("<unknown>")

	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
