package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"gokv/internal/cluster"
	"gokv/internal/context/environment"
	"gokv/proto/commonpb"
)

type CommandRequest struct {
	Command string `json:"command"`
}

func StartRESTServer(env *environment.Environment, cm *cluster.ClusterManager) {
	smux := http.NewServeMux()
	smux.HandleFunc("/command", commandHandler(cm))

	if env.ExternalTlsServerCertPath != "" || env.ExternalTlsServerKeyPath != "" {
		slog.Info(fmt.Sprintf("REST: starting with TLS on %s", env.ExternalRestBindAddr))
		if err := http.ListenAndServeTLS(env.ExternalRestBindAddr, env.ExternalTlsServerCertPath, env.ExternalTlsServerKeyPath, smux); err != nil {
			slog.Error(fmt.Sprintf("REST: could not start listening with TLS: %v", err))
		}
	} else {
		slog.Info(fmt.Sprintf("REST: starting on %s", env.ExternalRestBindAddr))
		if err := http.ListenAndServe(env.ExternalRestBindAddr, smux); err != nil {
			slog.Error(fmt.Sprintf("REST: could not start listening: %v", err))
		}
	}
}

func commandHandler(cm *cluster.ClusterManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}

		var req CommandRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "Error decoding JSON request", http.StatusBadRequest)
			return
		}

		parts := strings.Fields(req.Command)

		if len(parts) < 2 {
			http.Error(w, "Invalid command format: command, key, [args ...]", http.StatusBadRequest)
			return
		}

		name := parts[0]
		key := parts[1]
		var args [][]byte
		for _, arg := range parts[2:] {
			args = append(args, []byte(arg))
		}

		slog.Debug(fmt.Sprintf("REST: received command %s %s", name, key))

		resp, err := cm.RunCommand(r.Context(), &commonpb.CommandRequest{
			Command: name,
			Key:     key,
			Args:    args,
		})

		if resp.Error != "" {
			http.Error(w, resp.Error, http.StatusBadRequest)
			return
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := commonpb.ValueToInterface(resp.Response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(data); err != nil {
			slog.Error(fmt.Sprintf("REST: error encoding command response: %v", err))
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}
}
