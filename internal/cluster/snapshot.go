package cluster

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"gokv/internal/context/config"
	"gokv/internal/context/environment"
	"gokv/internal/storage"
	"gokv/proto/commonpb"

	"google.golang.org/protobuf/proto"
)

func (cm *ClusterManager) StartSnapshot(env *environment.Environment, cfg *config.Config) {
	if cfg.Persistence.PersistenceInterval == 0 {
		slog.Info("snapshot: disabled")
		return
	}

	if env.PersistencePath == "" {
		slog.Warn("snapshot: no persistence path")
		return
	}

	ticker := time.NewTicker(cfg.Persistence.PersistenceInterval)
	done := make(chan bool)

	for {
		select {
		case <-done:
			ticker.Stop()
			return
		case <-ticker.C:
			cm.snapshot(env)
		}
	}
}

func (cm *ClusterManager) snapshot(env *environment.Environment) {
	slog.Debug("snapshot: creating snapshot")

	file, err := os.Create(env.PersistencePath)
	if err != nil {
		slog.Error(fmt.Sprintf("snapshot: failed to create file: %v", err))
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	cm.DataStore.Scan(-1, 0, func(key string, store storage.Storable) {
		responsibleNodeIDs := cm.HashRing.Get(key)
		if cm.NodeID == responsibleNodeIDs[0] {
			commandMap := cm.createMigrationCommands(key, store, []string{cm.NodeID})
			if commands, ok := commandMap[cm.NodeID]; ok {
				for _, cmd := range commands {
					data, err := proto.Marshal(cmd)
					if err != nil {
						slog.Error(fmt.Sprintf("snapshot: failed to marshal command: %v", err))
						continue
					}

					if err := binary.Write(writer, binary.LittleEndian, int64(len(data))); err != nil {
						slog.Error(fmt.Sprintf("snapshot: failed to write message length: %v", err))
						continue
					}

					if _, err := writer.Write(data); err != nil {
						slog.Error(fmt.Sprintf("snapshot: failed to write message data: %v", err))
						continue
					}
				}
			}
		}
	})

	if err := writer.Flush(); err != nil {
		slog.Error(fmt.Sprintf("snapshot: failed to flush writer: %v", err))
	}

	slog.Debug("snapshot: snapshot created")
}

func (cm *ClusterManager) LoadSnapshot(env *environment.Environment) {
	if env.PersistencePath == "" {
		slog.Warn("snapshot: file path not found")
		return
	}

	slog.Debug("snapshot: loading snapshot")

	file, err := os.Open(env.PersistencePath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Error("snapshot: snapshot file not found")
			return
		}
		slog.Error(fmt.Sprintf("snapshot: failed to open snapshot file: %v", err))
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		var length int64
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			slog.Error(fmt.Sprintf("snapshot: failed to read message length: %v", err))
			return
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			slog.Error(fmt.Sprintf("snapshot: failed to read message data: %v", err))
			return
		}

		var cmd commonpb.CommandRequest
		if err := proto.Unmarshal(data, &cmd); err != nil {
			slog.Error(fmt.Sprintf("snapshot: failed to unmarshal command: %v", err))
			continue
		}

		responsibleNodeIDs := cm.HashRing.Get(cmd.Key)
		if cm.NodeID == responsibleNodeIDs[0] {
			cm.RunLocalCommand(&cmd)
		}
	}

	slog.Debug("snapshot: snapshot loaded")
}
