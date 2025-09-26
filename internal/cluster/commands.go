package cluster

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"gokv/internal/hashmap"
	"gokv/internal/hashring"
	"gokv/proto/commonpb"
)

// HGet retrieves data from hashmap and serializes it.
func (cm *ClusterManager) HGet(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" {
		return nil, errors.New("hget: requires 1 or more arguments: hash, ...field")
	}

	keys := make([]string, len(args))
	for i, b := range args {
		keys[i] = string(b)
	}

	entries, err := cm.HashMap.Get(hash, keys...)
	if err != nil {
		return nil, errors.New("hget: hash not found")
	}

	response := commonpb.CommandResponse{}
	kvList := &commonpb.KeyValueList{
		Data: make([]*commonpb.KeyValue, len(entries)),
	}

	i := 0
	for key, entry := range entries {
		kv := &commonpb.KeyValue{
			Key:   key,
			Value: entry.Data,
		}
		kvList.Data[i] = kv
		i++
	}

	response.Response = &commonpb.CommandResponse_List{List: kvList}
	return &response, nil
}

// HSet sets data into the HashMap.
func (cm *ClusterManager) HSet(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, errors.New("hset: requires 3 or 4 arguments: hash, field, value[, ttl]")
	}
	key := string(args[0])
	data := args[1]
	ttl := time.Duration(0)
	if len(args) == 3 {
		duration, err := time.ParseDuration(string(args[2]))
		if err != nil {
			return nil, fmt.Errorf("hset: invalid TTL: %w", err)
		}
		ttl = duration
	}

	cm.HashMap.Set(hash, key, data, ttl)
	response := commonpb.CommandResponse{
		Response: &commonpb.CommandResponse_Success{Success: true},
	}
	return &response, nil
}

// HDel deletes one or more fields from a hash. If no args are passed, it deletes the entire hash.
func (cm *ClusterManager) HDel(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" {
		return nil, errors.New("hdel: requires 1 or more arguments: hash, ...field")
	}

	keys := make([]string, len(args))
	for i, b := range args {
		keys[i] = string(b)
	}

	count := cm.HashMap.Del(hash, keys...)

	response := commonpb.CommandResponse{
		Response: &commonpb.CommandResponse_Count{Count: int64(count)},
	}
	return &response, nil
}

// HScan scans cluster at the specific cursor.
func (cm *ClusterManager) HScan(cur string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if len(args) > 0 {
		return nil, errors.New("hscan: requires 1 argument: cursor")
	}

	cursor, err := strconv.Atoi(cur)
	if err != nil {
		return nil, errors.New("hscan: cursor must be a integer")
	}

	nodeIDs := cm.HashRing.GetNodes()
	cursorPerNode := int(math.Ceil(float64(cm.HashMap.ShardsCount) / float64(cm.HashMap.ShardsPerCursor)))
	totalCursors := cursorPerNode * len(nodeIDs)
	if cursor < 0 || cursor >= totalCursors {
		return nil, errors.New("hscan: invalid cursor")
	}

	nextCursor := cursor + 1
	if nextCursor >= totalCursors {
		nextCursor = 0
	}

	response := commonpb.CommandResponse{}
	kvMap := &commonpb.KeyValueMap{
		Cursor: int64(nextCursor),
		Data:   make(map[string]*commonpb.KeyValueList, 0),
	}

	cm.HashMap.Scan(cursor%cursorPerNode, func(hash string, he *hashmap.HashEntry) {
		responsibleNodeIDs := cm.HashRing.Get(hash)
		if responsibleNodeIDs[0] == cm.NodeID {
			kvList := &commonpb.KeyValueList{
				Data: make([]*commonpb.KeyValue, 0),
			}

			he.Mu.RLock()
			for key, entry := range he.Items {
				kv := &commonpb.KeyValue{
					Key:   key,
					Value: entry.Data,
				}
				kvList.Data = append(kvList.Data, kv)
			}
			he.Mu.RUnlock()

			kvMap.Data[hash] = kvList
		}
	})

	response.Response = &commonpb.CommandResponse_Map{Map: kvMap}
	return &response, err
}

// finds nodeID responsible for the cursor of the HSCAN command
func (cm *ClusterManager) findCursorNode(req *commonpb.CommandRequest, hr *hashring.HashRing) (string, error) {
	cursor, err := strconv.Atoi(req.Key)
	if err != nil {
		return "", errors.New("hscan: cursor must be a integer")
	}

	nodeIDs := hr.GetNodes()
	cursorPerNode := int(math.Ceil(float64(cm.HashMap.ShardsCount) / float64(cm.HashMap.ShardsPerCursor)))
	totalCursors := cursorPerNode * len(nodeIDs)
	if cursor < 0 || cursor >= totalCursors {
		return "", errors.New("hscan: invalid cursor")
	}

	nextCursor := cursor + 1
	if nextCursor >= totalCursors {
		nextCursor = 0
	}

	return nodeIDs[cursor/cursorPerNode], nil
}
