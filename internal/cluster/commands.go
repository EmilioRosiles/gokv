package cluster

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"gokv/internal/hashmap"
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

	data := make(map[string]*commonpb.Value, len(entries))

	i := 0
	for key, entry := range entries {
		data[key] = commonpb.NewBytes(entry.Data)
		i++
	}

	response := commonpb.CommandResponse{Response: commonpb.NewMap(data)}
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
	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
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

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(count))}
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

	data := make(map[string]*commonpb.Value)

	cm.HashMap.Scan(cursor%cursorPerNode, func(hash string, he *hashmap.HashEntry) {
		responsibleNodeIDs := cm.HashRing.Get(hash)
		if responsibleNodeIDs[0] == cm.NodeID {
			entryMap := make(map[string]*commonpb.Value)

			he.Mu.RLock()
			for key, entry := range he.Items {
				entryMap[key] = commonpb.NewBytes(entry.Data)
			}
			he.Mu.RUnlock()

			data[hash] = commonpb.NewMap(entryMap)
		}
	})

	cursorValue := commonpb.NewCursor(uint64(nextCursor), commonpb.NewMap(data))
	response := commonpb.CommandResponse{Response: cursorValue}
	return &response, err
}

// Finds nodeID responsible for the cursor of the HSCAN command
func (cm *ClusterManager) findCursorNode(req *commonpb.CommandRequest) ([]string, error) {
	cursor, err := strconv.Atoi(req.Key)
	if err != nil {
		return []string{}, errors.New("hscan: cursor must be a integer")
	}

	nodeIDs := cm.HashRing.GetNodes()
	cursorPerNode := int(math.Ceil(float64(cm.HashMap.ShardsCount) / float64(cm.HashMap.ShardsPerCursor)))
	totalCursors := cursorPerNode * len(nodeIDs)
	if cursor < 0 || cursor >= totalCursors {
		return []string{}, errors.New("hscan: invalid cursor")
	}

	nextCursor := cursor + 1
	if nextCursor >= totalCursors {
		nextCursor = 0
	}

	return []string{nodeIDs[cursor/cursorPerNode]}, nil
}

func (cm *ClusterManager) LPush(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" {
		return nil, errors.New("lpush: requires 2 or more arguments: name, ...value")
	}

	cm.ListMap.PushFront(listName, args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) LPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, errors.New("lpop: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, fmt.Errorf("lpop: invalid count")
	}

	values, err := cm.ListMap.PopFront(listName, count)
	if err != nil {
		return nil, fmt.Errorf("lpop: list error %w", err)
	}

	data := make([]*commonpb.Value, len(values))
	for i, value := range values {
		data[i] = commonpb.NewBytes(value)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewList(data...)}
	return &response, nil
}

func (cm *ClusterManager) RPush(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" {
		return nil, errors.New("rpush: requires 2 or more arguments: name, ...value")
	}

	cm.ListMap.PushBack(listName, args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) RPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, errors.New("rpop: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, fmt.Errorf("rpop: invalid count")
	}

	values, err := cm.ListMap.PopBack(listName, count)
	if err != nil {
		return nil, fmt.Errorf("rpop: list error, %w", err)
	}

	data := make([]*commonpb.Value, len(values))
	for i, value := range values {
		data[i] = commonpb.NewBytes(value)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewList(data...)}
	return &response, nil
}

func (cm *ClusterManager) LLen(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" {
		return nil, errors.New("llen: requires 1 argument: name")
	}

	len := cm.ListMap.Length(listName)

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(len))}
	return &response, nil
}

func (cm *ClusterManager) LScan(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	return nil, nil
}
