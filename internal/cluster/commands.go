package cluster

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"gokv/internal/storage"
	"gokv/proto/commonpb"
)

// Scan scans cluster keys at the specific cursor.
func (cm *ClusterManager) Scan(cur string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if len(args) > 0 {
		return nil, errors.New("HSCAN: requires 1 argument: cursor")
	}

	cursor, err := strconv.Atoi(cur)
	if err != nil {
		return nil, errors.New("HSCAN: cursor must be a integer")
	}

	nodeIDs := cm.HashRing.GetNodes()
	cursorPerNode := int(math.Ceil(float64(cm.DataStore.ShardsCount) / float64(cm.DataStore.ShardsPerCursor)))
	totalCursors := cursorPerNode * len(nodeIDs)
	if cursor < 0 || cursor >= totalCursors {
		return nil, errors.New("HSCAN: invalid cursor")
	}

	nextCursor := cursor + 1
	if nextCursor >= totalCursors {
		nextCursor = 0
	}

	data := make([]*commonpb.Value, 0)

	cm.DataStore.Scan(cursor%cursorPerNode, func(hash string, store storage.Storable) {
		responsibleNodeIDs := cm.HashRing.Get(hash)
		if responsibleNodeIDs[0] == cm.NodeID {
			data = append(data, commonpb.NewString(hash))
		}
	})

	cursorValue := commonpb.NewCursor(uint64(nextCursor), commonpb.NewList(data...))
	response := commonpb.CommandResponse{Response: cursorValue}
	return &response, err
}

// Finds nodeID responsible for the cursor of the SCAN command
func (cm *ClusterManager) findCursorNode(req *commonpb.CommandRequest) ([]string, error) {
	cursor, err := strconv.Atoi(req.Key)
	if err != nil {
		return []string{}, errors.New("HSCAN: cursor must be a integer")
	}

	nodeIDs := cm.HashRing.GetNodes()
	cursorPerNode := int(math.Ceil(float64(cm.DataStore.ShardsCount) / float64(cm.DataStore.ShardsPerCursor)))
	totalCursors := cursorPerNode * len(nodeIDs)
	if cursor < 0 || cursor >= totalCursors {
		return []string{}, errors.New("HSCAN: invalid cursor")
	}

	nextCursor := cursor + 1
	if nextCursor >= totalCursors {
		nextCursor = 0
	}

	return []string{nodeIDs[cursor/cursorPerNode]}, nil
}

// HGet retrieves data from hashmap and serializes it.
func (cm *ClusterManager) HGet(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" || len(args) == 0 {
		return nil, errors.New("HGET: requires 2 or more arguments: hash, field [field ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		return nil, errors.New("HGET: hash not found")
	}

	if store.Type() != storage.Hash {
		return nil, errors.New("HGET: invalid data structure found")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	entries := store.(*storage.HashMap).Get(keys...)

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
	if hash == "" || len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("HSET: invalid number of arguments: hash, field value [field value ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		return nil, errors.New("HSET: hash not found")
	}

	if store.Type() != storage.Hash {
		return nil, errors.New("HSET: invalid data structure found")
	}

	data := make(map[string]*storage.FieldEntry, len(args)/2)
	for i := 0; i < len(args)-1; i += 2 {
		key := string(args[i])
		value := args[i+1]
		data[key] = &storage.FieldEntry{Data: value}
	}

	store.(*storage.HashMap).Set(data)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

// HDel deletes one or more fields from a hash. If no args are passed, it deletes the entire hash.
func (cm *ClusterManager) HDel(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" || len(args) != 0 {
		return nil, errors.New("HDEL: requires 2 or more arguments: hash, field [field ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		return nil, errors.New("HDEL: hash not found")
	}

	if store.Type() != storage.Hash {
		return nil, errors.New("HDEL: invalid data structure found")
	}

	keys := make([]string, len(args))
	for i, b := range args {
		keys[i] = string(b)
	}

	count := store.(*storage.HashMap).Del(keys...)

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(count))}
	return &response, nil
}

func (cm *ClusterManager) LPush(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) == 0 {
		return nil, errors.New("LPUSH: requires 2 or more arguments: name, value [value ...]")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		return nil, errors.New("LPUSH: list not found")
	}

	if store.Type() != storage.List {
		return nil, errors.New("LPUSH: invalid data structure found")
	}

	store.(*storage.ListMap).PushFront(args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) LPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, errors.New("LPOP: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, fmt.Errorf("LPOP: invalid count")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		return nil, errors.New("LPUSH: list not found")
	}

	if store.Type() != storage.List {
		return nil, errors.New("LPUSH: invalid data structure found")
	}

	values := store.(*storage.ListMap).PopFront(count)
	if err != nil {
		return nil, fmt.Errorf("LPOP: list error %w", err)
	}

	data := make([]*commonpb.Value, len(values))
	for i, value := range values {
		data[i] = commonpb.NewBytes(value)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewList(data...)}
	return &response, nil
}

func (cm *ClusterManager) RPush(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) == 0 {
		return nil, errors.New("RPUSH: requires 2 or more arguments: name, value [value ...]")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		return nil, errors.New("RPUSH: list not found")
	}

	if store.Type() != storage.List {
		return nil, errors.New("RPUSH: invalid data structure found")
	}

	store.(*storage.ListMap).PushBack(args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) RPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, errors.New("RPOP: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, fmt.Errorf("RPOP: invalid count")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		return nil, errors.New("RPOP: list not found")
	}

	if store.Type() != storage.List {
		return nil, errors.New("RPOP: invalid data structure found")
	}

	values := store.(*storage.ListMap).PopBack(count)
	if err != nil {
		return nil, fmt.Errorf("RPOP: list error %w", err)
	}

	data := make([]*commonpb.Value, len(values))
	for i, value := range values {
		data[i] = commonpb.NewBytes(value)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewList(data...)}
	return &response, nil
}

func (cm *ClusterManager) LLen(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 0 {
		return nil, errors.New("LLEN: requires 1 argument: name")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		return nil, errors.New("LLEN: list not found")
	}

	if store.Type() != storage.List {
		return nil, errors.New("LLEN: invalid data structure found")
	}

	len := store.(*storage.ListMap).Len()

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(len))}
	return &response, nil
}
