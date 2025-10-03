package cluster

import (
	"fmt"
	"strconv"
	"time"

	"gokv/internal/storage"
	"gokv/proto/commonpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Del removes a key from the DataStore.
func (cm *ClusterManager) Del(key string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if key == "" || len(args) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "DEL: requires 1 argument: key")
	}

	count := 0
	_, ok := cm.DataStore.Get(key)
	if ok {
		cm.DataStore.Del(key)
		count = 1
	}

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(count))}
	return &response, nil
}

// Expire adds expiration time to a key from the DataStore.
func (cm *ClusterManager) Expire(key string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if key == "" || len(args) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "EXPIRE: requires 2 arguments: key, ttl")
	}

	store, ok := cm.DataStore.Get(key)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewBool(false)}
		return &response, nil
	}

	ttl, err := time.ParseDuration(string(args[0]))
	if err != nil || ttl < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "EXPIRE: invalid TTL")
	}

	store.SetTtl(ttl)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

// Scan scans cluster keys at the specific cursor.
func (cm *ClusterManager) Scan(cursorStr string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if cursorStr == "" || len(args) > 1 {
		return nil, status.Errorf(codes.InvalidArgument, "SCAN: requires 1 or 2 arguments: cursor, [count]")
	}

	nodeIdx, cursor, err := parseCursor(cursorStr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "SCAN: %s", err.Error())
	}

	if cursor < 0 || cursor >= int(cm.DataStore.ShardsCount) {
		return nil, status.Errorf(codes.InvalidArgument, "SCAN: local cursor")
	}

	count := 4
	if len(args) == 1 {
		count, err = strconv.Atoi(string(args[0]))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "SCAN: invalid count")
		}
	}

	nextCursor := cursor + count
	nextNodeIdx := nodeIdx
	if nextCursor >= int(cm.DataStore.ShardsCount) {
		nextCursor = 0
		nextNodeIdx += 1
		if nextNodeIdx >= len(cm.HashRing.GetNodes()) {
			nextNodeIdx = 0
		}
	}

	data := make([]*commonpb.Value, 0)
	cm.DataStore.Scan(cursor, count, func(hash string, store storage.Storable) {
		responsibleNodeIDs := cm.HashRing.Get(hash)
		if responsibleNodeIDs[0] == cm.NodeID {
			data = append(data, commonpb.NewString(hash))
		}
	})

	nextCursorStr := fmt.Sprintf("%v.%v", nextNodeIdx, nextCursor)
	value := commonpb.NewCursor(nextCursorStr, len(data), commonpb.NewList(data...))
	response := commonpb.CommandResponse{Response: value}
	return &response, nil
}

// HGet retrieves data from hashmap and serializes it.
func (cm *ClusterManager) HGet(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" || len(args) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "HGET: requires 2 or more arguments: hash, field [field ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewNil()}
		return &response, nil
	}

	if store.Type() != storage.Hash {
		return nil, status.Errorf(codes.FailedPrecondition, "HGET: invalid data structure found")
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
		return nil, status.Errorf(codes.InvalidArgument, "HSET: invalid number of arguments: hash, field value [field value ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		store = storage.NewHash()
		cm.DataStore.Set(hash, store)
	}

	if store.Type() != storage.Hash {
		return nil, status.Errorf(codes.FailedPrecondition, "HSET: invalid data structure found")
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

// HDel deletes one or more fields from a hash.
func (cm *ClusterManager) HDel(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" || len(args) != 0 {
		return nil, status.Errorf(codes.InvalidArgument, "HDEL: requires 2 or more arguments: hash, field [field ...]")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewInt(0)}
		return &response, nil
	}

	if store.Type() != storage.Hash {
		return nil, status.Errorf(codes.FailedPrecondition, "HDEL: invalid data structure found")
	}

	keys := make([]string, len(args))
	for i, b := range args {
		keys[i] = string(b)
	}

	count := store.(*storage.HashMap).Del(keys...)
	if store.(*storage.HashMap).Len() == 0 {
		cm.DataStore.Del(hash)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(count))}
	return &response, nil
}

// HKeys returns all the field keys in a hash.
func (cm *ClusterManager) HKeys(hash string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if hash == "" || len(args) != 0 {
		return nil, status.Errorf(codes.InvalidArgument, "HKEYS: requires 1 argument: hash")
	}

	store, ok := cm.DataStore.Get(hash)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewNil()}
		return &response, nil
	}

	if store.Type() != storage.Hash {
		return nil, status.Errorf(codes.FailedPrecondition, "HKEYS: invalid data structure found")
	}

	keys := store.(*storage.HashMap).Keys()
	data := make([]*commonpb.Value, len(keys))
	for i, key := range keys {
		data[i] = commonpb.NewString(key)
	}

	response := commonpb.CommandResponse{Response: commonpb.NewList(data...)}
	return &response, nil
}

func (cm *ClusterManager) LPush(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "LPUSH: requires 2 or more arguments: name, value [value ...]")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		store = storage.NewList()
		cm.DataStore.Set(listName, store)
	}

	if store.Type() != storage.List {
		return nil, status.Errorf(codes.FailedPrecondition, "LPUSH: invalid data structure found")
	}

	store.(*storage.ListMap).PushFront(args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) LPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "LPOP: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "LPOP: invalid count")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewNil()}
		return &response, nil
	}

	if store.Type() != storage.List {
		return nil, status.Errorf(codes.FailedPrecondition, "LPUSH: invalid data structure found")
	}

	values := store.(*storage.ListMap).PopFront(count)
	if store.(*storage.ListMap).Len() == 0 {
		cm.DataStore.Del(listName)
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
		return nil, status.Errorf(codes.InvalidArgument, "RPUSH: requires 2 or more arguments: name, value [value ...]")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		store = storage.NewList()
		cm.DataStore.Set(listName, store)
	}

	if store.Type() != storage.List {
		return nil, status.Errorf(codes.FailedPrecondition, "RPUSH: invalid data structure found")
	}

	store.(*storage.ListMap).PushBack(args...)

	response := commonpb.CommandResponse{Response: commonpb.NewBool(true)}
	return &response, nil
}

func (cm *ClusterManager) RPop(listName string, args ...[]byte) (*commonpb.CommandResponse, error) {
	if listName == "" || len(args) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "RPOP: requires 2 arguments: name, count")
	}

	count, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RPOP: invalid count")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewNil()}
		return &response, nil
	}

	if store.Type() != storage.List {
		return nil, status.Errorf(codes.FailedPrecondition, "RPOP: invalid data structure found")
	}

	values := store.(*storage.ListMap).PopBack(count)
	if store.(*storage.ListMap).Len() == 0 {
		cm.DataStore.Del(listName)
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
		return nil, status.Errorf(codes.InvalidArgument, "LLEN: requires 1 argument: name")
	}

	store, ok := cm.DataStore.Get(listName)
	if !ok {
		response := commonpb.CommandResponse{Response: commonpb.NewInt(0)}
		return &response, nil
	}

	if store.Type() != storage.List {
		return nil, status.Errorf(codes.FailedPrecondition, "LLEN: invalid data structure found")
	}

	len := store.(*storage.ListMap).Len()

	response := commonpb.CommandResponse{Response: commonpb.NewInt(int64(len))}
	return &response, nil
}
