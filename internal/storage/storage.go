package storage

import (
	"hash/fnv"
	"runtime"
	"sync"
	"time"
)

type StorageType int

const (
	Hash StorageType = iota
	List
)

type Concurrent struct {
	mu sync.RWMutex
}

type Transient struct {
	expiresAt int64
}

// Storable represents a data structure that can be stored in the DataStore.
type Storable interface {
	Type() StorageType
	Mu() *sync.RWMutex
	ExpiresAt() int64
	SetTtl(ttl time.Duration)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]Storable
}

// DataStore is the unified, thread-safe, sharded key-value store.
type DataStore struct {
	shards          []*shard
	janitor         *janitor
	ShardsCount     uint64
	ShardsPerCursor int
}

// NewDataStore creates a new DataStore with a background cleanup goroutine.
func NewDataStore(shardsPerCursor int, cleanupInterval time.Duration) *DataStore {
	shardsCount := getShardCount()
	ds := &DataStore{
		shards:          make([]*shard, shardsCount),
		ShardsCount:     uint64(shardsCount),
		ShardsPerCursor: shardsPerCursor,
	}

	for i := range shardsCount {
		ds.shards[i] = &shard{
			data: make(map[string]Storable),
		}
	}

	if cleanupInterval > 0 {
		j := &janitor{
			Interval: cleanupInterval,
			stop:     make(chan bool),
		}
		ds.janitor = j
		go j.run(ds)
		runtime.SetFinalizer(ds, stopJanitor)
	}

	return ds
}

// getShardCount calculates the next power of 2 for a given number.
func getShardCount() uint64 {
	n := runtime.NumCPU() * 4
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return uint64(n)
}

func (ds *DataStore) getShard(key string) *shard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return ds.shards[hasher.Sum64()&(ds.ShardsCount-1)]
}

// Get retrieves a storable item from the DataStore.
func (ds *DataStore) Get(key string) (Storable, bool) {
	shard := ds.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, ok := shard.data[key]
	if !ok {
		return nil, false
	}

	now := time.Now().Unix()
	expiresAt := val.ExpiresAt()
	if expiresAt != 0 && now > expiresAt {
		return nil, false
	}

	return val, ok
}

// Set stores an item in the DataStore.
func (ds *DataStore) Set(key string, val Storable) {
	shard := ds.getShard(key)
	shard.mu.Lock()
	shard.data[key] = val
	shard.mu.Unlock()
}

// Del deletes an item from the DataStore.
func (ds *DataStore) Del(key string) {
	shard := ds.getShard(key)
	shard.mu.Lock()
	delete(shard.data, key)
	shard.mu.Unlock()
}

// Scan iterates over all keys in a specific cursor range and calls the callback.
func (ds *DataStore) Scan(cursor int, callback func(key string, val Storable)) {
	now := time.Now().Unix()
	start := cursor * ds.ShardsPerCursor
	end := start + ds.ShardsPerCursor

	if cursor == -1 {
		start = 0
		end = int(ds.ShardsCount)
	}

	if start >= int(ds.ShardsCount) {
		return
	}

	if end > int(ds.ShardsCount) {
		end = int(ds.ShardsCount)
	}

	for i := start; i < end; i++ {
		shard := ds.shards[i]
		shard.mu.RLock()
		for key, val := range shard.data {
			expiresAt := val.ExpiresAt()
			if expiresAt != 0 && now > expiresAt {
				continue
			}
			callback(key, val)
		}
		shard.mu.RUnlock()
	}
}

type janitor struct {
	Interval time.Duration
	stop     chan bool
}

func (j *janitor) run(ds *DataStore) {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			ds.deleteExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func (ds *DataStore) deleteExpired() {
	now := time.Now().Unix()

	for i := 0; i < int(ds.ShardsCount); i++ {
		shard := ds.shards[i]
		shard.mu.Lock()
		for key, storable := range shard.data {
			expiresAt := storable.ExpiresAt()
			storable.Mu().Lock()
			if expiresAt != 0 && now > expiresAt {
				delete(shard.data, key)
			}
			storable.Mu().Unlock()
		}
		shard.mu.Unlock()
	}
}

func stopJanitor(ds *DataStore) {
	ds.janitor.stop <- true
}
