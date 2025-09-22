package hashmap

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"gokv/internal/command"
	"gokv/proto/clusterpb"
	"hash/fnv"
)

// fieldEntry represents an individual field in a hash, with its own TTL.
type FieldEntry struct {
	Data      []byte // The data of the field.
	ExpiresAt int64  // The expiration time of the field in Unix nanoseconds.
}

// hashEntry represents a hash with its own mutex and map of fields.
type HashEntry struct {
	Mu    sync.RWMutex
	Items map[string]*FieldEntry // The map of fields in the hash.
}

const shardCount = 256

// shard represents a shard of the hash map.
type shard struct {
	mu     sync.RWMutex
	hashes map[string]*HashEntry
}

// HashMap is a thread-safe, in-memory key-value store that supports field-level TTLs.
type HashMap struct {
	shards  [shardCount]*shard // The shards of the hash map.
	janitor *janitor           // The janitor for cleaning up expired items.
}

// getShard returns the shard for a given key.
func (c *HashMap) getShard(key string) *shard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return c.shards[hasher.Sum64()%shardCount]
}

// HGet retrieves a field from a hash.
// It returns the data if the field exists and has not expired.
func (c *HashMap) HGet(hash string, args ...[]byte) (any, error) {
	shard := c.getShard(hash)
	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		return nil, errors.New("hget: hash not found")
	}

	he.Mu.RLock()
	defer he.Mu.RUnlock()

	if len(args) == 0 {
		kvList := &clusterpb.KeyValueList{
			List: make([]*clusterpb.KeyValue, 0),
		}

		for key, entry := range he.Items {
			if entry.ExpiresAt > 0 && time.Now().Unix() > entry.ExpiresAt {
				continue
			}
			kvList.List = append(kvList.List, &clusterpb.KeyValue{Key: key, Value: entry.Data})
		}

		return kvList, nil
	}

	if len(args) == 1 {
		entry, ok := he.Items[string(args[0])]
		if !ok || entry.ExpiresAt > 0 && time.Now().Unix() > entry.ExpiresAt {
			return nil, errors.New("hget: key not found")
		}

		return entry.Data, nil
	}

	kvList := &clusterpb.KeyValueList{
		List: make([]*clusterpb.KeyValue, 0),
	}

	for _, arg := range args {
		key := string(arg)
		entry, ok := he.Items[key]
		if !ok || entry.ExpiresAt > 0 && time.Now().Unix() > entry.ExpiresAt {
			continue
		}
		kvList.List = append(kvList.List, &clusterpb.KeyValue{Key: key, Value: entry.Data})
	}
	return kvList, nil
}

// HSet sets a field in a hash to a given value with a specific TTL.
// If the hash does not exist, it will be created.
// If ttl is 0, the field will not expire.
func (c *HashMap) HSet(hash string, args ...[]byte) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, errors.New("hset: requires 2 or 3 arguments: field, value[, ttl]")
	}
	key := string(args[0])
	data := args[1]
	var expiresAt int64 = 0
	if len(args) == 3 {
		var err error
		ttl, err := time.ParseDuration(string(args[2]))
		if err != nil {
			return nil, fmt.Errorf("hset: invalid TTL: %w", err)
		}
		if ttl > 0 {
			expiresAt = time.Now().Add(ttl).Unix()
		}
	}

	entry := &FieldEntry{
		Data:      data,
		ExpiresAt: expiresAt,
	}

	shard := c.getShard(hash)
	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		shard.mu.Lock()
		// Re-check in case another goroutine created it while we waited for the lock.
		if he, ok = shard.hashes[hash]; !ok {
			he = &HashEntry{Items: make(map[string]*FieldEntry)}
			shard.hashes[hash] = he
		}
		shard.mu.Unlock()
	}

	he.Mu.Lock()
	he.Items[key] = entry
	he.Mu.Unlock()
	return true, nil
}

// HDel deletes one or more fields from a hash. If no args are passed, it deletes the entire hash.
func (c *HashMap) HDel(hash string, args ...[]byte) (any, error) {
	shard := c.getShard(hash)
	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		return int64(0), nil
	}

	// If no args, delete the whole hash.
	if len(args) == 0 {
		shard.mu.Lock()
		delete(shard.hashes, hash)
		shard.mu.Unlock()
		return int64(1), nil
	}

	// Otherwise, delete specified keys.
	deletedCount := 0
	he.Mu.Lock()
	for _, keyBytes := range args {
		key := string(keyBytes)
		if _, ok := he.Items[key]; ok {
			delete(he.Items, key)
			deletedCount++
		}
	}
	isEmpty := len(he.Items) == 0
	he.Mu.Unlock()

	if isEmpty {
		shard.mu.Lock()
		delete(shard.hashes, hash)
		shard.mu.Unlock()
	}

	return int64(deletedCount), nil
}

// Scan iterates over all hashes in the hash map and calls the callback for each pair.
func (c *HashMap) ScanHash(callback func(hash string, he *HashEntry)) {
	for i := range shardCount {
		shard := c.shards[i]
		shard.mu.RLock()
		for hash, he := range shard.hashes {
			callback(hash, he)
		}
		shard.mu.RUnlock()
	}
}

// Scan iterates over all hash key-value in the hash map and calls the callback for each pair.
func (c *HashMap) ScanEntry(callback func(hash string, key string, entry *FieldEntry)) {
	for i := range shardCount {
		shard := c.shards[i]
		shard.mu.RLock()
		for hash, he := range shard.hashes {
			he.Mu.RLock()
			for key, entry := range he.Items {
				callback(hash, key, entry)
			}
			he.Mu.RUnlock()
		}
		shard.mu.RUnlock()
	}
}

// janitor manages the cleanup of expired items in the HashMap.
type janitor struct {
	Interval time.Duration // The interval at which to run the cleanup.
	stop     chan bool     // A channel to stop the janitor.
}

// run starts a goroutine that periodically cleans up expired items.
func (j *janitor) run(c *HashMap) {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

// deleteExpired iterates through the cache and removes expired items.
func (c *HashMap) deleteExpired() {
	now := time.Now().Unix()

	for i := range shardCount {
		shard := c.shards[i]
		shard.mu.Lock()
		for hash, he := range shard.hashes {
			for key, entry := range he.Items {
				if entry.ExpiresAt > 0 && now > entry.ExpiresAt {
					delete(he.Items, key)
				}
			}

			if len(he.Items) == 0 {
				delete(shard.hashes, hash)
			}
		}
		shard.mu.Unlock()
	}
}

// stopJanitor stops the cleanup goroutine.
func stopJanitor(c *HashMap) {
	c.janitor.stop <- true
}

// NewHashMap creates a new HashMap with a background cleanup goroutine.
// The cleanupInterval determines how often the janitor checks for expired items.
func NewHashMap(cr *command.CommandRegistry, cleanupInterval time.Duration) *HashMap {
	c := &HashMap{}
	for i := range shardCount {
		c.shards[i] = &shard{
			hashes: make(map[string]*HashEntry),
		}
	}

	if cleanupInterval > 0 {
		j := &janitor{
			Interval: cleanupInterval,
			stop:     make(chan bool),
		}
		c.janitor = j
		go j.run(c)
		runtime.SetFinalizer(c, stopJanitor)
	}

	// Register HashMap commands.
	cr.Register("HGET", command.Command{Run: c.HGet})
	cr.Register("HSET", command.Command{Run: c.HSet, Level: command.Replica})
	cr.Register("HDEL", command.Command{Run: c.HDel, Level: command.Replica})

	return c
}
