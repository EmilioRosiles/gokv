package hashmap

import (
	"errors"
	"hash/fnv"
	"runtime"
	"sync"
	"time"
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

// shard represents a shard of the hash map.
type shard struct {
	mu     sync.RWMutex
	hashes map[string]*HashEntry
}

// HashMap is a thread-safe, in-memory key-value store that supports field-level TTLs.
type HashMap struct {
	shards          []*shard // The shards of the hash map.
	janitor         *janitor // The janitor for cleaning up expired items.
	ShardsCount     uint64
	ShardsPerCursor int
}

// getShard returns the shard for a given key.
func (c *HashMap) getShard(key string) *shard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return c.shards[hasher.Sum64()%c.ShardsCount]
}

// Get retrieves a map of key FieldEntry from the HashMap.
// It returns the data if the field exists and has not expired.
func (c *HashMap) Get(hash string, keys ...string) (map[string]*FieldEntry, error) {
	shard := c.getShard(hash)
	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		return nil, errors.New("hget: hash not found")
	}

	he.Mu.RLock()
	defer he.Mu.RUnlock()

	entries := make(map[string]*FieldEntry, 0)

	if len(keys) == 0 {
		for key, entry := range he.Items {
			if entry.ExpiresAt > 0 && time.Now().Unix() > entry.ExpiresAt {
				continue
			}
			entries[key] = entry
		}

		return entries, nil
	}

	for _, key := range keys {
		entry, ok := he.Items[key]
		if !ok || entry.ExpiresAt > 0 && time.Now().Unix() > entry.ExpiresAt {
			continue
		}
		entries[key] = entry
	}

	return entries, nil
}

// Set sets a field in a hash to a given value with a specific TTL.
// If the hash does not exist, it will be created.
// If ttl is 0, the field will not expire.
func (c *HashMap) Set(hash, key string, data []byte, ttl time.Duration) {
	var expiresAt int64 = 0
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).Unix()
	}

	shard := c.getShard(hash)

	entry := &FieldEntry{
		Data:      data,
		ExpiresAt: expiresAt,
	}

	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		shard.mu.Lock()
		if he, ok = shard.hashes[hash]; !ok {
			he = &HashEntry{Items: make(map[string]*FieldEntry)}
			shard.hashes[hash] = he
		}
		shard.mu.Unlock()
	}

	he.Mu.Lock()
	he.Items[key] = entry
	he.Mu.Unlock()
}

// Del deletes one or more fields from a hash. If no keys are passed, it deletes the entire hash.
func (c *HashMap) Del(hash string, keys ...string) int {
	shard := c.getShard(hash)

	shard.mu.RLock()
	he, ok := shard.hashes[hash]
	shard.mu.RUnlock()

	if !ok {
		return 0
	}

	// If no keys, delete the whole hash.
	if len(keys) == 0 {
		shard.mu.Lock()
		he := shard.hashes[hash]
		he.Mu.RLock()
		count := len(he.Items)
		he.Mu.RUnlock()
		delete(shard.hashes, hash)
		shard.mu.Unlock()
		return count
	}

	// Otherwise, delete specified keys.
	deletedCount := 0
	he.Mu.Lock()
	defer he.Mu.Unlock()
	for _, key := range keys {
		if _, ok := he.Items[key]; ok {
			delete(he.Items, key)
			deletedCount++
		}
	}

	isEmpty := len(he.Items) == 0

	if isEmpty {
		shard.mu.Lock()
		delete(shard.hashes, hash)
		shard.mu.Unlock()
	}

	return deletedCount
}

// Scan iterates over all hashes in the hash map and calls the callback for each pair.
// If the cursor is -1 it will scan the whole HashMap
func (c *HashMap) Scan(cursor int, callback func(hash string, he *HashEntry)) {
	start := cursor * c.ShardsPerCursor
	end := start + c.ShardsPerCursor

	if cursor == -1 {
		start = 0
		end = int(c.ShardsCount)
	}

	if start >= int(c.ShardsCount) {
		return
	}

	if end > int(c.ShardsCount) {
		end = int(c.ShardsCount)
	}

	for i := start; i < end; i++ {
		shard := c.shards[i]
		shard.mu.RLock()
		for hash, he := range shard.hashes {
			callback(hash, he)
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

	for i := 0; i < int(c.ShardsCount); i++ {
		shard := c.shards[i]
		shard.mu.Lock()
		for hash, he := range shard.hashes {
			he.Mu.Lock()
			for key, entry := range he.Items {
				if entry.ExpiresAt != 0 && now > entry.ExpiresAt {
					delete(he.Items, key)
				}
			}

			if len(he.Items) == 0 {
				delete(shard.hashes, hash)
			}
			he.Mu.Unlock()
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
func NewHashMap(cleanupInterval time.Duration, shardsCount int, shardsPerCursor int) *HashMap {
	c := &HashMap{
		shards:          make([]*shard, shardsCount),
		ShardsCount:     uint64(shardsCount),
		ShardsPerCursor: shardsPerCursor,
	}

	for i := range shardsCount {
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

	return c
}
