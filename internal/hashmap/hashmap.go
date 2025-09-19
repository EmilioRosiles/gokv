package hashmap

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gokv/internal/command"
	"gokv/proto/clusterpb"
)

// fieldEntry represents an individual field in a hash, with its own TTL.
type fieldEntry struct {
	Data      []byte
	ExpiresAt int64
}

// hashEntry represents a hash with its own mutex and map of fields.
type hashEntry struct {
	mu    sync.RWMutex
	items map[string]*fieldEntry
}

// HashMap is a thread-safe, in-memory key-value store.
// It supports field-level TTLs.
type HashMap struct {
	mu      sync.RWMutex
	hashes  map[string]*hashEntry
	janitor *janitor
}

// HGet retrieves a field from a hash.
// It returns the data and true if the field exists and has not expired.
func (c *HashMap) HGet(hash string, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, errors.New("HGET requires 1 argument: key")
	}
	key := string(args[0])

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return nil, errors.New("HGET hash not found")
	}

	he.mu.RLock()
	entry, ok := he.items[key]
	he.mu.RUnlock()

	if !ok || entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
		return nil, errors.New("HGET key not found")
	}

	return entry.Data, nil
}

// HSet sets a field in a hash to a given value with a specific TTL.
// If the hash does not exist, it will be created.
// If ttl is 0, the field will not expire.
func (c *HashMap) HSet(hash string, args ...[]byte) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, errors.New("HSET requires 2 or 3 arguments: field, value[, ttl]")
	}
	key := string(args[0])
	data := args[1]
	var expiresAt int64 = 0
	if len(args) == 3 {
		var err error
		ttl, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("HSET invalid TTL: %w", err)
		}
		if ttl > 0 {
			expiresAt = time.Now().Unix() + ttl
		}
	}

	entry := &fieldEntry{
		Data:      data,
		ExpiresAt: expiresAt,
	}

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		he = &hashEntry{items: make(map[string]*fieldEntry)}
		c.mu.Lock()
		c.hashes[hash] = he
		c.mu.Unlock()
	}

	he.mu.Lock()
	he.items[key] = entry
	he.mu.Unlock()
	return true, nil
}

// HDel deletes one or more fields from a hash. If no args are passed, it deletes the entire hash.
func (c *HashMap) HDel(hash string, args ...[]byte) (any, error) {
	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return int64(0), nil
	}

	// If no args, delete the whole hash
	if len(args) == 0 {
		c.mu.Lock()
		delete(c.hashes, hash)
		c.mu.Unlock()
		return int64(1), nil
	}

	// Otherwise, delete specified keys
	deletedCount := 0
	he.mu.Lock()
	for _, keyBytes := range args {
		key := string(keyBytes)
		if _, ok := he.items[key]; ok {
			delete(he.items, key)
			deletedCount++
		}
	}
	isEmpty := len(he.items) == 0
	he.mu.Unlock()

	if isEmpty {
		c.mu.Lock()
		delete(c.hashes, hash)
		c.mu.Unlock()
	}

	return int64(deletedCount), nil
}

// HGetAll retrieves all fields and values from a hash.
func (c *HashMap) HGetAll(hash string, args ...[]byte) (any, error) {
	if len(args) != 0 {
		return nil, errors.New("HGETALL does not take any arguments")
	}

	results := c.getAll(hash)
	if results == nil {
		return &clusterpb.KeyValueList{List: []*clusterpb.KeyValue{}}, nil
	}

	kvList := &clusterpb.KeyValueList{
		List: make([]*clusterpb.KeyValue, 0, len(results)),
	}

	for key, entry := range results {
		kvList.List = append(kvList.List, &clusterpb.KeyValue{Key: key, Value: entry.Data})
	}

	return kvList, nil
}

// HScan performs a scan of the entire hashmap.
func (c *HashMap) HScan(key string, args ...[]byte) (any, error) {
	if len(args) != 0 {
		return nil, errors.New("HSCAN does not take any arguments")
	}

	hashKeys := c.scanKeys()
	responseMap := &clusterpb.KeyValueMap{
		Map: make(map[string]*clusterpb.KeyValueList),
	}

	for _, hashKey := range hashKeys {
		fields := c.getAll(hashKey)
		if fields == nil {
			continue
		}

		kvList := &clusterpb.KeyValueList{
			List: make([]*clusterpb.KeyValue, 0, len(fields)),
		}

		for key, entry := range fields {
			kvList.List = append(kvList.List, &clusterpb.KeyValue{Key: key, Value: entry.Data})
		}
		responseMap.Map[hashKey] = kvList
	}

	return responseMap, nil
}

// getAll retrieves all non-expired fields and their values from a hash.
func (c *HashMap) getAll(hash string) map[string]*fieldEntry {
	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return nil
	}

	he.mu.RLock()
	defer he.mu.RUnlock()

	results := make(map[string]*fieldEntry)
	for key, entry := range he.items {
		if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
			continue
		}
		results[key] = entry
	}

	return results
}

// scanKeys retrieves all keys from the cache.
func (c *HashMap) scanKeys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.hashes))
	for k := range c.hashes {
		keys = append(keys, k)
	}
	return keys
}

// Janitor struct to hold cleanup properties.
type janitor struct {
	Interval time.Duration
	stop     chan bool
}

// runJanitor starts a goroutine that periodically cleans up expired items.
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
	now := time.Now().UnixNano()
	c.mu.RLock()
	hashes := c.scanKeys()
	c.mu.RUnlock()

	for _, hash := range hashes {
		c.mu.RLock()
		he, ok := c.hashes[hash]
		c.mu.RUnlock()

		if !ok {
			continue
		}

		he.mu.Lock()
		for key, entry := range he.items {
			if entry.ExpiresAt > 0 && now > entry.ExpiresAt {
				delete(he.items, key)
			}
		}
		isEmtpy := len(he.items) == 0
		he.mu.Unlock()

		if isEmtpy {
			c.mu.Lock()
			delete(c.hashes, hash)
			c.mu.Unlock()
		}
	}
}

// stopJanitor stops the cleanup goroutine.
func stopJanitor(c *HashMap) {
	c.janitor.stop <- true
}

// NewHashMap creates a new cache with a background cleanup goroutine.
// The cleanupInterval determines how often the janitor checks for expired items.
func NewHashMap(cr *command.CommandRegistry, cleanupInterval time.Duration) *HashMap {
	c := &HashMap{
		hashes: make(map[string]*hashEntry),
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

	// Register HashMap commands
	cr.Register("HGET", c.HGet)
	cr.Register("HSET", c.HSet)
	cr.Register("HDEL", c.HDel)
	cr.Register("HGETALL", c.HGetAll)
	cr.Register("HSCAN", c.HScan)

	return c
}
