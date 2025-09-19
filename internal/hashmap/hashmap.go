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
	Data      []byte // The data of the field.
	ExpiresAt int64  // The expiration time of the field in Unix nanoseconds.
}

// hashEntry represents a hash with its own mutex and map of fields.
type hashEntry struct {
	mu    sync.RWMutex
	items map[string]*fieldEntry // The map of fields in the hash.
}

// HashMap is a thread-safe, in-memory key-value store that supports field-level TTLs.
type HashMap struct {
	mu      sync.RWMutex
	hashes  map[string]*hashEntry // The map of hashes.
	janitor *janitor              // The janitor for cleaning up expired items.
}

// HGet retrieves a field from a hash.
// It returns the data if the field exists and has not expired.
func (c *HashMap) HGet(hash string, args ...[]byte) (any, error) {
	if len(args) != 1 {
		return nil, errors.New("hget: requires 1 argument: key")
	}
	key := string(args[0])

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return nil, errors.New("hget: hash not found")
	}

	he.mu.RLock()
	entry, ok := he.items[key]
	he.mu.RUnlock()

	if !ok || entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
		return nil, errors.New("hget: key not found")
	}

	return entry.Data, nil
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
		ttl, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("hset: invalid TTL: %w", err)
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

	// If no args, delete the whole hash.
	if len(args) == 0 {
		c.mu.Lock()
		delete(c.hashes, hash)
		c.mu.Unlock()
		return int64(1), nil
	}

	// Otherwise, delete specified keys.
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
		return nil, errors.New("hgetall: does not take any arguments")
	}

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return &clusterpb.KeyValueList{List: []*clusterpb.KeyValue{}}, nil
	}

	he.mu.RLock()
	defer he.mu.RUnlock()

	kvList := &clusterpb.KeyValueList{
		List: make([]*clusterpb.KeyValue, 0),
	}

	for key, entry := range he.items {
		if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
			continue
		}
		kvList.List = append(kvList.List, &clusterpb.KeyValue{Key: key, Value: entry.Data})
	}

	return kvList, nil
}

// GetAllData returns all the data in the hashmap.
func (c *HashMap) GetAllData() map[string]map[string]*fieldEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data := make(map[string]map[string]*fieldEntry)
	for hash, he := range c.hashes {
		data[hash] = make(map[string]*fieldEntry)
		he.mu.RLock()
		for key, entry := range he.items {
			data[hash][key] = entry
		}
		he.mu.RUnlock()
	}

	return data
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
	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	for hash, he := range c.hashes {
		for key, entry := range he.items {
			if entry.ExpiresAt > 0 && now > entry.ExpiresAt {
				delete(he.items, key)
			}
		}

		if len(he.items) == 0 {
			delete(c.hashes, hash)
		}
	}
}

// stopJanitor stops the cleanup goroutine.
func stopJanitor(c *HashMap) {
	c.janitor.stop <- true
}

// NewHashMap creates a new HashMap with a background cleanup goroutine.
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

	// Register HashMap commands.
	cr.Register("HGET", c.HGet)
	cr.Register("HSET", c.HSet)
	cr.Register("HDEL", c.HDel)
	cr.Register("HGETALL", c.HGetAll)

	return c
}
