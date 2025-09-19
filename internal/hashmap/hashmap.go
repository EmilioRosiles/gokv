package hashmap

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gokv/internal/command"
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

// HSet sets a field in a hash to a given value with a specific TTL.
// If the hash does not exist, it will be created.
// If ttl is 0, the field will not expire.
func (c *HashMap) HSet(hash string, args ...[]byte) ([]byte, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, errors.New("HSET requires 2 or 3 arguments: field, value[, ttl]")
	}
	key := string(args[0])
	data := args[0]
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
	return nil, nil
}

// HGet retrieves a field from a hash.
// It returns the data and true if the field exists and has not expired.
// Otherwise, it returns zero value for V and false.
func (c *HashMap) HGet(hash string, args ...[]byte) ([]byte, error) {
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

// HDel deletes one or more fields from a hash.
func (c *HashMap) HDel(hash string, args ...[]byte) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("HDEL requires 1 argument: key")
	}
	key := string(args[0])

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return nil, errors.New("HDEL hash not found")
	}

	he.mu.Lock()
	delete(he.items, key)
	isEmtpy := len(he.items) == 0
	he.mu.Unlock()

	if isEmtpy {
		c.mu.Lock()
		delete(c.hashes, hash)
		c.mu.Unlock()
	}
	return nil, nil
}

// HDel deletes all the fields in a hash.
func (c *HashMap) HDelAll(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.hashes, hash)
}

// HGetAll retrieves all non-expired fields and their values from a hash.
func (c *HashMap) HGetAll(hash string) map[string]*fieldEntry {
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

// HScan retrieves all keys from the cache.
func (c *HashMap) HScan() []string {
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
	hashes := c.HScan()
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

	return c
}
