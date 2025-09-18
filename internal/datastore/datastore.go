package datastore

import (
	"runtime"
	"sync"
	"time"
)

// fieldEntry represents an individual field in a hash, with its own TTL.
type fieldEntry[V any] struct {
	data      V
	expiresAt int64
}

// hashEntry represents a hash with its own mutex and map of fields.
type hashEntry[C comparable, V any] struct {
	mu    sync.RWMutex
	items map[C]*fieldEntry[V]
}

// Datastore is a thread-safe, in-memory key-value store.
// It supports field-level TTLs.
type Datastore[K comparable, C comparable, V any] struct {
	mu      sync.RWMutex
	hashes  map[K]*hashEntry[C, V]
	janitor *janitor[K, C, V]
}

// HSet sets a field in a hash to a given value with a specific TTL.
// If the hash does not exist, it will be created.
// If ttl is 0, the field will not expire.
func (c *Datastore[K, C, V]) HSet(hash K, key C, data V, ttl time.Duration) {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	entry := &fieldEntry[V]{
		data:      data,
		expiresAt: expiresAt,
	}

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		he = &hashEntry[C, V]{items: make(map[C]*fieldEntry[V])}
		c.mu.Lock()
		c.hashes[hash] = he
		c.mu.Unlock()
	}

	he.mu.Lock()
	he.items[key] = entry
	he.mu.Unlock()
}

// HGet retrieves a field from a hash.
// It returns the data and true if the field exists and has not expired.
// Otherwise, it returns zero value for V and false.
func (c *Datastore[K, C, V]) HGet(hash K, key C) (V, bool) {
	var zeroV V

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return zeroV, false
	}

	he.mu.RLock()
	entry, ok := he.items[key]
	he.mu.RUnlock()

	if !ok {
		return zeroV, false
	}

	if entry.expiresAt > 0 && time.Now().UnixNano() > entry.expiresAt {
		return zeroV, false
	}

	return entry.data, true
}

// HDel deletes one or more fields from a hash.
func (c *Datastore[K, C, V]) HDel(hash K, keys ...C) {
	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return
	}

	he.mu.Lock()
	for _, key := range keys {
		delete(he.items, key)
	}
	isEmtpy := len(he.items) == 0
	he.mu.Unlock()

	if isEmtpy {
		c.mu.Lock()
		delete(c.hashes, hash)
		c.mu.Unlock()
	}
}

// HDel deletes all the fields in a hash.
func (c *Datastore[K, C, V]) HDelAll(hash K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.hashes, hash)
}

// HGetAll retrieves all non-expired fields and their values from a hash.
func (c *Datastore[K, C, V]) HGetAll(hash K) []V {
	var results []V

	c.mu.RLock()
	he, ok := c.hashes[hash]
	c.mu.RUnlock()

	if !ok {
		return results
	}

	he.mu.RLock()
	defer he.mu.RUnlock()

	for _, entry := range he.items {
		if entry.expiresAt > 0 && time.Now().UnixNano() > entry.expiresAt {
			continue
		}
		results = append(results, entry.data)
	}

	return results
}

// HScan retrieves all keys from the cache.
func (c *Datastore[K, C, V]) HScan() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.hashes))
	for k := range c.hashes {
		keys = append(keys, k)
	}
	return keys
}

// Janitor struct to hold cleanup properties.
type janitor[K comparable, C comparable, V any] struct {
	Interval time.Duration
	stop     chan bool
}

// runJanitor starts a goroutine that periodically cleans up expired items.
func (j *janitor[K, C, V]) run(c *Datastore[K, C, V]) {
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
func (c *Datastore[K, C, V]) deleteExpired() {
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
			if entry.expiresAt > 0 && now > entry.expiresAt {
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
func stopJanitor[K comparable, C comparable, V any](c *Datastore[K, C, V]) {
	c.janitor.stop <- true
}

// NewDatastore creates a new cache with a background cleanup goroutine.
// The cleanupInterval determines how often the janitor checks for expired items.
func NewDatastore[K comparable, C comparable, V any](cleanupInterval time.Duration) *Datastore[K, C, V] {
	c := &Datastore[K, C, V]{
		hashes: make(map[K]*hashEntry[C, V]),
	}

	if cleanupInterval > 0 {
		j := &janitor[K, C, V]{
			Interval: cleanupInterval,
			stop:     make(chan bool),
		}
		c.janitor = j
		go j.run(c)
		runtime.SetFinalizer(c, stopJanitor[K, C, V])
	}

	return c
}
