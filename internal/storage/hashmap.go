package storage

import (
	"maps"
	"sync"
	"time"
)

// FieldEntry represents an individual field in a hash, with its own TTL.
type FieldEntry struct {
	Data []byte
}

// Hash represents a hash data structure, implementing Storable.
type HashMap struct {
	Concurrent
	Transient
	items map[string]*FieldEntry
}

// NewHash creates a new Hash.
func NewHash() *HashMap {
	return &HashMap{
		items: make(map[string]*FieldEntry),
	}
}

// Type returns the type of the storable.
func (h *HashMap) Type() StorageType {
	return Hash
}

// Mu returns the mutex of the storable.
func (h *HashMap) Mu() *sync.RWMutex {
	return &h.mu
}

// ExpiresAt returns the expiration time of the storable.
func (h *HashMap) ExpiresAt() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.expiresAt
}

// Get retrieves fields from the hash.
func (h *HashMap) Get(keys ...string) map[string]*FieldEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	entries := make(map[string]*FieldEntry)

	if h.expiresAt > 0 && time.Now().Unix() > h.expiresAt {
		return nil
	}

	for _, key := range keys {
		entry, ok := h.items[key]
		if !ok {
			continue
		}
		entries[key] = entry
	}

	return entries
}

// Set adds or updates a field in the hash.
func (h *HashMap) Set(entries map[string]*FieldEntry) {
	h.mu.Lock()
	maps.Copy(h.items, entries)
	h.mu.Unlock()
}

// Del deletes fields from the hash. Returns the number of deleted fields.
func (h *HashMap) Del(keys ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	deletedCount := 0
	for _, key := range keys {
		if _, ok := h.items[key]; ok {
			delete(h.items, key)
			deletedCount++
		}
	}
	return deletedCount
}

// Len returns the number of keys stored in the hash.
func (h *HashMap) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.items)
}

// Keys returns the list of keys stored in the hash.
func (h *HashMap) Keys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	keys := make([]string, len(h.items))
	i := 0
	for key := range h.items {
		keys[i] = key
		i++
	}
	return keys
}
