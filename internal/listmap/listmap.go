package listmap

import (
	"container/list"
	"hash/fnv"
	"sync"
)

// ListEntry represents a List with its on .
type ListEntry struct {
	Mu    sync.RWMutex
	Items *list.List // The map of fields in the hash.
}

// shard represents a shard of the list.
type shard struct {
	mu     sync.RWMutex
	hashes map[string]*ListEntry
}

// List is a thread-safe, in-memory map of lists.
type ListMap struct {
	shards          []*shard // The shards of the hash map.
	ShardsCount     uint64
	ShardsPerCursor int
}

// getShard returns the shard for a given key.
func (lm *ListMap) getShard(key string) *shard {
	hasher := fnv.New64a()
	hasher.Write([]byte(key))
	return lm.shards[hasher.Sum64()%lm.ShardsCount]
}

// PushBack adds one or more values to the end of a list.
// It returns true if the operation was successful, false otherwise.
func (lm *ListMap) PushBack(listName string, values ...[]byte) {
	shard := lm.getShard(listName)

	shard.mu.RLock()
	li, ok := shard.hashes[listName]
	shard.mu.RUnlock()

	if !ok {
		shard.mu.Lock()
		li = &ListEntry{
			Items: list.New(),
		}
		shard.hashes[listName] = li
		shard.mu.Unlock()
	}

	li.Mu.Lock()
	defer li.Mu.Unlock()

	for _, value := range values {
		li.Items.PushBack(value)
	}
}

// PushFront adds one or more values to the beginning of a list.
// It returns true if the operation was successful, false otherwise.
func (lm *ListMap) PushFront(listName string, values ...[]byte) {
	shard := lm.getShard(listName)

	shard.mu.RLock()
	li, ok := shard.hashes[listName]
	shard.mu.RUnlock()

	if !ok {
		shard.mu.Lock()
		li = &ListEntry{
			Items: list.New(),
		}
		shard.hashes[listName] = li
		shard.mu.Unlock()
	}

	li.Mu.Lock()
	defer li.Mu.Unlock()

	for _, value := range values {
		li.Items.PushFront(value)
	}
}

// PopBack removes and returns the last `count` elements of a list.
// If the list is empty or does not exist, it returns an empty slice and an error.
func (lm *ListMap) PopBack(listName string, count int) ([][]byte, error) {
	shard := lm.getShard(listName)

	shard.mu.RLock()
	li, ok := shard.hashes[listName]
	shard.mu.RUnlock()

	if !ok {
		return [][]byte{}, nil
	}

	li.Mu.Lock()
	defer li.Mu.Unlock()

	var values [][]byte

	for i := 0; i < count && li.Items.Len() > 0; i++ {
		element := li.Items.Back()
		if element == nil {
			break
		}
		values = append(values, element.Value.([]byte))
		li.Items.Remove(element)
	}

	if li.Items.Len() == 0 {
		shard.mu.Lock()
		delete(shard.hashes, listName)
		shard.mu.Unlock()
	}

	return values, nil
}

// PopFront removes and returns the first `count` elements of a list.
// If the list is empty or does not exist, it returns an empty slice and an error.
func (lm *ListMap) PopFront(listName string, count int) ([][]byte, error) {
	shard := lm.getShard(listName)

	shard.mu.RLock()
	li, ok := shard.hashes[listName]
	shard.mu.RUnlock()

	if !ok {
		return [][]byte{}, nil
	}

	li.Mu.Lock()
	defer li.Mu.Unlock()

	var values [][]byte

	for i := 0; i < count && li.Items.Len() > 0; i++ {
		element := li.Items.Front()
		if element == nil {
			break
		}
		values = append(values, element.Value.([]byte))
		li.Items.Remove(element)
	}

	if li.Items.Len() == 0 {
		shard.mu.Lock()
		delete(shard.hashes, listName)
		shard.mu.Unlock()
	}

	return values, nil
}

// Length returns the number of elements in a list.
// If the list does not exist, it returns 0.
func (lm *ListMap) Length(listName string) int {
	shard := lm.getShard(listName)

	shard.mu.RLock()
	li, ok := shard.hashes[listName]
	shard.mu.RUnlock()

	if !ok {
		return 0
	}

	li.Mu.RLock()
	defer li.Mu.RUnlock()

	return li.Items.Len()
}

// Scan iterates over all lists and calls the callback for each one.
// If the cursor is -1 it will scan the whole ListMap
func (c *ListMap) Scan(cursor int, callback func(listName string, le *ListEntry)) {
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
		for listName, le := range shard.hashes {
			callback(listName, le)
		}
		shard.mu.RUnlock()
	}
}

// NewList creates a new List data structure.
func NewListMap(shardsCount int, shardsPerCursor int) *ListMap {
	c := &ListMap{
		shards:          make([]*shard, shardsCount),
		ShardsCount:     uint64(shardsCount),
		ShardsPerCursor: shardsPerCursor,
	}

	for i := range shardsCount {
		c.shards[i] = &shard{
			hashes: make(map[string]*ListEntry),
		}
	}

	return c
}
