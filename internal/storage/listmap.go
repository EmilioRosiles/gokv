package storage

import (
	"container/list"
	"sync"
	"time"
)

// List represents a list data structure, implementing Storable.
type ListMap struct {
	Concurrent
	Transient
	Items *list.List
}

// NewList creates a new List.
func NewList() *ListMap {
	return &ListMap{
		Items: list.New(),
	}
}

// Type returns the type of the storable.
func (lm *ListMap) Type() StorageType {
	return List
}

// Mu returns the mutex of the storable.
func (lm *ListMap) Mu() *sync.RWMutex {
	return &lm.mu
}

// ExpiresAt returns the expiration time of the storable.
func (lm *ListMap) ExpiresAt() int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.expiresAt
}

func (lm *ListMap) SetTtl(ttl time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.expiresAt = time.Now().Add(ttl).Unix()
}

// PushFront adds values to the front of the list.
func (lm *ListMap) PushFront(values ...[]byte) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, value := range values {
		lm.Items.PushFront(value)
	}
}

// PushBack adds values to the back of the list.
func (lm *ListMap) PushBack(values ...[]byte) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, value := range values {
		lm.Items.PushBack(value)
	}
}

// PopFront removes and returns the first `count` elements.
func (lm *ListMap) PopFront(count int) [][]byte {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var values [][]byte
	for i := 0; i < count && lm.Items.Len() > 0; i++ {
		element := lm.Items.Front()
		values = append(values, element.Value.([]byte))
		lm.Items.Remove(element)
	}
	return values
}

// PopBack removes and returns the last `count` elements.
func (lm *ListMap) PopBack(count int) [][]byte {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var values [][]byte
	for i := 0; i < count && lm.Items.Len() > 0; i++ {
		element := lm.Items.Back()
		values = append(values, element.Value.([]byte))
		lm.Items.Remove(element)
	}
	return values
}

// Len returns the number of items in the list.
func (lm *ListMap) Len() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.Items.Len()
}
