package hashring

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"log/slog"
	"sort"
	"strconv"
	"sync"
)

type HashFunc func(data []byte) uint32

// Consistent Hashing implementation. Dictates how the keys are distributed in the Cluster.
// This algorith minimizes key redistribution if the cluster state changes
type HashRing struct {
	mu          sync.RWMutex
	hash        HashFunc
	vNodeCount  int
	Replicas    int
	keys        []int
	hashMap     map[int]string
	LastVersion uint64
}

// Creates new hashring
func New(vNodeCount int, replicas int, fn HashFunc) *HashRing {
	h := &HashRing{
		vNodeCount: vNodeCount,
		Replicas:   replicas,
		hash:       fn,
		hashMap:    make(map[int]string),
	}
	if h.hash == nil {
		h.hash = crc32.ChecksumIEEE
	}
	return h
}

// Add node to hash ring
func (h *HashRing) Add(nodeIDs ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, node := range nodeIDs {
		for i := 0; i < h.vNodeCount; i++ {
			hash := int(h.hash([]byte(strconv.Itoa(i) + node)))
			h.keys = append(h.keys, hash)
			h.hashMap[hash] = node
		}
	}
	sort.Ints(h.keys)
}

// Removes node from hash ring
func (h *HashRing) Remove(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.vNodeCount; i++ {
		hash := int(h.hash([]byte(strconv.Itoa(i) + nodeID)))
		delete(h.hashMap, hash)

		for j, k := range h.keys {
			if k == hash {
				h.keys = append(h.keys[:j], h.keys[j+1:]...)
				break
			}
		}
	}
}

// Get ID of the responsible node for a key
func (h *HashRing) Get(nodeID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.keys) == 0 {
		return []string{}
	}

	hash := int(h.hash([]byte(nodeID)))
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}

	nodes := make([]string, 0, h.Replicas)
	for i := idx; len(nodes) < h.Replicas && len(nodes) < len(h.keys); i++ {
		if i == len(h.keys) {
			i = 0
		}
		nodes = append(nodes, h.hashMap[h.keys[i]])
	}

	return nodes
}

// GetVersion returns a hash of all the alive peers in the cluster.
func (h *HashRing) GetVersion() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.keys) == 0 {
		slog.Warn("hash ring: error computing hash ring version: no keys found")
		return 0
	}

	hasher := fnv.New64a()
	for _, key := range h.keys {
		err := binary.Write(hasher, binary.BigEndian, int64(key))
		if err != nil {
			slog.Warn(fmt.Sprintf("hash ring: error computing hash ring version: %v", err))
			return 0
		}
	}

	return hasher.Sum64()
}

func (h *HashRing) GetLastVersion() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.LastVersion
}

func (h *HashRing) CommitVersion() {
	currentVersion := h.GetVersion()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.LastVersion = currentVersion
}
