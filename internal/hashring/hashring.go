package hashring

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
)

type vNode struct {
	hash   uint32
	nodeID string
}

// Consistent Hashing implementation. Dictates how the keys are distributed in the Cluster.
// This algorith minimizes key redistribution if the cluster state changes
type HashRing struct {
	mu         sync.RWMutex
	hash       hash.Hash32
	vNodeCount int
	Replicas   int
	vNodes     []vNode
}

// Creates new hashring
func New(vNodeCount int, replicas int) *HashRing {
	if replicas < 0 {
		slog.Error("hashring: invalid number of replicas")
		os.Exit(1)
	}

	return &HashRing{
		vNodeCount: vNodeCount,
		Replicas:   replicas,
		hash:       fnv.New32a(),
	}
}

// Add node to hash ring
func (h *HashRing) Add(nodeIDs ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, node := range nodeIDs {
		for i := 0; i < h.vNodeCount; i++ {
			h.hash.Write([]byte(strconv.Itoa(i) + node))
			hash := h.hash.Sum32()
			h.hash.Reset()
			h.vNodes = append(h.vNodes, vNode{hash: hash, nodeID: node})
		}
	}
	sort.Slice(h.vNodes, func(i, j int) bool { return h.vNodes[i].hash < h.vNodes[j].hash })
}

// Removes node from hash ring
func (h *HashRing) Remove(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newVNodes := make([]vNode, 0, len(h.vNodes))
	for _, v := range h.vNodes {
		if v.nodeID != nodeID {
			newVNodes = append(newVNodes, v)
		}
	}
	h.vNodes = newVNodes
}

// Get ID of the responsible node for a key
func (h *HashRing) Get(key string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vNodes) == 0 {
		return []string{}
	}

	h.hash.Write([]byte(key))
	hash := h.hash.Sum32()
	h.hash.Reset()

	idx := sort.Search(len(h.vNodes), func(i int) bool {
		return h.vNodes[i].hash >= hash
	})
	if idx == len(h.vNodes) {
		idx = 0
	}

	uniqueNodes := make([]string, 0, h.Replicas+1)
	seen := make(map[string]struct{})

	i := idx
	for len(uniqueNodes) < h.Replicas+1 && len(seen) < len(h.vNodes)/h.vNodeCount {
		vNode := h.vNodes[i]
		if _, exists := seen[vNode.nodeID]; !exists {
			seen[vNode.nodeID] = struct{}{}
			uniqueNodes = append(uniqueNodes, vNode.nodeID)
		}
		i++
		if i == len(h.vNodes) {
			i = 0
		}
	}

	return uniqueNodes
}

// GetNodes returns a sorted list of unique node IDs in the hash ring.
func (h *HashRing) GetNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vNodes) == 0 {
		return []string{}
	}

	nodeCount := len(h.vNodes) / h.vNodeCount

	uniqueNodes := make([]string, 0, nodeCount)
	seen := make(map[string]struct{}, nodeCount)

	for _, vNode := range h.vNodes {
		if _, exists := seen[vNode.nodeID]; !exists {
			seen[vNode.nodeID] = struct{}{}
			uniqueNodes = append(uniqueNodes, vNode.nodeID)
		}
	}

	return uniqueNodes
}

// GetVersion returns a hash of all the alive peers in the cluster.
func (h *HashRing) GetVersion() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vNodes) == 0 {
		slog.Warn("hash ring: error computing hash ring version: no keys found")
		return 0
	}

	hasher := fnv.New64a()
	for _, vNode := range h.vNodes {
		err := binary.Write(hasher, binary.BigEndian, int64(vNode.hash))
		if err != nil {
			slog.Warn(fmt.Sprintf("hash ring: error computing hash ring version: %v", err))
			return 0
		}
	}

	return hasher.Sum64()
}

// Returns a copy of the hashring
func (h *HashRing) Copy() *HashRing {
	h.mu.RLock()
	defer h.mu.RUnlock()

	newRing := &HashRing{
		hash:       h.hash,
		vNodeCount: h.vNodeCount,
		Replicas:   h.Replicas,
		vNodes:     make([]vNode, len(h.vNodes)),
	}

	copy(newRing.vNodes, h.vNodes)

	return newRing
}
