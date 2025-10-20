package skiplist

import (
	"sync"
	"sync/atomic"
)

// Less is a function that returns true if a is less than b.
type Less[K comparable] func(a, b K) bool

// SkipListMap ties components together and keeps public API unchanged.
type SkipListMap[K comparable, V any] struct {
	less       Less[K]
	head       *node[K, V]
	tail       *node[K, V]
	metrics    *Metrics
	rng        *RNG
	nodePool   sync.Pool
	markerPool sync.Pool
	// hot-path function fields (concrete functions, not interfaces)
	find        func(key K) (preds, succs []*node[K, V], found bool)
	loadNextPtr func(n *node[K, V], level int) **node[K, V]
	advanceFrom func(start *node[K, V]) *node[K, V]
	// mutator groups structural updates; concrete type to avoid interface overhead
	mutator *mutatorImpl[K, V]
}

// New returns a new SkipListMap.
func New[K comparable, V any](less Less[K]) *SkipListMap[K, V] {
	head, tail := newSentinels[K, V]()
	rng := newRNG()
	m := &SkipListMap[K, V]{
		less: less,
		head: head,
		tail: tail,
		rng:  rng,
	}
	m.metrics = newMetrics(rng)
	m.nodePool.New = func() any {
		return &node[K, V]{
			next: make([]atomic.Pointer[*node[K, V]], MaxLevel),
		}
	}
	m.markerPool.New = func() any {
		return &node[K, V]{
			marker: true,
			next:   make([]atomic.Pointer[*node[K, V]], 1),
		}
	}
	// wire function fields to implementation functions
	m.find = m.findImpl
	m.loadNextPtr = m.loadNextPtrImpl
	m.advanceFrom = m.advanceFromImpl
	m.mutator = &mutatorImpl[K, V]{m: m}
	return m
}

// Get returns the value for a key.
// The boolean is true if the key exists, false otherwise.
func (m *SkipListMap[K, V]) Get(key K) (V, bool) {
	_, succs, found := m.find(key)
	if !found {
		var v V
		return v, false
	}
	valPtr := succs[0].val.Load()
	if getAfterFindHook != nil && getAfterFindHook(succs[0]) {
		valPtr = succs[0].val.Load()
	}
	if valPtr == nil {
		var v V
		return v, false
	}
	return *valPtr, true
}

// Contains returns true if the key exists in the skip list.
func (m *SkipListMap[K, V]) Contains(key K) bool {
	_, _, found := m.find(key)
	return found
}

// Put inserts or updates the value for the given key.
// It returns the previous value and a flag indicating whether an existing entry was replaced.
func (m *SkipListMap[K, V]) Put(key K, value V) (V, bool) {
	return m.mutator.put(key, value)
}

// Delete removes the value associated with the given key from the skip list.
// The removal is performed in two phases: logical deletion followed by
// physical unlinking of the node from each level.
func (m *SkipListMap[K, V]) Delete(key K) (V, bool) {
	return m.mutator.delete(key)
}

// SeekGE returns an iterator positioned at the first element whose key is
// greater than or equal to the provided key. The returned iterator is valid
// if and only if such an element exists.
func (m *SkipListMap[K, V]) SeekGE(key K) *Iterator[K, V] {
	it := m.Iterator()
	it.SeekGE(key)
	return it
}

// LenInt64 returns the current length of the skip list as an int64.
func (m *SkipListMap[K, V]) LenInt64() int64 {
	return m.metrics.Len()
}

// InsertCASStats reports the total number of CAS retries and successful
// insertions observed at the skip list's bottom level. These counters enable
// contention analysis in benchmarks.
func (m *SkipListMap[K, V]) InsertCASStats() (retries, successes int64) {
	return m.metrics.InsertCASStats()
}
