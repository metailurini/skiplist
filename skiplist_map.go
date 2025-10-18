package skiplist

import (
	"sync/atomic"
)

// These hooks are intended solely for test instrumentation and must not perform blocking
// or mutating operations that affect production correctness.
var (
	// getAfterFindHook is invoked after finding a node in Get operations.
	getAfterFindHook func(node any) bool

	// ensureMarkerHook is invoked after placing a marker node in ensureMarker.
	ensureMarkerHook func(node any)

	// putLevelCASHook is invoked during a CAS operation when inserting a node at a given level.
	putLevelCASHook func(level int, pred any, expected any, newNodePtr any)
)

// SkipListMap is a concurrent skip list implementation.
type SkipListMap[K comparable, V any] struct {
	less               Less[K]
	head               *node[K, V]
	tail               *node[K, V]
	length             int64
	insertCASRetries   int64
	insertCASSuccesses int64
	rng                atomic.Uint64
}

// New returns a new SkipListMap.
func New[K comparable, V any](less Less[K]) *SkipListMap[K, V] {
	head, tail := newSentinels[K, V]()
	m := &SkipListMap[K, V]{
		less: less,
		head: head,
		tail: tail,
	}
	m.rng.Store(newRandomSeed())
	return m
}

// find returns the predecessors and successors of the given key at each level.
// The returned `found` is true if the key is found in the skip list and is not logically deleted.
func (m *SkipListMap[K, V]) find(key K) (preds, succs []*node[K, V], found bool) {
	preds = make([]*node[K, V], MaxLevel)
	succs = make([]*node[K, V], MaxLevel)

	x := m.head
	for i := MaxLevel - 1; i >= 0; i-- {
		for {
			nextPtr := x.next[i].Load()
			var next *node[K, V]
			if nextPtr != nil {
				next = *nextPtr
			}
			if next == nil {
				next = m.tail
			}

			if next != m.tail {
				if next.marker || next.val.Load() == nil {
					succPtr := m.loadNextPtr(next, i)
					if !x.next[i].CompareAndSwap(nextPtr, succPtr) {
						continue
					}
					continue
				}
			}

			if next == m.tail || !m.less(next.key, key) {
				preds[i] = x
				succs[i] = next
				break
			}

			x = next
		}
	}

	// The candidate node is the successor of the predecessor at the bottom level.
	candidate := succs[0]

	// If the candidate is not the tail and its key matches, we've found it.
	// We also need to check if it's not logically deleted.
	if candidate != nil && candidate != m.tail && candidate.key == key {
		if candidate.val.Load() != nil {
			found = true
		}
	}

	return preds, succs, found
}

// loadNextPtr returns the pointer to the next node at the specified level,
// skipping over marker nodes if necessary.
func (m *SkipListMap[K, V]) loadNextPtr(n *node[K, V], level int) **node[K, V] {
	if n == nil {
		return &m.tail
	}
	if level >= len(n.next) {
		return &m.tail
	}
	succ := n.next[level].Load()
	if succ == nil {
		return &m.tail
	}
	next := *succ
	if next == nil || !next.marker {
		return succ
	}
	// next is a marker
	if level >= len(next.next) {
		return &m.tail
	}
	markerSucc := next.next[level].Load()
	if markerSucc != nil {
		return markerSucc
	}
	return &m.tail
}

// Get returns the value for a key.
func (m *SkipListMap[K, V]) LenInt64() int64 {
	return atomic.LoadInt64(&m.length)
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
	var pendingPtr **node[K, V]
	nextLevel := 1

	for {
		preds, succs, found := m.find(key)

		if pendingPtr != nil {
			pending := *pendingPtr

			if succs[0] != pending {
				pendingPtr = nil
				var zero V
				return zero, false
			}

			done, resumeLevel := m.finishLevels(preds, succs, pendingPtr, nextLevel)
			if done {
				var zero V
				return zero, false
			}

			nextLevel = resumeLevel
			continue
		}

		if found {
			node := succs[0]
			for {
				oldPtr := node.val.Load()
				if oldPtr == nil {
					break
				}
				oldVal := *oldPtr
				newVal := value
				if node.val.CompareAndSwap(oldPtr, &newVal) {
					return oldVal, true
				}
			}
			continue
		}

		height := randomLevel(&m.rng)
		valCopy := value
		newNode := newNode(key, &valCopy, height)
		pendingPtr = &newNode
		nextLevel = 1

		pred0 := preds[0]
		if pred0 == nil || len(pred0.next) == 0 {
			pred0 = m.head
		}

		expected0 := pred0.next[0].Load()
		succNode0 := succs[0]
		succPtr0 := expected0
		if succPtr0 == nil {
			succPtr0 = &m.tail
		}

		if succNode0 != nil && succNode0 != m.tail {
			if expected0 == nil || *expected0 != succNode0 {
				atomic.AddInt64(&m.insertCASRetries, 1)
				pendingPtr = nil
				continue
			}
		} else {
			if expected0 != nil && *expected0 != m.tail {
				atomic.AddInt64(&m.insertCASRetries, 1)
				pendingPtr = nil
				continue
			}
		}

		newNode.next[0].Store(succPtr0)

		if !pred0.next[0].CompareAndSwap(expected0, pendingPtr) {
			atomic.AddInt64(&m.insertCASRetries, 1)
			pendingPtr = nil
			continue
		}

		atomic.AddInt64(&m.insertCASSuccesses, 1)
		atomic.AddInt64(&m.length, 1)

		if height == 1 {
			pendingPtr = nil
			var zero V
			return zero, false
		}

		done, resumeLevel := m.finishLevels(preds, succs, pendingPtr, nextLevel)
		if done {
			var zero V
			return zero, false
		}

		nextLevel = resumeLevel
	}
}

// finishLevels links a pending node at higher levels (above 0) using CAS.
// Returns true on success, or false with resume level on failure.
// Handles concurrency with retries on CAS failures or stale snapshots.
func (m *SkipListMap[K, V]) finishLevels(preds, succs []*node[K, V], pendingPtr **node[K, V], nextLevel int) (bool, int) {
	if pendingPtr == nil {
		return true, 0
	}

	pending := *pendingPtr

	height := len(pending.next)
	for level := nextLevel; level < height; level++ {
		pred := preds[level]
		if pred == nil {
			pred = m.head
		}
		if level >= len(pred.next) {
			// The predecessor we observed no longer has this level; retry.
			atomic.AddInt64(&m.insertCASRetries, 1)
			return false, level
		}

		expected := pred.next[level].Load()
		succNode := succs[level]
		succPtr := expected
		if succPtr == nil {
			succPtr = &m.tail
		}

		if succNode != nil && succNode != m.tail {
			if expected == nil || *expected != succNode {
				// The snapshot at this level is stale; retry the insertion.
				atomic.AddInt64(&m.insertCASRetries, 1)
				return false, level
			}
		} else {
			if expected != nil && *expected != m.tail {
				atomic.AddInt64(&m.insertCASRetries, 1)
				return false, level
			}
		}

		pending.next[level].Store(succPtr)

		if putLevelCASHook != nil {
			putLevelCASHook(level, pred, expected, pendingPtr)
		}

		if !pred.next[level].CompareAndSwap(expected, pendingPtr) {
			atomic.AddInt64(&m.insertCASRetries, 1)
			return false, level
		}
	}

	pendingPtr = nil
	return true, len(pending.next)
}

// Delete removes the value associated with the given key from the skip list.
// The removal is performed in two phases: logical deletion followed by
// physical unlinking of the node from each level.
func (m *SkipListMap[K, V]) Delete(key K) (V, bool) {
	for {
		preds, succs, found := m.find(key)
		if !found {
			var zero V
			return zero, false
		}

		target := succs[0]
		oldVal, ok := m.logicalDelete(target)
		if !ok {
			continue
		}
		markerPtr := m.ensureMarker(target)

		if retry := m.unlinkNode(preds, target, markerPtr); retry {
			continue
		}

		return oldVal, true
	}
}

func (m *SkipListMap[K, V]) logicalDelete(target *node[K, V]) (V, bool) {
	for {
		current := target.val.Load()
		if current == nil {
			var zero V
			return zero, false
		}
		oldVal := *current
		if target.val.CompareAndSwap(current, nil) {
			atomic.AddInt64(&m.length, -1)
			return oldVal, true
		}
	}
}

func (m *SkipListMap[K, V]) ensureMarker(target *node[K, V]) **node[K, V] {
	for {
		nextPtr := target.next[0].Load()
		succPtr := nextPtr
		if succPtr == nil {
			succPtr = &m.tail
		}

		nextNode := *succPtr
		if nextNode.marker {
			return nextPtr
		}

		marker := &node[K, V]{
			key:    target.key,
			next:   make([]atomic.Pointer[*node[K, V]], 1),
			marker: true,
		}
		marker.next[0].Store(succPtr)

		markerPtr := &marker
		if target.next[0].CompareAndSwap(nextPtr, markerPtr) {
			if ensureMarkerHook != nil {
				ensureMarkerHook(target)
			}
			return markerPtr
		}
	}
}

func (m *SkipListMap[K, V]) unlinkNode(preds []*node[K, V], target *node[K, V], markerPtr **node[K, V]) bool {
	succPtr0 := &m.tail
	if markerPtr != nil {
		if marker := *markerPtr; marker != nil && marker.marker {
			if next := marker.next[0].Load(); next != nil {
				succPtr0 = next
			}
		}
	}

	topLevel := len(target.next) - 1
	for level := topLevel; level >= 0; level-- {
		succPtr := succPtr0
		if level > 0 {
			if next := target.next[level].Load(); next != nil {
				succPtr = next
			} else {
				succPtr = &m.tail
			}
		}

		pred := preds[level]
		if pred == nil {
			pred = m.head
		}

		for {
			if level >= len(pred.next) {
				break
			}

			current := pred.next[level].Load()

			var expectedNode *node[K, V]
			if current == nil {
				expectedNode = m.tail
			} else {
				expectedNode = *current
			}

			if expectedNode == target || (level == 0 && expectedNode != nil && expectedNode.marker) {
				if pred.next[level].CompareAndSwap(current, succPtr) {
					break
				}
				continue
			}

			break
		}
	}

	pred0 := preds[0]
	if pred0 == nil {
		pred0 = m.head
	}
	if len(pred0.next) == 0 {
		return false
	}

	nextPtr := pred0.next[0].Load()
	if nextPtr == nil {
		return false
	}

	nextNode := *nextPtr
	if nextNode == target || nextNode.marker {
		return true
	}

	return false
}

func (m *SkipListMap[K, V]) advanceFrom(start *node[K, V]) *node[K, V] {
	base := start
	for {
		if base == nil {
			base = m.head
		}
		if base == nil {
			return nil
		}

		if len(base.next) == 0 {
			return nil
		}

		nextPtr := base.next[0].Load()
		if nextPtr == nil {
			return nil
		}

		next := *nextPtr
		if next == nil {
			if base.next[0].CompareAndSwap(nextPtr, &m.tail) {
				continue
			}
			continue
		}

		if next == m.tail {
			return nil
		}

		if next.marker {
			succPtr := next.next[0].Load()
			if succPtr == nil {
				succPtr = &m.tail
			}
			base.next[0].CompareAndSwap(nextPtr, succPtr)
			continue
		}

		if next.val.Load() == nil {
			m.find(next.key)
			continue
		}

		return next
	}
}

// SeekGE returns an iterator positioned at the first element whose key is
// greater than or equal to the provided key. The returned iterator is valid
// if and only if such an element exists.
func (m *SkipListMap[K, V]) SeekGE(key K) *Iterator[K, V] {
	it := m.Iterator()
	it.SeekGE(key)
	return it
}

// InsertCASStats reports the total number of CAS retries and successful
// insertions observed at the skip list's bottom level. These counters enable
// contention analysis in benchmarks.
func (m *SkipListMap[K, V]) InsertCASStats() (retries, successes int64) {
	return atomic.LoadInt64(&m.insertCASRetries), atomic.LoadInt64(&m.insertCASSuccesses)
}
