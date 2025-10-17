package skiplist

import "sync/atomic"

// Map is a concurrent skip list implementation.
type Map[K comparable, V any] struct {
	less   Less[K]
	head   *node[K, V]
	tail   *node[K, V]
	length int64
}

// New returns a new skip list.
func New[K comparable, V any](less Less[K]) *Map[K, V] {
	head, tail := newSentinels[K, V]()
	return &Map[K, V]{
		less: less,
		head: head,
		tail: tail,
	}
}

// find returns the predecessors and successors of the given key at each level.
// The returned `found` is true if the key is found in the skip list and is not logically deleted.
func (m *Map[K, V]) find(key K) (preds, succs []*node[K, V], found bool) {
	preds = make([]*node[K, V], MaxLevel)
	succs = make([]*node[K, V], MaxLevel)

	x := m.head
	for i := MaxLevel - 1; i >= 0; i-- {
		for {
			var next *node[K, V] = *(x.next[i].Load())
			if next == m.tail || !m.less(next.key, key) {
				break
			}
			x = next
		}
		preds[i] = x
		succs[i] = *(x.next[i].Load())
	}

	// The candidate node is the successor of the predecessor at the bottom level.
	candidate := succs[0]

	// If the candidate is not the tail and its key matches, we've found it.
	// We also need to check if it's not logically deleted.
	if candidate != m.tail && candidate.key == key {
		if candidate.val.Load() != nil {
			found = true
		}
	}

	return preds, succs, found
}

func (m *Map[K, V]) Len() int {
	return int(atomic.LoadInt64(&m.length))
}

// Get returns the value for a key.
// The boolean is true if the key exists, false otherwise.
func (m *Map[K, V]) Get(key K) (V, bool) {
	_, succs, found := m.find(key)
	if !found {
		var v V
		return v, false
	}
	return *succs[0].val.Load(), true
}

// Contains returns true if the key exists in the skip list.
func (m *Map[K, V]) Contains(key K) bool {
	_, _, found := m.find(key)
	return found
}

// Set inserts a new key-value pair into the skip list.
// If the key already exists, the value is updated.
func (m *Map[K, V]) Set(key K, value V) {
	preds, succs, found := m.find(key)
	if found {
		// The key already exists, so we just need to update the value.
		succs[0].val.Store(&value)
		return
	}

	// The key doesn't exist, so we need to insert a new node.
	level := randomLevel()
	newNode := newNode(key, &value, level)

	// Link the new node's next pointers to the successors.
	for i := range level {
		newNode.next[i].Store(&succs[i])
	}

	// Atomically insert the new node at the bottom level.
	// This is the linearization point.
	for {
		if preds[0].next[0].CompareAndSwap(&succs[0], &newNode) {
			break
		}
		// The CAS failed, so we need to restart the find operation.
		// This can happen if another thread inserted a node in between.
		preds, succs, found = m.find(key)
		if found {
			// Key was inserted by another goroutine, update value and return.
			succs[0].val.Store(&value)
			return
		}
		// Re-link the new node's next pointers to the new successors.
		for i := range level {
			newNode.next[i].Store(&succs[i])
		}
	}

	// Link the higher levels.
	for i := 1; i < level; i++ {
		for {
			if preds[i].next[i].CompareAndSwap(&succs[i], &newNode) {
				break
			}
			// If the CAS fails, it means another thread has modified the list.
			// We need to re-scan the level to find the correct predecessors and successors.
			preds, succs, _ = m.find(key)
			// Re-link the new node's next pointers to the new successors.
			newNode.next[i].Store(&succs[i])
		}
	}
	atomic.AddInt64(&m.length, 1)
}
