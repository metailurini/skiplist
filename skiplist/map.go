package skiplist

import "sync/atomic"

var getAfterFindHook func(node any) bool

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

	loadNextPtr := func(n *node[K, V], level int) **node[K, V] {
		if n == nil {
			return &m.tail
		}
		if level < len(n.next) {
			if succ := n.next[level].Load(); succ != nil {
				return succ
			}
		}
		return &m.tail
	}

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

			if next == m.tail || !m.less(next.key, key) {
				if next != m.tail && next.val.Load() == nil {
					succPtr := loadNextPtr(next, i)
					if !x.next[i].CompareAndSwap(nextPtr, succPtr) {
						continue
					}
					continue
				}
				preds[i] = x
				succs[i] = next
				break
			}

			if next.val.Load() == nil {
				succPtr := loadNextPtr(next, i)
				if !x.next[i].CompareAndSwap(nextPtr, succPtr) {
					continue
				}
				continue
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
func (m *Map[K, V]) Contains(key K) bool {
	_, _, found := m.find(key)
	return found
}

// Set inserts or updates the value for the given key.
func (m *Map[K, V]) Set(key K, value V) {
	for {
		preds, succs, found := m.find(key)
		if found {
			node := succs[0]
			for {
				old := node.val.Load()
				if old == nil {
					break
				}
				newVal := value
				if node.val.CompareAndSwap(old, &newVal) {
					return
				}
			}
			continue
		}

		height := randomLevel()
		valCopy := value
		newNode := newNode(key, &valCopy, height)
		newNodePtr := new(*node[K, V])
		*newNodePtr = newNode

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
				continue
			}
		} else {
			if expected0 != nil && *expected0 != m.tail {
				continue
			}
			succNode0 = m.tail
		}

		newNode.next[0].Store(succPtr0)

		if !pred0.next[0].CompareAndSwap(expected0, newNodePtr) {
			continue
		}

		atomic.AddInt64(&m.length, 1)

		preds, succs, _ = m.find(key)

		for level := 1; level < height; level++ {
			for {
				pred := preds[level]
				if pred == nil {
					pred = m.head
				}
				if level >= len(pred.next) {
					preds, succs, _ = m.find(key)
					continue
				}

				expected := pred.next[level].Load()
				succNode := succs[level]
				succPtr := expected
				if succPtr == nil {
					succPtr = &m.tail
				}

				if succNode != nil && succNode != m.tail {
					if expected == nil || *expected != succNode {
						preds, succs, _ = m.find(key)
						continue
					}
				} else {
					if expected != nil && *expected != m.tail {
						preds, succs, _ = m.find(key)
						continue
					}
					succNode = m.tail
				}

				newNode.next[level].Store(succPtr)

				if pred.next[level].CompareAndSwap(expected, newNodePtr) {
					break
				}

				preds, succs, _ = m.find(key)
			}
		}

		return
	}
}
