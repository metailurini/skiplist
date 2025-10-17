package skiplist

import "sync/atomic"

var getAfterFindHook func(node any) bool
var ensureMarkerHook func(node any)

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
				if next := *succ; next != nil && next.marker {
					if level < len(next.next) {
						if markerSucc := next.next[level].Load(); markerSucc != nil {
							return markerSucc
						}
					}
					return &m.tail
				}
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

			if next != m.tail {
				if next.marker {
					succPtr := loadNextPtr(next, i)
					if !x.next[i].CompareAndSwap(nextPtr, succPtr) {
						continue
					}
					continue
				}

				if next.val.Load() == nil {
					succPtr := loadNextPtr(next, i)
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
		newNodePtr := &newNode

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

		restart := false
		for level := 1; level < height; level++ {
			pred := preds[level]
			if pred == nil {
				pred = m.head
			}
			if level >= len(pred.next) {
				restart = true
				break
			}

			expected := pred.next[level].Load()
			succNode := succs[level]
			succPtr := expected
			if succPtr == nil {
				succPtr = &m.tail
			}

			if succNode != nil && succNode != m.tail {
				if expected == nil || *expected != succNode {
					restart = true
					break
				}
			} else {
				if expected != nil && *expected != m.tail {
					restart = true
					break
				}
				succNode = m.tail
			}

			newNode.next[level].Store(succPtr)

			if !pred.next[level].CompareAndSwap(expected, newNodePtr) {
				restart = true
				break
			}
		}

		if restart {
			continue
		}

		return
	}
}

// Delete removes the value associated with the given key from the skip list.
// The removal is performed in two phases: logical deletion followed by
// physical unlinking of the node from each level.
func (m *Map[K, V]) Delete(key K) {
	for {
		preds, succs, found := m.find(key)
		if !found {
			return
		}

		target := succs[0]

		m.logicalDelete(target)
		markerPtr := m.ensureMarker(target)

		if retry := m.unlinkNode(preds, target, markerPtr); retry {
			continue
		}

		return
	}
}

func (m *Map[K, V]) logicalDelete(target *node[K, V]) bool {
	for {
		current := target.val.Load()
		if current == nil {
			return false
		}
		if target.val.CompareAndSwap(current, nil) {
			atomic.AddInt64(&m.length, -1)
			return true
		}
	}
}

func (m *Map[K, V]) ensureMarker(target *node[K, V]) **node[K, V] {
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

func (m *Map[K, V]) unlinkNode(preds []*node[K, V], target *node[K, V], markerPtr **node[K, V]) bool {
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
