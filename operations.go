package skiplist

// mutatorImpl groups the mutating algorithms.
type mutatorImpl[K comparable, V any] struct {
	m *SkipListMap[K, V]
}

// put inserts or updates the value for the given key in the skiplist.
// It returns the previous value and true if the key existed, otherwise zero value and false.
func (u *mutatorImpl[K, V]) put(key K, value V) (V, bool) {
	var pendingPtr **node[K, V]
	nextLevel := 1

	for {
		preds, succs, found := u.m.find(key)

		if pendingPtr != nil {
			pending := *pendingPtr

			if succs[0] != pending {
				var zero V
				return zero, false
			}

			done, resumeLevel := u.finishLevels(preds, succs, pendingPtr, nextLevel)
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
					markerPtr := u.ensureMarker(node)
					u.physicalDelete(preds, node, markerPtr)
					break
				}
				if node.val.CompareAndSwap(oldPtr, &value) {
					return *oldPtr, true
				}
			}
			continue
		}

		height := u.m.rng.RandomLevel()
		valCopy := value
		newNode := u.m.acquireNode(key, &valCopy, height)
		pendingPtr = &newNode
		nextLevel = 1

		pred0 := preds[0]
		if pred0 == nil || len(pred0.next) == 0 {
			pred0 = u.m.head
		}

		expected0 := pred0.next[0].Load()
		succNode0 := succs[0]
		succPtr0 := expected0
		if succPtr0 == nil {
			succPtr0 = &u.m.tail
		}

		if succNode0 != nil && succNode0 != u.m.tail {
			if expected0 == nil || *expected0 != succNode0 {
				u.m.metrics.IncInsertCASRetry()
				pendingPtr = nil
				u.m.releaseNode(newNode)
				continue
			}
		} else {
			if expected0 != nil && *expected0 != u.m.tail {
				u.m.metrics.IncInsertCASRetry()
				pendingPtr = nil
				u.m.releaseNode(newNode)
				continue
			}
		}

		newNode.next[0].Store(succPtr0)

		if !pred0.next[0].CompareAndSwap(expected0, pendingPtr) {
			u.m.metrics.IncInsertCASRetry()
			pendingPtr = nil
			u.m.releaseNode(newNode)
			continue
		}

		u.m.metrics.IncInsertCASSuccess()
		u.m.metrics.AddLen(1)

		if height == 1 {
			pendingPtr = nil
			var zero V
			return zero, false
		}

		done, resumeLevel := u.finishLevels(preds, succs, pendingPtr, nextLevel)
		if done {
			var zero V
			return zero, false
		}

		nextLevel = resumeLevel
	}
}

// finishLevels completes the insertion of a new node at higher levels in the skiplist.
// It returns true if done, and the next level to resume from.
func (u *mutatorImpl[K, V]) finishLevels(preds, succs []*node[K, V], pendingPtr **node[K, V], nextLevel int) (bool, int) {
	if pendingPtr == nil {
		return true, 0
	}

	pending := *pendingPtr

	height := len(pending.next)
	for level := nextLevel; level < height; level++ {
		pred := preds[level]
		if pred == nil {
			pred = u.m.head
		}
		if level >= len(pred.next) {
			// The predecessor we observed no longer has this level; retry.
			u.m.metrics.IncInsertCASRetry()
			return false, level
		}

		expected := pred.next[level].Load()
		succNode := succs[level]
		succPtr := expected
		if succPtr == nil {
			succPtr = &u.m.tail
		}

		if succNode != nil && succNode != u.m.tail {
			if expected == nil || *expected != succNode {
				// The snapshot at this level is stale; retry the insertion.
				u.m.metrics.IncInsertCASRetry()
				return false, level
			}
		} else {
			if expected != nil && *expected != u.m.tail {
				u.m.metrics.IncInsertCASRetry()
				return false, level
			}
		}

		pending.next[level].Store(succPtr)

		if putLevelCASHook != nil {
			putLevelCASHook(level, pred, expected, pendingPtr)
		}

		if !pred.next[level].CompareAndSwap(expected, pendingPtr) {
			u.m.metrics.IncInsertCASRetry()
			return false, level
		}
	}

	pendingPtr = nil
	return true, len(pending.next)
}

// logicalDelete marks the value of the target node as deleted.
// It returns the old value and true if successful, otherwise zero value and false.
func (u *mutatorImpl[K, V]) logicalDelete(target *node[K, V]) (V, bool) {
	var zero V
	if target == nil {
		return zero, false
	}
	for {
		cur := target.val.Load()
		if cur == nil {
			return zero, false
		}
		if target.val.CompareAndSwap(cur, nil) {
			u.m.metrics.AddLen(-1)
			return *cur, true
		}
	}
}

// ensureMarker ensures a marker node is placed after the target node for deletion.
// It returns a pointer to the marker node.
func (u *mutatorImpl[K, V]) ensureMarker(target *node[K, V]) **node[K, V] {
	for {
		nextPtr := target.next[0].Load()
		succPtr := nextPtr
		if succPtr == nil {
			succPtr = &u.m.tail
		}
		nextNode := *succPtr
		if nextNode.marker {
			return nextPtr
		}
		marker := u.m.acquireMarker(target.key)
		marker.next[0].Store(succPtr)
		markerPtr := &marker
		if target.next[0].CompareAndSwap(nextPtr, markerPtr) {
			if ensureMarkerHook != nil {
				ensureMarkerHook(target)
			}
			return markerPtr
		}
		marker.next[0].Store(nil)
		u.m.releaseMarkerNode(marker)
	}
}

// physicalDelete removes the target node from the skiplist at all levels.
// It returns true if the deletion should be retried.
func (u *mutatorImpl[K, V]) physicalDelete(preds []*node[K, V], target *node[K, V], markerPtr **node[K, V]) bool {
	succPtr0 := &u.m.tail
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
				succPtr = &u.m.tail
			}
		}

		pred := preds[level]
		if pred == nil {
			pred = u.m.head
		}

		for {
			if level >= len(pred.next) {
				break
			}

			current := pred.next[level].Load()

			var expectedNode *node[K, V]
			if current == nil {
				expectedNode = u.m.tail
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
		pred0 = u.m.head
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

// delete removes the key-value pair for the given key from the skiplist.
// It returns the old value and true if the key existed, otherwise zero value and false.
func (u *mutatorImpl[K, V]) delete(key K) (V, bool) {
	for {
		preds, succs, found := u.m.find(key)
		if !found {
			var zero V
			return zero, false
		}

		target := succs[0]
		oldVal, ok := u.logicalDelete(target)
		if !ok {
			var zero V
			return zero, false
		}
		markerPtr := u.ensureMarker(target)

		if retry := u.physicalDelete(preds, target, markerPtr); retry {
			continue
		}

		u.m.releaseMarkerPtr(markerPtr)
		u.m.releaseNode(target)

		if _, _, verifyFound := u.m.find(key); verifyFound {
			// A concurrent insertion added the key back before the delete
			// could finish. Retry the removal so the delete only reports
			// success once the key is absent.
			continue
		}

		return oldVal, true
	}
}
