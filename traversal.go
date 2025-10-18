package skiplist

// findImpl: simplified traversal helper; mirrors original behavior but kept concise.
func (m *SkipListMap[K, V]) findImpl(key K) (preds, succs []*node[K, V], found bool) {
	preds = make([]*node[K, V], MaxLevel)
	succs = make([]*node[K, V], MaxLevel)

	x := m.head
	for i := MaxLevel - 1; i >= 0; i-- {
		for {
			ptr := x.next[i].Load()
			var next *node[K, V]
			if ptr != nil {
				next = *ptr
			}
			if next == nil {
				next = m.tail
			}

			// Skip markers or logically deleted nodes (help unlinking).
			if next != m.tail {
				if next.marker || next.val.Load() == nil {
					succPtr := m.loadNextPtr(next, i)
					if !x.next[i].CompareAndSwap(ptr, succPtr) {
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

	candidate := succs[0]
	if candidate != nil && candidate != m.tail && candidate.key == key {
		if candidate.val.Load() != nil {
			found = true
		}
	}
	return
}

func (m *SkipListMap[K, V]) loadNextPtrImpl(n *node[K, V], level int) **node[K, V] {
	if n == nil || level >= len(n.next) {
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
	if level >= len(next.next) {
		return &m.tail
	}
	markerSucc := next.next[level].Load()
	if markerSucc != nil {
		return markerSucc
	}
	return &m.tail
}

func (m *SkipListMap[K, V]) advanceFromImpl(start *node[K, V]) *node[K, V] {
	base := start
	for {
		if base == nil {
			base = m.head
		}
		if base == nil || len(base.next) == 0 {
			return nil
		}
		ptr := base.next[0].Load()
		if ptr == nil {
			return nil
		}
		next := *ptr
		if next == nil {
			if base.next[0].CompareAndSwap(ptr, &m.tail) {
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
			base.next[0].CompareAndSwap(ptr, succPtr)
			continue
		}
		if next.val.Load() == nil {
			m.find(next.key)
			continue
		}
		return next
	}
}
