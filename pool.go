package skiplist

import "sync/atomic"

func (m *SkipListMap[K, V]) acquireNode(key K, val *V, level int) *node[K, V] {
	n := m.nodePool.Get().(*node[K, V])

	if cap(n.next) < level {
		n.next = make([]atomic.Pointer[*node[K, V]], level)
	} else {
		n.next = n.next[:level]
		for i := range n.next {
			n.next[i].Store(nil)
		}
	}

	n.marker = false
	n.key = key
	n.val.Store(val)
	return n
}

func (m *SkipListMap[K, V]) releaseNode(n *node[K, V]) {
	if n == nil || n == m.head || n == m.tail || n.marker {
		return
	}

	var zeroK K
	n.key = zeroK
	n.marker = false
	n.val.Store(nil)

	if cap(n.next) > 0 {
		n.next = n.next[:cap(n.next)]
		for i := range n.next {
			n.next[i].Store(nil)
		}
	}

	m.nodePool.Put(n)
}

func (m *SkipListMap[K, V]) acquireMarker(key K) *node[K, V] {
	marker := m.markerPool.Get().(*node[K, V])

	if cap(marker.next) == 0 {
		marker.next = make([]atomic.Pointer[*node[K, V]], 1)
	} else {
		marker.next = marker.next[:1]
		marker.next[0].Store(nil)
	}

	marker.marker = true
	marker.key = key
	marker.val.Store(nil)
	return marker
}

func (m *SkipListMap[K, V]) releaseMarkerPtr(markerPtr **node[K, V]) {
	if markerPtr == nil {
		return
	}
	marker := *markerPtr
	m.releaseMarkerNode(marker)
}

func (m *SkipListMap[K, V]) releaseMarkerNode(marker *node[K, V]) {
	if marker == nil || marker == m.head || marker == m.tail || !marker.marker {
		return
	}

	marker.marker = false
	marker.val.Store(nil)
	if cap(marker.next) == 0 {
		marker.next = make([]atomic.Pointer[*node[K, V]], 1)
	} else {
		marker.next = marker.next[:cap(marker.next)]
		for i := range marker.next {
			marker.next[i].Store(nil)
		}
	}

	m.markerPool.Put(marker)
}
