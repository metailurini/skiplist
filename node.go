package skiplist

import "sync/atomic"

// node holds key/value and per-level next pointers.
type node[K, V any] struct {
	key K
	// val is a pointer to the value. A nil value indicates that the node is logically deleted.
	val    atomic.Pointer[V]
	next   []atomic.Pointer[*node[K, V]]
	marker bool
}

const (
	MaxLevel = 32
	P        = 1.0 / 2.0
)

func newNode[K, V any](key K, val *V, level int) *node[K, V] {
	n := &node[K, V]{
		key:  key,
		next: make([]atomic.Pointer[*node[K, V]], level),
	}
	n.val.Store(val)
	return n
}

func newSentinels[K, V any]() (*node[K, V], *node[K, V]) {
	head := &node[K, V]{next: make([]atomic.Pointer[*node[K, V]], MaxLevel)}
	tail := &node[K, V]{}
	for i := range head.next {
		head.next[i].Store(&tail)
	}
	return head, tail
}
