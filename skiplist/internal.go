package skiplist

import (
	"math/bits"
	"math/rand"
	"sync/atomic"
)

// node is a node in the skip list.
type node[K, V any] struct {
	key K
	// val is a pointer to the value.
	// A nil value indicates that the node is logically deleted.
	val  atomic.Pointer[V]
	next []atomic.Pointer[*node[K, V]]
}

const (
	// MaxLevel is the maximum level of the skip list.
	// It is set to 32, which is a common value for skip lists.
	MaxLevel = 32

	// P is the probability of a node being in a higher level.
	// It is set to 1/2, which is a common value for skip lists.
	P = 1.0 / 2.0
)

// randomLevel returns a random level for a new node.
// It uses a fast bit-sampling method to generate a geometric distribution.
func randomLevel() int {
	// See: https://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightMultLookup
	// rand.Uint64() returns a 64-bit unsigned integer.
	// We want to find the number of trailing zeros in this random number.
	// The probability of the last bit being 0 is 1/2.
	// The probability of the last two bits being 0 is 1/4.
	// The probability of the last n bits being 0 is 1/2^n.
	// This gives us a geometric distribution with p=1/2.
	// We use bits.TrailingZeros64 to count the number of trailing zeros.
	// We add 1 to the result because the minimum level is 1.
	level := bits.TrailingZeros64(rand.Uint64()) + 1
	if level > MaxLevel {
		return MaxLevel
	}
	return level
}

// newNode creates a new node with the given key, value, and level.
func newNode[K, V any](key K, val *V, level int) *node[K, V] {
	n := &node[K, V]{
		key:  key,
		next: make([]atomic.Pointer[*node[K, V]], level),
	}
	n.val.Store(val)
	return n
}

// newSentinels creates new head and tail sentinels.
func newSentinels[K, V any]() (*node[K, V], *node[K, V]) {
	head := &node[K, V]{
		next: make([]atomic.Pointer[*node[K, V]], MaxLevel),
	}
	tail := &node[K, V]{}
	for i := range head.next {
		head.next[i].Store(&tail)
	}
	return head, tail
}
