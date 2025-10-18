package skiplist

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// node is a node in the skip list.
type node[K, V any] struct {
	key K
	// val is a pointer to the value.
	// A nil value indicates that the node is logically deleted.
	val  atomic.Pointer[V]
	next []atomic.Pointer[*node[K, V]]
	// marker indicates whether this node is a marker node used during deletion.
	marker bool
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
func randomLevel(seed *atomic.Uint64) int {
	level := bits.TrailingZeros64(nextRandom64(seed)) + 1
	if level > MaxLevel {
		return MaxLevel
	}
	return level
}

const defaultSeed = uint64(0xdeadbeefcafebabe)

func nextRandom64(seed *atomic.Uint64) uint64 {
	for {
		current := seed.Load()
		if current == 0 {
			newSeed := newRandomSeed()
			if seed.CompareAndSwap(0, newSeed) {
				current = newSeed
			} else {
				continue
			}
		}

		x := current
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		if x == 0 {
			x = defaultSeed
		}

		if seed.CompareAndSwap(current, x) {
			return x * 2685821657736338717
		}
	}
}

func newRandomSeed() uint64 {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		seed = defaultSeed
	}
	return seed
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
