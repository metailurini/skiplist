package skiplist

import (
	"math/bits"
	"sync/atomic"
	"time"
)

const defaultSeed = uint64(0xdeadbeefcafebabe)

func newRandomSeed() uint64 {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		seed = defaultSeed
	}
	return seed
}

type RNG struct {
	seed atomic.Uint64
}

func newRNG() *RNG {
	r := &RNG{}
	r.seed.Store(newRandomSeed())
	return r
}

func (r *RNG) nextRandom64() uint64 {
	for {
		current := r.seed.Load()
		if current == 0 {
			r.seed.CompareAndSwap(0, newRandomSeed())
			continue
		}
		x := current
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		if x == 0 {
			x = defaultSeed
		}
		if r.seed.CompareAndSwap(current, x) {
			return x * 2685821657736338717
		}
	}
}

func (r *RNG) RandomLevel() int {
	level := bits.TrailingZeros64(r.nextRandom64()) + 1
	if level > MaxLevel {
		return MaxLevel
	}
	return level
}
