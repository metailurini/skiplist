package skiplist

import (
	"math/bits"
	"math/rand"
	"sync"
	"time"
)

type RNG struct {
	pool sync.Pool
	once sync.Once
}

func newRNG() *RNG {
	r := &RNG{}
	r.pool.New = func() any {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return r
}

func newRNGWithSeed(seed int64) *RNG {
	r := &RNG{}
	r.pool.New = func() any {
		return rand.New(rand.NewSource(seed))
	}
	return r
}

func (r *RNG) ensurePool() {
	r.once.Do(func() {
		if r.pool.New == nil {
			r.pool.New = func() any {
				return rand.New(rand.NewSource(time.Now().UnixNano()))
			}
		}
	})
}

func (r *RNG) nextRandom64() uint64 {
	r.ensurePool()
	rr := r.pool.Get().(*rand.Rand)
	v := rr.Uint64()
	r.pool.Put(rr)
	return v
}

func (r *RNG) RandomLevel() int {
	level := bits.TrailingZeros64(r.nextRandom64()) + 1
	if level > MaxLevel {
		return MaxLevel
	}
	return level
}
