package skiplist

import (
	"math/bits"
	"runtime"
	"sync/atomic"
)

type metricShard struct {
	insertCASRetries   atomic.Int64
	insertCASSuccesses atomic.Int64
	length             atomic.Int64
	// Pad to cache line size to prevent false sharing.
	_ [40]byte
}

type Metrics struct {
	shards []metricShard
	mask   uint32
	rng    *RNG
}

func newMetrics(rng *RNG) *Metrics {
	shardCount := 1
	if rng != nil {
		shardCount = runtime.GOMAXPROCS(0)
		if shardCount < 1 {
			shardCount = 1
		}
		shardCount = nextPowerOfTwo(shardCount)
	}
	return &Metrics{
		shards: make([]metricShard, shardCount),
		mask:   uint32(shardCount - 1),
		rng:    rng,
	}
}

func nextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}

func (m *Metrics) shard() *metricShard {
	if len(m.shards) == 1 || m.rng == nil {
		return &m.shards[0]
	}
	idx := uint32(m.rng.nextRandom64()) & m.mask
	return &m.shards[idx]
}

func (m *Metrics) IncInsertCASRetry() {
	m.shard().insertCASRetries.Add(1)
}

func (m *Metrics) IncInsertCASSuccess() {
	m.shard().insertCASSuccesses.Add(1)
}

func (m *Metrics) AddLen(d int64) {
	m.shard().length.Add(d)
}

func (m *Metrics) Len() int64 {
	var total int64
	for i := range m.shards {
		total += m.shards[i].length.Load()
	}
	return total
}

func (m *Metrics) InsertCASStats() (int64, int64) {
	var retries, successes int64
	for i := range m.shards {
		retries += m.shards[i].insertCASRetries.Load()
		successes += m.shards[i].insertCASSuccesses.Load()
	}
	return retries, successes
}
