package skiplist

import "sync/atomic"

type Metrics struct {
	insertCASRetries   atomic.Int64
	insertCASSuccesses atomic.Int64
	length             atomic.Int64
}

func newMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) IncInsertCASRetry()   { m.insertCASRetries.Add(1) }
func (m *Metrics) IncInsertCASSuccess() { m.insertCASSuccesses.Add(1) }
func (m *Metrics) AddLen(d int64)       { m.length.Add(d) }
func (m *Metrics) Len() int64           { return m.length.Load() }
func (m *Metrics) InsertCASStats() (int64, int64) {
	return m.insertCASRetries.Load(), m.insertCASSuccesses.Load()
}
