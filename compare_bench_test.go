package skiplist

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/metailurini/skiplist/skl"
)

func BenchmarkCompareSkipLists(b *testing.B) {
	distributions := []struct {
		name string
		kind distributionKind
	}{
		{name: "Uniform", kind: distUniform},
		{name: "Ascending", kind: distAscending},
		{name: "Zipfian", kind: distZipf},
	}

	workloads := []struct {
		name         string
		writePercent int
	}{
		{name: "ReadMostly", writePercent: 5},
		{name: "WriteHeavy", writePercent: 90},
		{name: "Mixed", writePercent: 50},
	}

	threadCounts := []int{1, 2, 4, 8, 16, 32}
	const keyRange = 1 << 12

	less := func(a, b int) bool { return a < b }

	for _, dist := range distributions {
		dist := dist
		b.Run(dist.name, func(b *testing.B) {
			for _, workload := range workloads {
				workload := workload
				b.Run(workload.name, func(b *testing.B) {
					for _, threads := range threadCounts {
						threads := threads
						b.Run(fmt.Sprintf("LockFree_P%d", threads), func(b *testing.B) {
							m := New[int, int](less)
							for i := range keyRange / 2 {
								_, _ = m.Put(i, i)
							}

							var ascendingCounter uint64
							var ops int64

							b.ResetTimer()

							var wg sync.WaitGroup
							wg.Add(threads)
							for tIdx := range threads {
								go func(worker int) {
									defer wg.Done()
									seed := int64(worker+1) * 1_000_003
									r := rand.New(rand.NewSource(seed))
									var zipf *rand.Zipf
									if dist.kind == distZipf {
										upper := uint64(keyRange - 1)
										if upper == 0 {
											upper = 1
										}
										zipf = rand.NewZipf(r, 1.2, 1, upper)
									}

									for {
										idx := atomic.AddInt64(&ops, 1)
										if idx > int64(b.N) {
											break
										}

										var key int
										switch dist.kind {
										case distUniform:
											key = r.Intn(keyRange)
										case distAscending:
											key = int(atomic.AddUint64(&ascendingCounter, 1)-1) % keyRange
										case distZipf:
											key = int(zipf.Uint64())
										}

										opChoice := r.Intn(100)
										if opChoice < workload.writePercent {
											if r.Intn(2) == 0 {
												value := r.Intn(1 << 16)
												_, _ = m.Put(key, value)
											} else {
												_, _ = m.Delete(key)
											}
										} else {
											if r.Intn(2) == 0 {
												_, _ = m.Get(key)
											} else {
												_ = m.Contains(key)
											}
										}
									}
								}(tIdx)
							}

							wg.Wait()
							b.StopTimer()
						})

						b.Run(fmt.Sprintf("LockBased_P%d", threads), func(b *testing.B) {
							cfg := skl.NewConfig()
							list, _ := skl.InitSkipList[int, int](cfg)
							for i := range keyRange / 2 {
								list.Put(i, i)
							}

							var mu sync.Mutex
							var ascendingCounter uint64
							var ops int64

							b.ResetTimer()

							var wg sync.WaitGroup
							wg.Add(threads)
							for tIdx := range threads {
								go func(worker int) {
									defer wg.Done()
									seed := int64(worker+1) * 1_000_003
									r := rand.New(rand.NewSource(seed))
									var zipf *rand.Zipf
									if dist.kind == distZipf {
										upper := uint64(keyRange - 1)
										if upper == 0 {
											upper = 1
										}
										zipf = rand.NewZipf(r, 1.2, 1, upper)
									}

									for {
										idx := atomic.AddInt64(&ops, 1)
										if idx > int64(b.N) {
											break
										}

										var key int
										switch dist.kind {
										case distUniform:
											key = r.Intn(keyRange)
										case distAscending:
											key = int(atomic.AddUint64(&ascendingCounter, 1)-1) % keyRange
										case distZipf:
											key = int(zipf.Uint64())
										}

										mu.Lock()
										opChoice := r.Intn(100)
										if opChoice < workload.writePercent {
											if r.Intn(2) == 0 {
												value := r.Intn(1 << 16)
												list.Put(key, value)
											} else {
												_ = list.Remove(key)
											}
										} else {
											if r.Intn(2) == 0 {
												_, _ = list.Get(key)
											} else {
												_, _ = list.Get(key) // for contains-like, check if no error
											}
										}
										mu.Unlock()
									}
								}(tIdx)
							}

							wg.Wait()
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}
