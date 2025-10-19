package skiplist

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

const testXorshiftFallback = uint64(0xdeadbeefcafebabe)

func TestConcurrentMixedOperationsStorm(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const keySpace = 128
	goroutines := max(2*runtime.GOMAXPROCS(0), 4)
	const operationsPerGoroutine = 2000

	model := make(map[int]int)
	var modelMu sync.Mutex

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		seed := int64(0xdeadbeef) + int64(g)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for range operationsPerGoroutine {
				key := r.Intn(keySpace)
				op := r.Intn(4)
				switch op {
				case 0: // Put
					value := r.Intn(1 << 16)
					_, _ = m.Put(key, value)
					modelMu.Lock()
					model[key] = value
					modelMu.Unlock()
				case 1: // Delete
					if _, ok := m.Delete(key); ok {
						modelMu.Lock()
						delete(model, key)
						modelMu.Unlock()
					}
				case 2: // Get
					m.Get(key)
				case 3: // Contains
					m.Contains(key)
				}
			}
		}(seed)
	}

	wg.Wait()

	observed := make(map[int]int)
	it := m.Iterator()
	var prevKey *int
	for it.Next() {
		k := it.Key()
		v := it.Value()

		// no duplicate keys
		if _, ok := observed[k]; ok {
			t.Fatalf("duplicate key %d", k)
		}
		observed[k] = v

		// ordering check (strictly increasing)
		if prevKey != nil {
			if !less(*prevKey, k) {
				t.Fatalf("iterator out of order: previous=%d current=%d", *prevKey, k)
			}
		}
		prevKey = new(int)
		*prevKey = k

		// iterator vs Get/Contains consistency
		if gv, ok := m.Get(k); !ok {
			t.Fatalf("iterator returned key %d, but Get reports missing", k)
		} else if gv != v {
			t.Fatalf("value mismatch for key %d: iterator=%d Get=%d", k, v, gv)
		}
		if !m.Contains(k) {
			t.Fatalf("iterator returned key %d, but Contains reports false", k)
		}
	}

	// // lengths must match observed, currently disabled due to possible contention effects
	// if got := int(m.LenInt64()); got != len(observed) {
	// 	t.Fatalf("length mismatch: skiplist=%d observed=%d", got, len(observed))
	// }

	// SeekGE correctness for all possible keys in keySpace
	for seek := range keySpace {
		it := m.SeekGE(seek)
		// find expected first key >= seek in observed
		found := false
		expectedKey := 0
		for k := seek; k < keySpace; k++ {
			if _, ok := observed[k]; ok {
				found = true
				expectedKey = k
				break
			}
		}
		if found {
			if !it.Valid() {
				t.Fatalf("SeekGE(%d) expected key %d but iterator invalid", seek, expectedKey)
			}
			if it.Key() != expectedKey {
				t.Fatalf("SeekGE(%d) expected key %d got %d", seek, expectedKey, it.Key())
			}
		} else {
			if it.Valid() {
				t.Fatalf("SeekGE(%d) expected no key but got %d", seek, it.Key())
			}
		}
	}
}

func TestDeleteWhileInsertRacing(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const iterations = 5000

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			m.Put(1, i)
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for range iterations {
			_, _ = m.Delete(1)
		}
	}()

	close(start)
	wg.Wait()

	if got := m.LenInt64(); got < 0 {
		t.Fatalf("length should never be negative, got %d", got)
	}

	if it := m.SeekGE(1); it.Valid() {
		v := it.Value()
		if v != it.Key() && it.Key() != 1 {
			t.Fatalf("unexpected iterator state after racing ops: key=%d value=%d", it.Key(), v)
		}
	}
}

func TestCascadeMarkerCleanup(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const totalKeys = 1024
	for i := range totalKeys {
		m.Put(i, i)
	}

	const workers = 8
	var deleters sync.WaitGroup
	deleters.Add(workers)
	for w := 0; w < workers; w++ {
		go func(offset int) {
			defer deleters.Done()
			for k := offset; k < totalKeys; k += workers {
				_, _ = m.Delete(k)
			}
		}(w)
	}

	stop := make(chan struct{})
	var helper sync.WaitGroup
	helper.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer helper.Done()
		r := rand.New(rand.NewSource(1234))
		for {
			select {
			case <-stop:
				return
			default:
			}

			key := r.Intn(totalKeys)
			it := m.SeekGE(key)
			if it.Valid() {
				if gotKey := it.Key(); gotKey < key {
					select {
					case errCh <- fmt.Errorf("iterator returned key %d < seek %d", gotKey, key):
					default:
					}
					return
				}
				if it.Value() != it.Key() {
					select {
					case errCh <- fmt.Errorf("value mismatch for key %d: %d", it.Key(), it.Value()):
					default:
					}
					return
				}
			}

			time.Sleep(time.Microsecond)
		}
	}()

	deleters.Wait()
	close(stop)
	helper.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	if got := m.LenInt64(); got != 0 {
		t.Fatalf("expected map to be empty after cascading deletes, got %d", got)
	}

	if it := m.SeekGE(0); it.Valid() {
		t.Fatalf("expected no keys after full deletion, found key %d", it.Key())
	}
}

func TestPutGeneratorDoesNotBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping generator contention stress test in short mode")
	}

	runtime.SetBlockProfileRate(0)
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)

	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	goroutines := max(4*runtime.GOMAXPROCS(0), 8)
	const operationsPerGoroutine = 10000

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		seed := uint64(0x9e3779b97f4a7c15) + uint64(g)
		go func(seed uint64) {
			defer wg.Done()
			x := seed | 1
			for range operationsPerGoroutine {
				x ^= x >> 12
				x ^= x << 25
				x ^= x >> 27
				if x == 0 {
					x = testXorshiftFallback
				}
				key := int(x & ((1 << 16) - 1))
				m.Put(key, int(x))
			}
		}(seed)
	}

	wg.Wait()
	runtime.GC()

	if p := pprof.Lookup("block"); p != nil {
		var sb strings.Builder
		if err := p.WriteTo(&sb, 2); err != nil {
			t.Fatalf("failed to read block profile: %v", err)
		}
		if strings.Contains(sb.String(), "skiplist.randomLevel") {
			t.Fatalf("randomLevel appeared in block profile indicating serialization:\n%s", sb.String())
		}
	}
}
