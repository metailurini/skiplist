package skiplist

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestConcurrentMixedOperationsStorm(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const keySpace = 128
	goroutines := 2 * runtime.GOMAXPROCS(0)
	if goroutines < 4 {
		goroutines = 4
	}
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
			for i := 0; i < operationsPerGoroutine; i++ {
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
	for it.Next() {
		observed[it.Key()] = it.Value()
	}

	modelMu.Lock()
	expected := make(map[int]int, len(model))
	for k, v := range model {
		expected[k] = v
	}
	expectedLen := len(model)
	modelMu.Unlock()

	if got := m.Len(); got != int64(expectedLen) {
		t.Fatalf("expected length %d after storm, got %d", expectedLen, got)
	}
	if len(observed) != expectedLen {
		t.Fatalf("expected %d keys, iterator observed %d", expectedLen, len(observed))
	}
	for k, v := range expected {
		got, ok := observed[k]
		if !ok || got != v {
			t.Fatalf("model/key mismatch for %d: want %d, got %d (present=%v)", k, v, got, ok)
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
		for i := 0; i < iterations; i++ {
			_, _ = m.Delete(1)
		}
	}()

	close(start)
	wg.Wait()

	if got := m.Len(); got < 0 {
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
	for i := 0; i < totalKeys; i++ {
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

	if got := m.Len(); got != 0 {
		t.Fatalf("expected map to be empty after cascading deletes, got %d", got)
	}

	if it := m.SeekGE(0); it.Valid() {
		t.Fatalf("expected no keys after full deletion, found key %d", it.Key())
	}
}
