package skiplist

import (
	"sync"
	"testing"
)

func TestIteratorNextTraversesElementsInOrder(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	for _, key := range []int{5, 1, 3} {
		m.Put(key, key*10)
	}

	it := m.Iterator()

	var keys []int
	for it.Next() {
		k := it.Key()
		v := it.Value()
		keys = append(keys, k)
		if expected := k * 10; v != expected {
			t.Fatalf("expected value %d for key %d, got %d", expected, k, v)
		}
	}

	expectedKeys := []int{1, 3, 5}
	if len(keys) != len(expectedKeys) {
		t.Fatalf("expected %d keys from iterator, got %d", len(expectedKeys), len(keys))
	}
	for i, want := range expectedKeys {
		if keys[i] != want {
			t.Fatalf("expected key %d at position %d, got %d", want, i, keys[i])
		}
	}

	if it.Valid() {
		t.Fatalf("expected iterator to be invalid after exhaustion")
	}
}

func TestIteratorSeekGEPositionsCorrectly(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, string](less)

	m.Put(1, "one")
	m.Put(3, "three")
	m.Put(5, "five")

	it := m.Iterator()

	if !it.SeekGE(2) {
		t.Fatalf("expected SeekGE to locate key >= 2")
	}
	if got := it.Key(); got != 3 {
		t.Fatalf("expected key 3 after SeekGE, got %d", got)
	}
	if got := it.Value(); got != "three" {
		t.Fatalf("expected value 'three', got %q", got)
	}

	if !it.Next() {
		t.Fatalf("expected iterator to advance to next element")
	}
	if got := it.Key(); got != 5 {
		t.Fatalf("expected key 5 after Next, got %d", got)
	}

	if it.Next() {
		t.Fatalf("expected iterator to report exhaustion")
	}

	if it.SeekGE(6) {
		t.Fatalf("expected SeekGE beyond last key to report false")
	}
}

func TestIteratorSkipsLogicallyDeletedNodes(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	for i := 1; i <= 3; i++ {
		m.Put(i, i)
	}

	_, succs, found := m.find(2)
	if !found {
		t.Fatalf("expected to locate key 2 for deletion simulation")
	}
	target := succs[0]
	target.val.Store(nil)
	m.metrics.AddLen(-1)

	it := m.Iterator()
	if !it.Next() {
		t.Fatalf("expected iterator to yield first element")
	}
	if got := it.Key(); got != 1 {
		t.Fatalf("expected first key 1, got %d", got)
	}

	if !it.Next() {
		t.Fatalf("expected iterator to skip logically deleted node and continue")
	}
	if got := it.Key(); got != 3 {
		t.Fatalf("expected iterator to skip deleted key and yield 3, got %d", got)
	}

	if it.Next() {
		t.Fatalf("expected iterator to be exhausted after final element")
	}
}

func TestIteratorSeekGESkipsLogicallyDeletedNodes(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	m.Put(1, 1)
	m.Put(2, 2)
	m.Put(3, 3)

	_, succs, found := m.find(2)
	if !found {
		t.Fatalf("expected to locate key 2 for deletion simulation")
	}
	target := succs[0]
	target.val.Store(nil)
	m.metrics.AddLen(-1)

	it := m.Iterator()
	if !it.SeekGE(2) {
		t.Fatalf("expected SeekGE to locate an element >= 2")
	}

	if got := it.Key(); got != 3 {
		t.Fatalf("expected SeekGE to skip deleted key and yield 3, got %d", got)
	}
}

func TestIteratorSkipsMarkersDuringConcurrentDeletion(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	m.Put(1, 1)
	m.Put(2, 2)

	markerReady := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once

	ensureMarkerHook = func(any) {
		once.Do(func() { close(markerReady) })
		<-resume
	}
	defer func() { ensureMarkerHook = nil }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = m.Delete(1)
	}()

	<-markerReady

	it := m.Iterator()
	if !it.Next() {
		t.Fatalf("expected iterator to yield successor during deletion")
	}
	if got := it.Key(); got != 2 {
		t.Fatalf("expected iterator to skip deleted key and marker, got %d", got)
	}

	if it.Next() {
		t.Fatalf("expected no additional elements during concurrent delete")
	}

	close(resume)
	wg.Wait()
}
