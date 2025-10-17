package skiplist

import (
	"sync"
	"testing"
)

func TestGetHandlesLogicalDeletionBetweenFindAndLoad(t *testing.T) {
	m := New[int, int](func(a, b int) bool { return a < b })

	value := 42
	node := newNode[int, int](1, &value, 1)
	node.next[0].Store(&m.tail)
	successor := node
	m.head.next[0].Store(&successor)
	m.length = 1

	getAfterFindHook = func(any) bool {
		node.val.Store(nil)
		return true
	}
	defer func() { getAfterFindHook = nil }()

	got, ok := m.Get(1)
	if ok {
		t.Fatalf("expected Get to report missing key after logical deletion")
	}
	if got != 0 {
		t.Fatalf("expected zero value after logical deletion, got %d", got)
	}
}

func TestFindSkipsLogicallyDeletedNodes(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	v1 := 1
	n1 := newNode(1, &v1, 1)
	v2 := 2
	n2 := newNode(2, &v2, 1)

	n1.next[0].Store(&n2)
	n2.next[0].Store(&m.tail)
	m.head.next[0].Store(&n1)

	// Logically delete the first node.
	n1.val.Store(nil)

	preds, succs, found := m.find(2)
	if !found {
		t.Fatalf("expected key 2 to be found")
	}
	if preds[0] != m.head {
		t.Fatalf("expected predecessor to be head after unlink, got %v", preds[0])
	}
	if succs[0] != n2 {
		t.Fatalf("expected successor to be the live node, got %v", succs[0])
	}
	if gotPtr := m.head.next[0].Load(); gotPtr == nil || *gotPtr != n2 {
		t.Fatalf("expected head to point to live successor, got %v", gotPtr)
	}

	if m.Contains(1) {
		t.Fatalf("expected deleted key to be ignored by Contains")
	}
	if !m.Contains(2) {
		t.Fatalf("expected Contains to find the live key")
	}
}

func TestFindHelpsUnlinkMarkersDuringConcurrentDeletion(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	v1 := 1
	target := newNode(1, &v1, 1)
	v2 := 2
	successor := newNode(2, &v2, 1)

	target.next[0].Store(&successor)
	successor.next[0].Store(&m.tail)
	m.head.next[0].Store(&target)
	m.length = 2

	markerReady := make(chan struct{})
	resumeDelete := make(chan struct{})

	ensureMarkerHook = func(any) {
		select {
		case <-markerReady:
		default:
			close(markerReady)
		}
		<-resumeDelete
	}
	defer func() { ensureMarkerHook = nil }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Delete(1)
	}()

	<-markerReady

	done := make(chan struct{})
	go func() {
		defer close(done)
		if !m.Contains(2) {
			t.Errorf("expected Contains to locate successor during concurrent delete")
		}
	}()
	<-done

	headNextPtr := m.head.next[0].Load()
	if headNextPtr == nil {
		t.Fatalf("expected head to reference a successor node")
	}
	headNext := *headNextPtr
	if headNext == nil {
		t.Fatalf("expected head to reference a concrete node")
	}
	if headNext.marker {
		t.Fatalf("expected head to skip marker node, still observed marker")
	}
	if headNext.key != 2 {
		t.Fatalf("expected head to point to successor key 2, got %v", headNext.key)
	}

	close(resumeDelete)
	wg.Wait()

	if m.Contains(1) {
		t.Fatalf("expected deleted key to remain absent after helping traversal")
	}
}

func TestSetInsertsAndRetrievesValue(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, string](less)

	m.Set(10, "ten")

	if !m.Contains(10) {
		t.Fatalf("expected Contains to report inserted key")
	}

	got, ok := m.Get(10)
	if !ok {
		t.Fatalf("expected Get to find inserted key")
	}
	if got != "ten" {
		t.Fatalf("expected value 'ten', got %q", got)
	}

	if gotLen := m.Len(); gotLen != 1 {
		t.Fatalf("expected length 1 after single insert, got %d", gotLen)
	}
}

func TestSetUpdatesExistingKey(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, string](less)

	m.Set(5, "first")
	m.Set(5, "second")

	if gotLen := m.Len(); gotLen != 1 {
		t.Fatalf("expected length to remain 1 after duplicate insert, got %d", gotLen)
	}

	got, ok := m.Get(5)
	if !ok {
		t.Fatalf("expected Get to find updated key")
	}
	if got != "second" {
		t.Fatalf("expected updated value 'second', got %q", got)
	}
}

func TestSetConcurrentUniqueInserts(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const goroutines = 8
	const perGoroutine = 512

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		start := g * perGoroutine
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := base + i
				m.Set(key, key*2)
			}
		}(start)
	}
	wg.Wait()

	expectedLen := goroutines * perGoroutine
	if gotLen := m.Len(); gotLen != expectedLen {
		keys := collectIntKeys(m)
		t.Logf("collected %d keys", len(keys))
		if len(keys) > 32 {
			keys = keys[:32]
		}
		t.Fatalf("expected length %d after concurrent inserts, got %d; sample keys: %v", expectedLen, gotLen, keys)
	}

	for i := 0; i < expectedLen; i++ {
		got, ok := m.Get(i)
		if !ok {
			keys := collectIntKeys(m)
			t.Logf("collected %d keys", len(keys))
			present := false
			for _, k := range keys {
				if k == i {
					present = true
					break
				}
			}
			if len(keys) > 32 {
				keys = keys[:32]
			}
			t.Fatalf("missing key %d after concurrent inserts; present=%v; sample keys: %v", i, present, keys)
		}
		if got != i*2 {
			t.Fatalf("expected value %d for key %d, got %d", i*2, i, got)
		}
	}
}

func TestSetConcurrentDuplicateInserts(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	const goroutines = 16

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		value := g
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			m.Set(42, v)
		}(value)
	}
	wg.Wait()

	if gotLen := m.Len(); gotLen != 1 {
		t.Fatalf("expected length 1 after duplicate inserts, got %d", gotLen)
	}
	if !m.Contains(42) {
		t.Fatalf("expected key 42 to be present after duplicate inserts")
	}
	got, ok := m.Get(42)
	if !ok {
		t.Fatalf("expected Get to find key 42 after duplicate inserts")
	}
	if got < 0 || got >= goroutines {
		t.Fatalf("unexpected value %d for key 42", got)
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	m.Set(1, 10)
	m.Set(2, 20)
	m.Set(3, 30)

	m.Delete(2)

	if m.Contains(2) {
		t.Fatalf("expected key 2 to be removed")
	}

	if _, ok := m.Get(2); ok {
		t.Fatalf("expected Get to report missing key after delete")
	}

	if !m.Contains(1) || !m.Contains(3) {
		t.Fatalf("expected neighboring keys to remain after delete")
	}

	if got := m.Len(); got != 2 {
		t.Fatalf("expected length 2 after delete, got %d", got)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	m.Set(42, 1)
	m.Delete(42)
	m.Delete(42)

	if m.Contains(42) {
		t.Fatalf("expected key to remain absent after repeated deletes")
	}

	if got := m.Len(); got != 0 {
		t.Fatalf("expected length 0 after repeated deletes, got %d", got)
	}
}

func TestDeleteConcurrentNeighborInserts(t *testing.T) {
	less := func(a, b int) bool { return a < b }
	m := New[int, int](less)

	m.Set(0, 0)
	m.Set(1, 1)
	m.Set(2, 2)

	var wg sync.WaitGroup
	const iterations = 512

	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			m.Set(-1, i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			m.Set(3, i)
		}
	}()
	go func() {
		defer wg.Done()
		m.Delete(1)
	}()
	wg.Wait()

	if m.Contains(1) {
		t.Fatalf("expected deleted key to remain absent after concurrent operations")
	}

	if !m.Contains(0) || !m.Contains(2) {
		t.Fatalf("expected neighboring keys to remain present after concurrent operations")
	}
}

func collectIntKeys(m *Map[int, int]) []int {
	keys := make([]int, 0)
	for node := m.head; ; {
		nextPtr := node.next[0].Load()
		if nextPtr == nil {
			break
		}
		next := *nextPtr
		if next == nil || next == m.tail {
			break
		}
		if next.val.Load() != nil {
			keys = append(keys, next.key)
		}
		node = next
	}
	return keys
}
