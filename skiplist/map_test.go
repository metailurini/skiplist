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
		keys = append(keys, next.key)
		node = next
	}
	return keys
}
