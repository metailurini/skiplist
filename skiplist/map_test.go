package skiplist

import "testing"

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
