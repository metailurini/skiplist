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
