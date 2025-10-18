package skiplist

// Iterator provides a forward-only view over the skip list.
type Iterator[K comparable, V any] struct {
	m       *SkipListMap[K, V]
	current *node[K, V]
	key     K
	value   V
	valid   bool
}

// Iterator returns a new iterator positioned before the first element.
func (m *SkipListMap[K, V]) Iterator() *Iterator[K, V] {
	return &Iterator[K, V]{m: m}
}

// Valid reports whether the iterator currently points at an element.
func (it *Iterator[K, V]) Valid() bool {
	if it == nil {
		return false
	}
	return it.valid
}

// Key returns the key at the iterator's current position.
// It should only be called when Valid reports true.
func (it *Iterator[K, V]) Key() K {
	var zero K
	if it == nil || !it.valid {
		return zero
	}
	return it.key
}

// Value returns the value at the iterator's current position.
// It should only be called when Valid reports true.
func (it *Iterator[K, V]) Value() V {
	var zero V
	if it == nil || !it.valid {
		return zero
	}
	return it.value
}

// SeekGE positions the iterator at the first element whose key is
// greater than or equal to the provided key. It returns true if such an
// element exists.
func (it *Iterator[K, V]) SeekGE(key K) bool {
	if it == nil || it.m == nil {
		return false
	}

	it.invalidate()

	_, succs, _ := it.m.find(key)
	current := succs[0]

	for {
		if current == nil || current == it.m.tail {
			return false
		}

		valPtr := current.val.Load()
		if valPtr != nil {
			it.current = current
			it.key = current.key
			it.value = *valPtr
			it.valid = true
			return true
		}

		next := it.m.advanceFrom(current)
		if next == nil {
			return false
		}
		current = next
	}
}

// Next advances the iterator to the next element and reports whether it
// successfully moved forward. If the iterator was not valid prior to the
// call, it advances to the first element.
func (it *Iterator[K, V]) Next() bool {
	if it == nil || it.m == nil {
		return false
	}

	start := it.current
	if !it.valid {
		start = nil
	}

	for {
		next := it.m.advanceFrom(start)
		if next == nil {
			it.invalidate()
			return false
		}

		valPtr := next.val.Load()
		if valPtr == nil {
			// The node was logically deleted before we could observe its value.
			// Continue the traversal from this node to locate the next live one.
			start = next
			continue
		}

		it.current = next
		it.key = next.key
		it.value = *valPtr
		it.valid = true
		return true
	}
}

func (it *Iterator[K, V]) invalidate() {
	if it == nil {
		return
	}
	it.current = nil
	it.valid = false
	var zeroK K
	var zeroV V
	it.key = zeroK
	it.value = zeroV
}
