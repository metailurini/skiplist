package skiplist

// Less is a function that returns true if a is less than b.
type Less[K comparable] func(a, b K) bool

// SkipList is the interface for a skip list.
type SkipList[K comparable, V any] interface {
	// Put inserts a new key-value pair into the skip list.
	// If the key already exists, its previous value is returned alongside
	// a flag indicating replacement.
	Put(key K, value V) (V, bool)

	// Set inserts or updates a key-value pair. Deprecated: prefer Put to
	// observe the replaced flag.
	Set(key K, value V)

	// Get returns the value for a key.
	// The boolean is true if the key exists, false otherwise.
	Get(key K) (V, bool)

	// Contains returns true if the key exists in the skip list.
	Contains(key K) bool

	// Delete removes a key from the skip list and reports the previous value.
	Delete(key K) (V, bool)

	// Len returns the number of elements in the skip list.
	Len() int64

	// SeekGE positions an iterator at the first key greater than or equal to
	// the provided key.
	SeekGE(key K) *Iterator[K, V]
}
