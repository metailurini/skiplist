package skiplist

// Less is a function that returns true if a is less than b.
type Less[K comparable] func(a, b K) bool

// SkipList is the interface for a skip list.
type SkipList[K comparable, V any] interface {
	// Set inserts a new key-value pair into the skip list.
	// If the key already exists, the value is updated.
	Set(key K, value V)

	// Get returns the value for a key.
	// The boolean is true if the key exists, false otherwise.
	Get(key K) (V, bool)

	// Contains returns true if the key exists in the skip list.
	Contains(key K) bool

	// Delete removes a key from the skip list.
	Delete(key K)

	// Len returns the number of elements in the skip list.
	Len() int
}
