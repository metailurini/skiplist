package skiplist

import (
	"fmt"
	"sync"
	"testing"
)

// TestSetGetContains tests the basic Set, Get, and Contains functionality.
func TestSetGetContains(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })

	// Test Set and Get for new keys
	m.Set(1, "one")
	val, ok := m.Get(1)
	if !ok || val != "one" {
		t.Errorf("Get(1) failed: got %v, %v, want %v, %v", val, ok, "one", true)
	}
	if !m.Contains(1) {
		t.Errorf("Contains(1) failed: got %v, want %v", false, true)
	}

	m.Set(2, "two")
	val, ok = m.Get(2)
	if !ok || val != "two" {
		t.Errorf("Get(2) failed: got %v, %v, want %v, %v", val, ok, "two", true)
	}
	if !m.Contains(2) {
		t.Errorf("Contains(2) failed: got %v, want %v", false, true)
	}

	// Test updating an existing key
	m.Set(1, "one_updated")
	val, ok = m.Get(1)
	if !ok || val != "one_updated" {
		t.Errorf("Get(1) after update failed: got %v, %v, want %v, %v", val, ok, "one_updated", true)
	}

	// Test Get for a non-existent key
	_, ok = m.Get(3)
	if ok {
		t.Errorf("Get(3) for non-existent key failed: got %v, want %v", true, false)
	}
	if m.Contains(3) {
		t.Errorf("Contains(3) for non-existent key failed: got %v, want %v", true, false)
	}

	// Test with more elements and different order
	m.Set(5, "five")
	m.Set(3, "three")
	m.Set(4, "four")

	expectedOrder := []struct {
		key int
		val string
	}{
		{1, "one_updated"},
		{2, "two"},
		{3, "three"},
		{4, "four"},
		{5, "five"},
	}

	// Verify order and values
	for _, item := range expectedOrder {
		val, ok := m.Get(item.key)
		if !ok || val != item.val {
			t.Errorf("Get(%d) failed: got %v, %v, want %v, %v", item.key, val, ok, item.val, true)
		}
	}
}

// TestSetConcurrent tests concurrent Set operations.
func TestSetConcurrent(t *testing.T) {
	m := New[int, int](func(a, b int) bool { return a < b })
	numGoroutines := 100
	numInsertsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numInsertsPerGoroutine; j++ {
				key := goroutineID*numInsertsPerGoroutine + j
				m.Set(key, key)
			}
		}(i)
	}
	wg.Wait()

	expectedLen := numGoroutines * numInsertsPerGoroutine
	if m.Len() != expectedLen {
		t.Errorf("Concurrent Set: expected length %d, got %d", expectedLen, m.Len())
	}

	for i := 0; i < expectedLen; i++ {
		val, ok := m.Get(i)
		if !ok || val != i {
			t.Errorf("Concurrent Set: Get(%d) failed: got %v, %v, want %v, %v", i, val, ok, i, true)
		}
		if !m.Contains(i) {
			t.Errorf("Concurrent Set: Contains(%d) failed: got %v, want %v", i, false, true)
		}
	}
}

// TestSetUpdateConcurrent tests concurrent updates to existing keys.
func TestSetUpdateConcurrent(t *testing.T) {
	m := New[int, int](func(a, b int) bool { return a < b })
	numKeys := 100
	numUpdatesPerGoroutine := 100
	numGoroutines := 10

	// Initialize map with keys
	for i := 0; i < numKeys; i++ {
		m.Set(i, 0)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numUpdatesPerGoroutine; j++ {
				key := j % numKeys // Update existing keys
				m.Set(key, goroutineID)
			}
		}(i)
	}
	wg.Wait()

	// Verify that all keys exist and have been updated by one of the goroutines.
	// The final value will be the value set by the last goroutine to update that key.
	for i := 0; i < numKeys; i++ {
		_, ok := m.Get(i)
		if !ok {
			t.Errorf("Concurrent Update: Key %d not found after updates", i)
		}
	}
}

// TestLen tests the Len method.
func TestLen(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })

	if m.Len() != 0 {
		t.Errorf("Expected initial length 0, got %d", m.Len())
	}

	m.Set(1, "one")
	if m.Len() != 1 {
		t.Errorf("Expected length 1, got %d", m.Len())
	}

	m.Set(2, "two")
	if m.Len() != 2 {
		t.Errorf("Expected length 2, got %d", m.Len())
	}

	m.Set(1, "one_updated") // Update existing key, length should not change
	if m.Len() != 2 {
		t.Errorf("Expected length 2 after update, got %d", m.Len())
	}

	m.Set(3, "three")
	if m.Len() != 3 {
		t.Errorf("Expected length 3, got %d", m.Len())
	}
}

// TestGetNonExistentKey tests Get for a key that does not exist.
func TestGetNonExistentKey(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })
	_, ok := m.Get(1)
	if ok {
		t.Errorf("Get for non-existent key returned true, want false")
	}
}

// TestContainsNonExistentKey tests Contains for a key that does not exist.
func TestContainsNonExistentKey(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })
	if m.Contains(1) {
		t.Errorf("Contains for non-existent key returned true, want false")
	}
}

// TestSetZeroValue tests setting a zero value.
func TestSetZeroValue(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Set(1, "")
	val, ok := m.Get(1)
	if !ok || val != "" {
		t.Errorf("Set/Get zero value failed: got %v, %v, want %v, %v", val, ok, "", true)
	}
}

// TestSetNilValue tests setting a nil value (for pointer types, though string is not a pointer).
// This test is more conceptual for generic types where V could be a pointer.
func TestSetNilValue(t *testing.T) {
	m := New[int, *string](func(a, b int) bool { return a < b })
	var nilStr *string // nil value for *string
	m.Set(1, nilStr)
	val, ok := m.Get(1)
	if !ok || val != nil {
		t.Errorf("Set/Get nil value failed: got %v, %v, want %v, %v", val, ok, nil, true)
	}

	// Ensure that a non-nil value can overwrite a nil value
	strVal := "hello"
	m.Set(1, &strVal)
	val, ok = m.Get(1)
	if !ok || *val != "hello" {
		t.Errorf("Set/Get non-nil value after nil failed: got %v, %v, want %v, %v", val, ok, &strVal, true)
	}
}

// TestSetDifferentTypes tests with different key and value types.
func TestSetDifferentTypes(t *testing.T) {
	m := New[string, float64](func(a, b string) bool { return a < b })
	m.Set("pi", 3.14)
	m.Set("e", 2.71)

	val, ok := m.Get("pi")
	if !ok || val != 3.14 {
		t.Errorf("Get(\"pi\") failed: got %v, %v, want %v, %v", val, ok, 3.14, true)
	}

	val, ok = m.Get("e")
	if !ok || val != 2.71 {
		t.Errorf("Get(\"e\") failed: got %v, %v, want %v, %v", val, ok, 2.71, true)
	}

	if !m.Contains("pi") {
		t.Errorf("Contains(\"pi\") failed")
	}
	if m.Contains("golden_ratio") {
		t.Errorf("Contains(\"golden_ratio\") unexpectedly returned true")
	}
}

// TestSetManyElements tests inserting a large number of elements.
func TestSetManyElements(t *testing.T) {
	m := New[int, int](func(a, b int) bool { return a < b })
	numElements := 10000

	for i := 0; i < numElements; i++ {
		m.Set(i, i*2)
	}

	if m.Len() != numElements {
		t.Errorf("Expected length %d, got %d", numElements, m.Len())
	}

	for i := 0; i < numElements; i++ {
		val, ok := m.Get(i)
		if !ok || val != i*2 {
			t.Errorf("Get(%d) failed: got %v, %v, want %v, %v", i, val, ok, i*2, true)
		}
		if !m.Contains(i) {
			t.Errorf("Contains(%d) failed", i)
		}
	}
}

// TestSetWithCollisionLikeKeys tests keys that might cause collisions if not handled properly (e.g., different cases for strings).
func TestSetWithCollisionLikeKeys(t *testing.T) {
	m := New[string, string](func(a, b string) bool { return a < b })

	m.Set("apple", "fruit")
	m.Set("Apple", "company") // Different key due to case sensitivity

	val, ok := m.Get("apple")
	if !ok || val != "fruit" {
		t.Errorf("Get(\"apple\") failed: got %v, %v, want %v, %v", val, ok, "fruit", true)
	}

	val, ok = m.Get("Apple")
	if !ok || val != "company" {
		t.Errorf("Get(\"Apple\") failed: got %v, %v, want %v, %v", val, ok, "company", true)
	}

	if m.Len() != 2 {
		t.Errorf("Expected length 2, got %d", m.Len())
	}
}

// TestSetGetContainsWithCustomLess tests with a custom less function.
func TestSetGetContainsWithCustomLess(t *testing.T) {
	// Custom less function for reverse order
	m := New[int, string](func(a, b int) bool { return a > b })

	m.Set(1, "one")
	m.Set(2, "two")
	m.Set(3, "three")

	// In reverse order, 3 should be "before" 2, and 2 before 1.
	// The internal structure will be reversed, but Get/Contains should still work.
	val, ok := m.Get(1)
	if !ok || val != "one" {
		t.Errorf("Get(1) failed with custom less: got %v, %v, want %v, %v", val, ok, "one", true)
	}
	if !m.Contains(2) {
		t.Errorf("Contains(2) failed with custom less")
	}
	if m.Contains(4) {
		t.Errorf("Contains(4) unexpectedly returned true with custom less")
	}
}

// TestSetGetContainsAfterConcurrentDeletes (conceptual, as Delete is not implemented yet)
// This test would ensure that Set/Get/Contains behave correctly after items have been logically deleted.
// For now, we can simulate by setting values to nil if V is a pointer type, or a zero value.
func TestSetGetContainsAfterLogicalDeletion(t *testing.T) {
	m := New[int, *string](func(a, b int) bool { return a < b })
	str1 := "value1"
	str2 := "value2"
	m.Set(1, &str1)
	m.Set(2, &str2)

	// "Logically delete" key 1 by setting its value to nil
	m.Set(1, nil)

	// Get for key 1 should return (nil, true) if the node is still there but value is nil
	// or (zero_value, false) if the node is truly gone.
	// Based on current `find` and `Get` logic, it should return (zero_value, false)
	// because `find` checks `candidate.val.Load() != nil` for `found`.
	val, ok := m.Get(1)
	if ok {
		t.Errorf("Get(1) after logical delete returned true, want false. Got value: %v", val)
	}
	if m.Contains(1) {
		t.Errorf("Contains(1) after logical delete returned true, want false")
	}

	// Key 2 should still be accessible
	val2, ok2 := m.Get(2)
	if !ok2 || *val2 != "value2" {
		t.Errorf("Get(2) after logical delete of 1 failed: got %v, %v, want %v, %v", val2, ok2, &str2, true)
	}
	if !m.Contains(2) {
		t.Errorf("Contains(2) after logical delete of 1 failed")
	}

	// Re-insert key 1
	str1_new := "value1_new"
	m.Set(1, &str1_new)
	val1_new, ok1_new := m.Get(1)
	if !ok1_new || *val1_new != "value1_new" {
		t.Errorf("Get(1) after re-insertion failed: got %v, %v, want %v, %v", val1_new, ok1_new, &str1_new, true)
	}
	if !m.Contains(1) {
		t.Errorf("Contains(1) after re-insertion failed")
	}
}

// TestSetWithManyUpdates tests a single key being updated many times.
func TestSetWithManyUpdates(t *testing.T) {
	m := New[int, int](func(a, b int) bool { return a < b })
	key := 10
	numUpdates := 1000

	for i := 0; i < numUpdates; i++ {
		m.Set(key, i)
	}

	val, ok := m.Get(key)
	if !ok || val != numUpdates-1 {
		t.Errorf("Get(%d) after many updates failed: got %v, %v, want %v, %v", key, val, ok, numUpdates-1, true)
	}
	if m.Len() != 1 {
		t.Errorf("Expected length 1 after many updates to single key, got %d", m.Len())
	}
}

// TestSetGetContainsEmptyMap tests operations on an empty map.
func TestSetGetContainsEmptyMap(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })

	if m.Len() != 0 {
		t.Errorf("Empty map length expected 0, got %d", m.Len())
	}
	_, ok := m.Get(1)
	if ok {
		t.Errorf("Get on empty map returned true, want false")
	}
	if m.Contains(1) {
		t.Errorf("Contains on empty map returned true, want false")
	}
}

// TestSetGetContainsAfterMixedOperations tests a mix of Set, Get, Contains.
func TestSetGetContainsAfterMixedOperations(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })

	m.Set(10, "ten")
	m.Set(20, "twenty")
	m.Set(5, "five")

	if !m.Contains(10) {
		t.Errorf("Contains(10) failed")
	}
	val, ok := m.Get(5)
	if !ok || val != "five" {
		t.Errorf("Get(5) failed: got %v, %v, want %v, %v", val, ok, "five", true)
	}

	m.Set(10, "ten_updated")
	val, ok = m.Get(10)
	if !ok || val != "ten_updated" {
		t.Errorf("Get(10) after update failed: got %v, %v, want %v, %v", val, ok, "ten_updated", true)
	}

	if m.Contains(15) {
		t.Errorf("Contains(15) unexpectedly returned true")
	}
	_, ok = m.Get(30)
	if ok {
		t.Errorf("Get(30) unexpectedly returned true")
	}

	if m.Len() != 3 {
		t.Errorf("Expected length 3, got %d", m.Len())
	}
}

// TestSetWithDifferentKeyTypes tests the generic key type.
func TestSetWithDifferentKeyTypes(t *testing.T) {
	// Test with string keys
	mStr := New[string, int](func(a, b string) bool { return a < b })
	mStr.Set("apple", 1)
	mStr.Set("banana", 2)
	valStr, okStr := mStr.Get("apple")
	if !okStr || valStr != 1 {
		t.Errorf("Get(\"apple\") failed for string key: got %v, %v, want %v, %v", valStr, okStr, 1, true)
	}

	// Test with float64 keys
	mFloat := New[float64, string](func(a, b float64) bool { return a < b })
	mFloat.Set(3.14, "pi")
	mFloat.Set(2.71, "e")
	valFloat, okFloat := mFloat.Get(3.14)
	if !okFloat || valFloat != "pi" {
		t.Errorf("Get(3.14) failed for float64 key: got %v, %v, want %v, %v", valFloat, okFloat, "pi", true)
	}
}

// TestSetWithStructKeys tests using a struct as a key (requires comparable).
type MyStructKey struct {
	ID   int
	Name string
}

func TestSetWithStructKeys(t *testing.T) {
	m := New[MyStructKey, string](func(a, b MyStructKey) bool {
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Name < b.Name
	})

	key1 := MyStructKey{ID: 1, Name: "Alice"}
	key2 := MyStructKey{ID: 2, Name: "Bob"}
	key3 := MyStructKey{ID: 1, Name: "Charlie"} // Different name, same ID

	m.Set(key1, "Value1")
	m.Set(key2, "Value2")
	m.Set(key3, "Value3")

	val, ok := m.Get(key1)
	if !ok || val != "Value1" {
		t.Errorf("Get(%v) failed: got %v, %v, want %v, %v", key1, val, ok, "Value1", true)
	}

	val, ok = m.Get(key2)
	if !ok || val != "Value2" {
		t.Errorf("Get(%v) failed: got %v, %v, want %v, %v", key2, val, ok, "Value2", true)
	}

	val, ok = m.Get(key3)
	if !ok || val != "Value3" {
		t.Errorf("Get(%v) failed: got %v, %v, want %v, %v", key3, val, ok, "Value3", true)
	}

	if m.Len() != 3 {
		t.Errorf("Expected length 3, got %d", m.Len())
	}

	// Test updating a struct key
	m.Set(key1, "Value1_Updated")
	val, ok = m.Get(key1)
	if !ok || val != "Value1_Updated" {
		t.Errorf("Get(%v) after update failed: got %v, %v, want %v, %v", key1, val, ok, "Value1_Updated", true)
	}
}

// TestSetGetContainsWithNegativeKeys tests with negative integer keys.
func TestSetGetContainsWithNegativeKeys(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })

	m.Set(-1, "negative one")
	m.Set(-5, "negative five")
	m.Set(0, "zero")

	val, ok := m.Get(-1)
	if !ok || val != "negative one" {
		t.Errorf("Get(-1) failed: got %v, %v, want %v, %v", val, ok, "negative one", true)
	}
	val, ok = m.Get(-5)
	if !ok || val != "negative five" {
		t.Errorf("Get(-5) failed: got %v, %v, want %v, %v", val, ok, "negative five", true)
	}
	val, ok = m.Get(0)
	if !ok || val != "zero" {
		t.Errorf("Get(0) failed: got %v, %v, want %v, %v", val, ok, "zero", true)
	}

	if !m.Contains(-1) {
		t.Errorf("Contains(-1) failed")
	}
	if m.Contains(-10) {
		t.Errorf("Contains(-10) unexpectedly returned true")
	}
}

// TestSetGetContainsWithMaxLevelNodes ensures that nodes at MaxLevel are handled correctly.
func TestSetGetContainsWithMaxLevelNodes(t *testing.T) {
	// This test is harder to control directly as randomLevel determines the level.
	// We can try to insert many nodes and ensure consistency.
	m := New[int, int](func(a, b int) bool { return a < b })
	numElements := 1000

	for i := 0; i < numElements; i++ {
		m.Set(i, i)
	}

	for i := 0; i < numElements; i++ {
		val, ok := m.Get(i)
		if !ok || val != i {
			t.Errorf("Get(%d) failed: got %v, %v, want %v, %v", i, val, ok, i, true)
		}
	}
	if m.Len() != numElements {
		t.Errorf("Expected length %d, got %d", numElements, m.Len())
	}
}

// TestSetGetContainsWithLargeValues tests with large string values.
func TestSetGetContainsWithLargeValues(t *testing.T) {
	m := New[int, string](func(a, b int) bool { return a < b })
	largeValue := fmt.Sprintf("This is a very large string value that should be stored without issues. It contains many characters and is designed to test the handling of larger data payloads within the skip list. %s", generateRandomString(1024))

	m.Set(1, largeValue)
	val, ok := m.Get(1)
	if !ok || val != largeValue {
		t.Errorf("Get(1) with large value failed: got length %d, want length %d", len(val), len(largeValue))
		if val != largeValue {
			t.Errorf("Value mismatch for large value")
		}
	}
	if !m.Contains(1) {
		t.Errorf("Contains(1) with large value failed")
	}
}

// Helper function to generate a random string for testing large values.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[i%len(charset)] // Simple way to get repeatable "random" string
	}
	return string(b)
}