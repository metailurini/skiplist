package skiplist

import "fmt"

func ExampleSkipListMap_Put() {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Put(1, "one")
	m.Put(2, "two")
	fmt.Println(m.LenInt64())
	// Output: 2
}

func ExampleSkipListMap_Get() {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Put(1, "one")
	m.Put(2, "two")
	val, ok := m.Get(1)
	fmt.Printf("%s %t\n", val, ok)
	// Output: one true
}

func ExampleSkipListMap_Delete() {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Put(1, "one")
	m.Put(2, "two")
	val, ok := m.Delete(1)
	fmt.Printf("%s %t\n", val, ok)
	fmt.Println(m.LenInt64())
	// Output: one true
	// 1
}

func ExampleSkipListMap_Iterator() {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Put(3, "three")
	m.Put(1, "one")
	m.Put(2, "two")
	it := m.Iterator()
	for it.Next() {
		fmt.Printf("%d:%s ", it.Key(), it.Value())
	}
	fmt.Println()
	// Output: 1:one 2:two 3:three
}

func ExampleSkipListMap_SeekGE() {
	m := New[int, string](func(a, b int) bool { return a < b })
	m.Put(1, "one")
	m.Put(3, "three")
	m.Put(5, "five")
	it := m.SeekGE(2)
	for it.Valid() {
		fmt.Printf("%d:%s ", it.Key(), it.Value())
		it.Next()
	}
	fmt.Println()
	// Output: 3:three 5:five
}
