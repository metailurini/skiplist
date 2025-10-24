package skl

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"
)

type stubRandSource struct {
	values []uint64
	idx    int
}

func (s *stubRandSource) Uint64() uint64 {
	if len(s.values) == 0 {
		return 0
	}
	if s.idx >= len(s.values) {
		return s.values[len(s.values)-1]
	}
	value := s.values[s.idx]
	s.idx++
	return value
}

func (s *stubRandSource) Seed(uint64) {}

func testConfig(t *testing.T) Config {
	t.Helper()
	return NewConfig()
}

type customCmpType struct {
	value int
}

func (c customCmpType) Compare(other any) int {
	o := other.(customCmpType)
	if c.value < o.value {
		return -1
	} else if c.value > o.value {
		return 1
	}
	return 0
}

func TestSkipList_Init(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Init with invalid key type", func(t *testing.T) {
		list, err := InitSkipList[struct{ int }, int](cfg)
		if !errors.Is(err, ErrUnsupportedType) {
			t.Errorf("expected error %v, got %v", ErrUnsupportedType, err)
		}
		if list != nil {
			t.Errorf("expected nil, got %v", list)
		}
	})

	t.Run("Init with custom type", func(t *testing.T) {
		list, err := InitSkipList[customCmpType, customCmpType](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		list.Put(customCmpType{1}, customCmpType{2})
		list.Put(customCmpType{3}, customCmpType{4})
		assertOrderedList(t, list.Head())
		if list.level < uint(2) {
			t.Errorf("expected %v >= %v", list.level, uint(2))
		}

		value, err := list.Get(customCmpType{1})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(customCmpType{2}, value) {
			t.Errorf("expected %v, got %v", customCmpType{2}, value)
		}

		value, err = list.Get(customCmpType{3})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(customCmpType{4}, value) {
			t.Errorf("expected %v, got %v", customCmpType{4}, value)
		}
	})

	t.Run("Init with correct key type", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		if list.level < uint(2) {
			t.Errorf("expected %v to be at least %v", list.level, uint(2))
		}
		debugSkipList(list)

		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
			_v, err := list.Get(fmt.Sprintf("k:%d", v))
			if !reflect.DeepEqual(v, _v) {
				t.Errorf("expected %v, got %v", v, _v)
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}

		assertOrderedList(t, list.Head())

		v, err := list.Get("k:8")
		if !reflect.DeepEqual(8, v) {
			t.Errorf("expected %v, got %v", 8, v)
		}
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		err = list.Remove("k:2")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		v, err = list.Get("k:2")
		if v != 0 {
			t.Errorf("expected empty, got %v", v)
		}
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
		}
	})
}

//nolint:funlen
func TestSkipList_Put(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Assert all added values", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		for _, v := range data {
			k := fmt.Sprintf("k:%d", v)
			_v, err := list.Get(k)
			if !reflect.DeepEqual(v, _v) {
				t.Errorf("expected %v, got %v", v, _v)
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}

		// should be 6 because data has 2 "8"
		if !reflect.DeepEqual(uint(6), list.Len()) {
			t.Errorf("expected %v, got %v", uint(6), list.Len())
		}
		assertOrderedList(t, list.Head())
	})

	t.Run("Override existing key", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		list.Put("k:3", 300)
		v, err := list.Get("k:3")
		if !reflect.DeepEqual(300, v) {
			t.Errorf("expected %v, got %v", 300, v)
		}
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// should be 6 because no new key
		if !reflect.DeepEqual(uint(6), list.Len()) {
			t.Errorf("expected %v, got %v", uint(6), list.Len())
		}
		assertOrderedList(t, list.Head())
	})

	t.Run("Empty key or value", func(t *testing.T) {
		// TODO: fix this case
		t.Skip()

		list, err := InitSkipList[Bytes, Bytes](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		list.Put(nil, nil)
		value, err := list.Get(nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(Bytes(nil), value) {
			t.Errorf("expected %v, got %v", Bytes(nil), value)
		}

		list.Put(nil, Bytes(""))
		value, err = list.Get(nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(Bytes(""), value) {
			t.Errorf("expected %v, got %v", Bytes(""), value)
		}

		list.Put(Bytes(""), nil)
		value, err = list.Get(nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(Bytes(nil), value) {
			t.Errorf("expected %v, got %v", Bytes(nil), value)
		}

		list.Put(Bytes(""), Bytes(""))
		value, err = list.Get(nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(Bytes(""), value) {
			t.Errorf("expected %v, got %v", Bytes(""), value)
		}

		debugSkipList(list)
		if !reflect.DeepEqual(uint(2), list.Len()) {
			t.Errorf("expected %v, got %v", uint(2), list.Len())
		}
		assertOrderedList(t, list.Head())
	})
}

func TestSkipList_Get(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[string, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.Put(fmt.Sprintf("k:%d", v), v)
	}

	v, err := list.Get("k:100")
	if v != 0 {
		t.Errorf("expected empty, got %v", v)
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
	}

	v, err = list.Get("k:8")
	if !reflect.DeepEqual(8, v) {
		t.Errorf("expected %v, got %v", 8, v)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSkipList_Remove(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[string, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	data := []int{6, 3, 5, 8, 1, 2, 9}
	actualLength := uint(len(data))

	for _, v := range data {
		list.Put(fmt.Sprintf("k:%d", v), v)
	}

	tests := []struct {
		name, key string
		existing  bool
	}{
		{
			name:     "remove first value",
			key:      "k:1",
			existing: true,
		},
		{
			name:     "remove mid value",
			key:      "k:3",
			existing: true,
		},
		{
			name:     "remove last value",
			key:      "k:8",
			existing: true,
		},
		{
			name:     "remove last value",
			key:      "k:100",
			existing: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			debugSkipList(list)
			err := list.Remove(tt.key)
			debugSkipList(list)
			if tt.existing {
				actualLength--
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
				}
			}

			v, err := list.Get(tt.key)
			if v != 0 {
				t.Errorf("expected empty, got %v", v)
			}
			keyShouldNotExists(t, tt.key, list)
			if !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
			}
			if !reflect.DeepEqual(actualLength, list.Len()) {
				t.Errorf("expected %v, got %v", actualLength, list.Len())
			}
			assertOrderedList(t, list.Head())
		})
	}
}

func TestSkipList_RemoveTailUpdates(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("single element clears tail", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		list.Put(1, 1)
		if list.tail == nil {
			t.Errorf("expected not nil")
		}

		err = list.Remove(1)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if list.tail != nil {
			t.Errorf("expected nil, got %v", list.tail)
		}
	})

	t.Run("multiple elements retarget tail", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		for i := 1; i <= 3; i++ {
			list.Put(i, i)
		}

		if list.tail == nil {
			t.Errorf("expected not nil")
		}
		if !reflect.DeepEqual(3, list.tail.Key) {
			t.Errorf("expected %v, got %v", 3, list.tail.Key)
		}

		err = list.Remove(3)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if list.tail == nil {
			t.Errorf("expected not nil")
		}
		if !reflect.DeepEqual(2, list.tail.Key) {
			t.Errorf("expected %v, got %v", 2, list.tail.Key)
		}
	})
}

func TestSkipList_IteratorReverse(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("basic", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		for i := 1; i <= 3; i++ {
			list.Put(i, i)
		}

		it, ok := list.Iterator().(*slIterator[int, int])
		if !ok {
			t.Errorf("expected true")
		}

		for it.HasNext() {
			_, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}

		var rev []int
		for it.HasPrev() {
			v, err := it.Prev()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			rev = append(rev, v)
		}

		if !reflect.DeepEqual([]int{3, 2, 1}, rev) {
			t.Errorf("expected %v, got %v", []int{3, 2, 1}, rev)
		}
	})

	t.Run("after remove", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		for i := 1; i <= 3; i++ {
			list.Put(i, i)
		}
		err = list.Remove(2)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		it, ok := list.Iterator().(*slIterator[int, int])
		if !ok {
			t.Errorf("expected true")
		}
		for it.HasNext() {
			_, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
		var rev []int
		for it.HasPrev() {
			v, err := it.Prev()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			rev = append(rev, v)
		}
		if !reflect.DeepEqual([]int{3, 1}, rev) {
			t.Errorf("expected %v, got %v", []int{3, 1}, rev)
		}
	})
}

func TestSkipList_IteratorMixed(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 3; i++ {
		list.Put(i, i)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	// Move forward one step then backward again. Prev returns the same
	// element that Next produced.
	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err := it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(1, v) {
		t.Errorf("expected %v, got %v", 1, v)
	}

	if !it.HasPrev() {
		t.Errorf("expected true")
	}

	v, err = it.Prev()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(1, v) {
		t.Errorf("expected %v, got %v", 1, v)
	}

	// Next after Prev yields the same element again.
	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err = it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(1, v) {
		t.Errorf("expected %v, got %v", 1, v)
	}

	// Advance once more to move past the first element.
	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err = it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(2, v) {
		t.Errorf("expected %v, got %v", 2, v)
	}
}

func TestSkipList_IteratorLast(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 4; i++ {
		list.Put(i, i)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	last, err := it.Last()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(4, last) {
		t.Errorf("expected %v, got %v", 4, last)
	}

	prev, err := it.Prev()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(3, prev) {
		t.Errorf("expected %v, got %v", 3, prev)
	}
}

func TestSLIterator_LastEmpty(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("nil iterator", func(t *testing.T) {
		it := &slIterator[int, int]{}
		last, err := it.Last()
		if last != 0 {
			t.Errorf("expected zero, got %v", last)
		}
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("empty list", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		it, ok := list.Iterator().(*slIterator[int, int])
		if !ok {
			t.Errorf("expected true")
		}

		last, err := it.Last()
		if last != 0 {
			t.Errorf("expected zero, got %v", last)
		}
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})
}

func TestSkipList_IteratorConcurrentMutations(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, v := range []int{1, 3, 5, 7} {
		list.Put(v, v)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	v, err := it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(1, v) {
		t.Errorf("expected %v, got %v", 1, v)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		list.Put(2, 2)
		_ = list.Remove(5)
	}()
	wg.Wait()

	forward := []int{1}
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		forward = append(forward, v)
	}
	if !reflect.DeepEqual([]int{1, 2, 3, 7}, forward) {
		t.Errorf("expected %v, got %v", []int{1, 2, 3, 7}, forward)
	}

	var backward []int
	for it.HasPrev() {
		v, err := it.Prev()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		backward = append(backward, v)
	}
	if !reflect.DeepEqual([]int{7, 3, 2, 1}, backward) {
		t.Errorf("expected %v, got %v", []int{7, 3, 2, 1}, backward)
	}
	assertOrderedList(t, list.Head())
}

func TestSkipList_IRangeReverse(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4, RangeAsc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	for it.HasNext() {
		_, err := it.Next()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	var rev []int
	for it.HasPrev() {
		v, err := it.Prev()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		rev = append(rev, v)
	}

	if !reflect.DeepEqual([]int{4, 3, 2}, rev) {
		t.Errorf("expected %v, got %v", []int{4, 3, 2}, rev)
	}
}

func TestSkipList_IRangeMixed(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4, RangeAsc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err := it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(2, v) {
		t.Errorf("expected %v, got %v", 2, v)
	}

	if !it.HasPrev() {
		t.Errorf("expected true")
	}
	v, err = it.Prev()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(2, v) {
		t.Errorf("expected %v, got %v", 2, v)
	}

	// Resume forward iteration
	// Next after Prev returns the same element again.
	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err = it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(2, v) {
		t.Errorf("expected %v, got %v", 2, v)
	}

	// Further Next calls resume forward iteration.
	if !it.HasNext() {
		t.Errorf("expected true")
	}
	v, err = it.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(3, v) {
		t.Errorf("expected %v, got %v", 3, v)
	}
}

func TestSkipList_IRangeDescendingPrev(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it := list.IRange(1, 5, RangeDesc)

	var values []int
	for it.HasPrev() {
		v, err := it.Prev()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		values = append(values, v)
	}

	if !reflect.DeepEqual([]int{5, 4, 3, 2, 1}, values) {
		t.Errorf("expected %v, got %v", []int{5, 4, 3, 2, 1}, values)
	}
}

func TestSLIRange_HasPrevClampsDescending(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4, RangeDesc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	lower := list.Head().Next()
	if lower == nil {
		t.Errorf("expected not nil")
	}
	it.desc = lower
	if it.HasPrev() {
		t.Errorf("expected false")
	}
	if it.desc != nil {
		t.Errorf("expected nil, got %v", it.desc)
	}
}

func TestSLIRange_PrevDescendingNoValues(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(4, 3, RangeDesc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	value, err := it.Prev()
	if value != 0 {
		t.Errorf("expected zero, got %v", value)
	}
	if !errors.Is(err, EOI) {
		t.Errorf("expected error %v, got %v", EOI, err)
	}
}

func TestSLIRange_LastEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil list", func(t *testing.T) {
		it := &slIRange[int, int]{}
		value, err := it.Last()
		if value != 0 {
			t.Errorf("expected zero, got %v", value)
		}
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	testCases := []struct {
		name     string
		keys     []int
		startKey int
		endKey   int
	}{
		{
			name:     "no node before end",
			keys:     []int{5, 6},
			startKey: 1,
			endKey:   0,
		},
		{
			name:     "node before start",
			keys:     []int{1, 2, 3, 4, 5},
			startKey: 4,
			endKey:   3,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := testConfig(t)

			list, err := InitSkipList[int, int](cfg)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			for _, v := range tc.keys {
				list.Put(v, v)
			}

			it := &slIRange[int, int]{
				list:     list,
				startKey: tc.startKey,
				endKey:   tc.endKey,
			}

			value, err := it.Last()
			if value != 0 {
				t.Errorf("expected zero, got %v", value)
			}
			if !errors.Is(err, EOI) {
				t.Errorf("expected error %v, got %v", EOI, err)
			}
		})
	}
}

func TestSkipList_IRangeDescendingLast(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 4; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4, RangeDesc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	last, err := it.Last()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(4, last) {
		t.Errorf("expected %v, got %v", 4, last)
	}

	var backwards []int
	for it.HasPrev() {
		v, err := it.Prev()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		backwards = append(backwards, v)
	}

	if !reflect.DeepEqual([]int{3, 2}, backwards) {
		t.Errorf("expected %v, got %v", []int{3, 2}, backwards)
	}
}

func TestSkipList_IRangeLast(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4, RangeAsc).(*slIRange[int, int])
	if !ok {
		t.Errorf("expected true")
	}

	last, err := it.Last()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(4, last) {
		t.Errorf("expected %v, got %v", 4, last)
	}

	prev, err := it.Prev()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(3, prev) {
		t.Errorf("expected %v, got %v", 3, prev)
	}
}

func TestSkipList_Clear(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Clear list properly", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		list.Put("1", 1)
		list.Put("2", 2)
		list.Put("3", 3)

		if !reflect.DeepEqual(uint(3), list.Len()) {
			t.Errorf("expected %v, got %v", uint(3), list.Len())
		}
		list.Clear()
		if !reflect.DeepEqual(uint(0), list.Len()) {
			t.Errorf("expected %v, got %v", uint(0), list.Len())
		}
	})

	t.Run("Expect error when clearing list was not init properly",
		func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("The code did not panic")
				}
				if !reflect.DeepEqual(ErrMalformedList, r) {
					t.Errorf("expected %v, got %v", ErrMalformedList, r)
				}
			}()

			var list *SkipList[struct{}, struct{}]
			list.Clear()
		})
}

func assertOrderedList[K, V Comparable](t *testing.T, head *SLNode[K, V]) {
	for head.Next() != nil {
		n := head.Next()
		if !reflect.DeepEqual(-1, Compare(head.Key, n.Key)) {
			t.Errorf("expected %v, got %v", -1, Compare(head.Key, n.Key))
		}
		head = head.Next()
	}
}

func keyShouldNotExists[K, V Comparable](t *testing.T, key K, list *SkipList[K, V]) {
	r := list.headNote.Next()
	for r != nil {
		for _, v := range r.forwards {
			if v == nil {
				continue
			}
			if Compare(key, v.Key) == CmpEqual {
				t.Errorf("Key %v should not exist in the skiplist", key)
			}
		}
		fmt.Println()
		r = r.Next()
	}
}

func debugSkipList[K Comparable, V any](list *SkipList[K, V]) {
	// fmt.Printf("--header--: %v\n", list.headNote)
	// r := list.headNote.Next()
	// for r != nil {
	// 	fmt.Printf("[%v<>%v] ", r.Key, r.Value)
	// 	for _, v := range r.forwards {
	// 		if v == nil {
	// 			continue
	// 		}
	// 		fmt.Printf("[%v<>%v] ", v.Key, v.Value)
	// 	}
	// 	fmt.Println()
	// 	r = r.Next()
	// }
}

func TestSkipList_IRange(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("Empty list", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		it := list.IRange(1, 5, RangeAsc)
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("Range covers all elements", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{1, 2, 3, 4, 5}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(0, 6, RangeAsc)
		expected := []int{10, 20, 30, 40, 50}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("Range completely outside (before)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(1, 5, RangeAsc)
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("Range completely outside (after)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(35, 40, RangeAsc)
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("Range partially overlaps (start before, end within)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(5, 30, RangeAsc)
		expected := []int{100, 200, 300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("Range partially overlaps (start within, end after)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 55, RangeAsc)
		expected := []int{300, 400, 500}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("Range completely within", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(20, 40, RangeAsc)
		expected := []int{200, 300, 400}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("Start and end keys are the same (single element)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 30, RangeAsc)
		expected := []int{300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("Start and end keys are the same (element not present)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(25, 25, RangeAsc)
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("Start key greater than end key", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 20, RangeAsc)
		if it.HasNext() {
			t.Errorf("expected false")
		}
		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
	})

	t.Run("List with duplicate keys (should only return one value per key)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		list.Put(10, 100)
		list.Put(20, 200)
		list.Put(10, 101) // Overwrites 100
		list.Put(30, 300)

		it := list.IRange(5, 35, RangeAsc)
		expected := []int{101, 200, 300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			actual = append(actual, val)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("Iterator behavior after reaching end", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		list.Put(1, 10)
		list.Put(2, 20)

		it := list.IRange(1, 2, RangeAsc)
		val1, err := it.Next()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(10, val1) {
			t.Errorf("expected %v, got %v", 10, val1)
		}
		if !it.HasNext() {
			t.Errorf("expected true")
		}

		val2, err := it.Next()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(20, val2) {
			t.Errorf("expected %v, got %v", 20, val2)
		}
		if it.HasNext() {
			t.Errorf("expected false")
		}

		_, err = it.Next()
		if !errors.Is(err, EOI) {
			t.Errorf("expected error %v, got %v", EOI, err)
		}
		if it.HasNext() {
			t.Errorf("expected false")
		} // Should still be false
	})
}

func TestSkipList_FindGreaterOrEqual(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("Empty list", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		node, err := list.FindGreaterOrEqual(5)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected error %v, got %v", ErrKeyNotFound, err)
		}
		if node != nil {
			t.Errorf("expected nil, got %v", node)
		}
	})

	t.Run("Existing elements", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		for _, k := range []int{10, 20, 30, 40, 50} {
			list.Put(k, k*10)
		}

		tests := []struct {
			name    string
			search  int
			wantKey int
			wantVal int
			wantErr error
		}{
			{
				name:    "exact match on first element",
				search:  10,
				wantKey: 10,
				wantVal: 100,
			},
			{
				name:    "exact match",
				search:  30,
				wantKey: 30,
				wantVal: 300,
			},
			{
				name:    "exact match on last element",
				search:  50,
				wantKey: 50,
				wantVal: 500,
			},
			{
				name:    "between keys",
				search:  25,
				wantKey: 30,
				wantVal: 300,
			},
			{
				name:    "before first",
				search:  5,
				wantKey: 10,
				wantVal: 100,
			},
			{
				name:    "after last",
				search:  60,
				wantErr: ErrKeyNotFound,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node, err := list.FindGreaterOrEqual(tt.search)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Errorf("expected error %v, got %v", tt.wantErr, err)
					}
					if node != nil {
						t.Errorf("expected nil, got %v", node)
					}
					return
				}

				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if node == nil {
					t.Errorf("expected not nil")
					return
				}
				if !reflect.DeepEqual(tt.wantKey, node.Key) {
					t.Errorf("expected %v, got %v", tt.wantKey, node.Key)
				}
				if !reflect.DeepEqual(tt.wantVal, node.Value) {
					t.Errorf("expected %v, got %v", tt.wantVal, node.Value)
				}
			})
		}
	})
}

func TestSkipList_randomLevelDistribution(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	list.rng = rand.NewPCG(1, 2)

	const sampleSize = 1 << 15
	counts := make([]int, int(cfg.skipListMaxLevel))

	for i := 0; i < sampleSize; i++ {
		lvl := list.randomLevel()
		counts[int(lvl-1)]++
	}

	for level := uint(1); level <= cfg.skipListMaxLevel; level++ {
		expected := math.Pow(cfg.skipListP, float64(level-1))
		if level < cfg.skipListMaxLevel {
			expected *= 1 - cfg.skipListP
		}
		actual := float64(counts[level-1]) / sampleSize
		if math.Abs(expected-actual) > 0.02 {
			t.Errorf("level %d", level)
		}
	}
}

func TestSkipList_randomLevelTrailingZeros(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.skipListDefaultLevel = 1
	cfg.skipListMaxLevel = 4
	cfg.skipListP = 0.5

	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	list.rng = &stubRandSource{values: []uint64{1, 1 << 1, 1 << 4, 0}}

	levels := []uint{
		list.randomLevel(),
		list.randomLevel(),
		list.randomLevel(),
		list.randomLevel(),
	}

	if !reflect.DeepEqual([]uint{1, 2, 4, 4}, levels) {
		t.Errorf("expected %v, got %v", []uint{1, 2, 4, 4}, levels)
	}
}

func TestSkipList_randomLevelWithCustomProbability(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.skipListDefaultLevel = 1
	cfg.skipListMaxLevel = 5
	cfg.skipListP = 0.25

	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	list.rng = &stubRandSource{values: []uint64{0, 0, 0, 1 << 63}}

	level := list.randomLevel()

	if !reflect.DeepEqual(uint(4), level) {
		t.Errorf("expected %v, got %v", uint(4), level)
	}
}

func TestSkipList_randomLevelMaxLevelOne(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.skipListDefaultLevel = 1
	cfg.skipListMaxLevel = 1

	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	list.rng = &stubRandSource{values: []uint64{1 << 10}}

	level := list.randomLevel()

	if !reflect.DeepEqual(uint(1), level) {
		t.Errorf("expected %v, got %v", uint(1), level)
	}
}
