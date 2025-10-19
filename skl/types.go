package skl

import (
	"cmp"
	"errors"
)

// CompareResult represents the outcome of a comparison between two values.
// It follows the semantics of cmp.Compare.
type CompareResult = int

const (
	UnsupportedTypeCode CompareResult = -2

	// CmpLess if x is less than y,
	CmpLess CompareResult = -1
	//	CmpEqual if x equals y,
	CmpEqual CompareResult = 0
	// CmpGreater if x is greater than y.
	CmpGreater CompareResult = 1
)

// ErrUnsupportedType is returned when a value does not implement the CmpType
// interface and therefore cannot be compared with Compare.
var ErrUnsupportedType = errors.New("unsupported type: type does not implement CmpType interface")

// CmpType must be implemented by types that provide their own comparison logic
// through the Compare function.
type CmpType interface {
	Compare(other any) int
}

// Comparable is a union constraint that lists all types which can be compared
// using the generic Compare function. It is exported so applications can define
// their own comparable types.
type Comparable interface {
	cmp.Ordered | *CmpType | any
}

// Compare returns the ordering between a and b. It supports builtin ordered
// types and any custom type that implements CmpType. Unsupported types return
// UnsupportedTypeCode.
func Compare[T Comparable](a, b T) CompareResult {
	switch a := any(a).(type) {
	case int:
		return cmp.Compare(a, any(b).(int))
	case int8:
		return cmp.Compare(a, any(b).(int8))
	case int16:
		return cmp.Compare(a, any(b).(int16))
	case int32:
		return cmp.Compare(a, any(b).(int32))
	case int64:
		return cmp.Compare(a, any(b).(int64))
	case uint:
		return cmp.Compare(a, any(b).(uint))
	case uint8:
		return cmp.Compare(a, any(b).(uint8))
	case uint16:
		return cmp.Compare(a, any(b).(uint16))
	case uint32:
		return cmp.Compare(a, any(b).(uint32))
	case uint64:
		return cmp.Compare(a, any(b).(uint64))
	case uintptr:
		return cmp.Compare(a, any(b).(uintptr))
	case float32:
		return cmp.Compare(a, any(b).(float32))
	case float64:
		return cmp.Compare(a, any(b).(float64))
	case string:
		return cmp.Compare(a, any(b).(string))
	case CmpType:
		return a.Compare(b)
	default:
		return UnsupportedTypeCode
	}
}

// ValidateCmpType verifies that the provided value is a supported Comparable
// type. It returns ErrUnsupportedType if the value cannot be compared.
func ValidateCmpType[T Comparable](a T) error {
	if Compare(a, a) == UnsupportedTypeCode {
		return ErrUnsupportedType
	}
	return nil
}

// EOI is end of iteration
//
//lint:ignore ST1012 this is a sentinel error, not a typical error
var EOI = errors.New("EOI")

// Iterator defines the iteration contract. Implementations may support
// moving both forward and backward. Iterators start positioned before the
// first element. Next advances to the next element and returns it. Prev
// returns the current element and moves the iterator one step backward; as a
// consequence, a Prev call immediately after a successful Next returns the
// same element again. It is exported so callers can provide their own iterator
// implementations that work with Rindb primitives.
type Iterator[T any] interface {
	// HasNext reports whether calling Next will succeed.
	HasNext() bool
	// Next advances to the next element and returns it. It returns EOI
	// when the iteration is exhausted.
	Next() (T, error)
	// HasPrev reports whether calling Prev will succeed.
	HasPrev() bool
	// Prev returns the current element and moves the iterator one step
	// backward. It returns EOI when no more elements remain.
	Prev() (T, error)
	// Last positions the iterator at the final element and returns it. The
	// subsequent Prev call should yield the element that precedes the value
	// returned by Last, mirroring how Next leaves the cursor after the
	// current record.
	Last() (T, error)
}

// RangeOrder represents the initial traversal direction for range iterators.
type RangeOrder int

const (
	// RangeAsc streams keys from smallest to largest.
	RangeAsc RangeOrder = iota
	// RangeDesc streams keys from largest to smallest.
	RangeDesc
)

// Config holds configuration for the SkipList.
type Config struct {
	// skipListDefaultLevel is initial height of the skip list
	skipListDefaultLevel uint

	// skipListMaxLevel is maximum height of the skip list
	skipListMaxLevel uint

	// skipListP is probability for skip list level promotion
	skipListP float64
}

// NewConfig creates a Config with default values.
func NewConfig() Config {
	return Config{
		skipListDefaultLevel: 2,
		skipListMaxLevel:     32,
		skipListP:            0.5,
	}
}

// WithSkipListDefaultLevel sets the initial height of the skip list.
func WithSkipListDefaultLevel(level uint) func(*Config) {
	return func(c *Config) { c.skipListDefaultLevel = level }
}

// WithSkipListMaxLevel sets the maximum height of the skip list.
func WithSkipListMaxLevel(maxLevel uint) func(*Config) {
	return func(c *Config) { c.skipListMaxLevel = maxLevel }
}

// WithSkipListP sets the probability for skip list level promotion.
func WithSkipListP(p float64) func(*Config) {
	return func(c *Config) { c.skipListP = p }
}

// Bytes is an alias for []byte, used for key/value types.
type Bytes = []byte

// Errors
var (
	// ErrMalformedList is returned when a SkipList has not been initialized
	// properly. It is exported so callers interacting with SkipList can
	// detect improper initialization.
	ErrMalformedList = errors.New("the list was not init-ed properly")
	// ErrKeyNotFound is returned when a key is not found in the SkipList.
	ErrKeyNotFound = errors.New("key not found")
)
