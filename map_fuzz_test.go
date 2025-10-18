package skiplist

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fuzzOp struct {
	typ byte
	key int
	val int
}

type fuzzRecord struct {
	index int
	op    fuzzOp
	start time.Time
	end   time.Time

	put *putResult
	get *getResult
	del *deleteResult
}

type putResult struct {
	old      int
	replaced bool
}

type getResult struct {
	value int
	ok    bool
}

type deleteResult struct {
	value int
	ok    bool
}

func FuzzSkipListMapLinearizability(f *testing.F) {
	f.Add([]byte{0, 1, 1, 0, 2, 2})
	f.Add([]byte{1, 2, 3, 2, 2, 4})
	f.Add([]byte{2, 3, 5, 0, 3, 7})

	less := func(a, b int) bool { return a < b }

	f.Fuzz(func(t *testing.T, input []byte) {
		const maxOps = 5
		ops := decodeFuzzOps(input, maxOps)
		if len(ops) == 0 {
			t.Skip()
		}

		m := New[int, int](less)
		records := make([]*fuzzRecord, len(ops))

		var wg sync.WaitGroup
		wg.Add(len(ops))
		for i, op := range ops {
			i, op := i, op
			go func() {
				defer wg.Done()
				rec := &fuzzRecord{index: i, op: op}
				rec.start = time.Now()
				switch op.typ % 3 {
				case 0: // Put
					old, replaced := m.Put(op.key, op.val)
					rec.put = &putResult{old: old, replaced: replaced}
				case 1: // Get
					value, ok := m.Get(op.key)
					rec.get = &getResult{value: value, ok: ok}
				case 2: // Delete
					value, ok := m.Delete(op.key)
					rec.del = &deleteResult{value: value, ok: ok}
				}
				rec.end = time.Now()
				records[i] = rec
			}()
		}
		wg.Wait()

		if !checkLinearizable(records) {
			t.Fatalf("non-linearizable history: %v", summarizeRecords(records))
		}
	})
}

func decodeFuzzOps(input []byte, maxOps int) []fuzzOp {
	if maxOps <= 0 {
		return nil
	}
	ops := make([]fuzzOp, 0, maxOps)
	for i := 0; i+2 < len(input) && len(ops) < maxOps; i += 3 {
		typ := input[i] % 3
		key := int(input[i+1] % 8)
		val := int(int8(input[i+2]))
		ops = append(ops, fuzzOp{typ: typ, key: key, val: val})
	}
	return ops
}

func checkLinearizable(records []*fuzzRecord) bool {
	n := len(records)
	if n == 0 {
		return true
	}

	deps := make([]uint32, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if !records[i].end.After(records[j].start) {
				deps[j] |= 1 << i
			}
		}
	}

	used := uint32(0)
	order := make([]*fuzzRecord, 0, n)

	var dfs func() bool
	dfs = func() bool {
		if len(order) == n {
			return validateSequential(order)
		}
		for i := 0; i < n; i++ {
			if used&(1<<i) != 0 {
				continue
			}
			if deps[i]&^used != 0 {
				continue
			}
			used |= 1 << i
			order = append(order, records[i])
			if dfs() {
				return true
			}
			order = order[:len(order)-1]
			used &^= 1 << i
		}
		return false
	}

	return dfs()
}

func validateSequential(order []*fuzzRecord) bool {
	model := make(map[int]int)
	for _, rec := range order {
		switch rec.op.typ % 3 {
		case 0:
			old, present := model[rec.op.key]
			if rec.put == nil {
				return false
			}
			if rec.put.replaced != present {
				return false
			}
			if present && rec.put.old != old {
				return false
			}
			if !present && rec.put.replaced {
				return false
			}
			model[rec.op.key] = rec.op.val
		case 1:
			expected, present := model[rec.op.key]
			if rec.get == nil {
				return false
			}
			if rec.get.ok != present {
				return false
			}
			if present && rec.get.value != expected {
				return false
			}
		case 2:
			expected, present := model[rec.op.key]
			if rec.del == nil {
				return false
			}
			if rec.del.ok != present {
				return false
			}
			if present {
				if rec.del.value != expected {
					return false
				}
				delete(model, rec.op.key)
			}
		}
	}
	return true
}

func summarizeRecords(records []*fuzzRecord) string {
	parts := make([]string, 0, len(records))
	for _, rec := range records {
		parts = append(parts, fmt.Sprintf("{%d %d %d}", rec.op.typ, rec.op.key, rec.op.val))
	}
	return fmt.Sprintf("%v", parts)
}
