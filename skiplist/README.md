# skiplist

A lock-free skip list implementation in Go.

## Algorithm sketch and API surface

The public API mirrors the deliverables described in the accompanying research
notes:

* `Put(k, v) (old, replaced)` inserts or replaces a key.
* `Get(k) (v, ok)` looks up a key.
* `Delete(k) (old, ok)` removes a key and reports the value that was present.
* `Contains(k) bool` observes presence.
* `LenInt64() int64` returns the number of live keys via an atomic counter.
* `SeekGE(k) *Iterator` positions an iterator at the first key ≥ `k`.

Searches walk the tower from the top level down while helping unlink marker
nodes that represent logically deleted elements. Insertions reuse that traversal
to capture predecessor/successor pairs and then perform a single CAS on level 0
to splice in the new node. Once that bottom-level CAS succeeds, the insert is
considered linearized; higher levels are linked opportunistically, retrying on
contention but without affecting the logical presence of the key. Deletions
follow the two-phase protocol described in the research: clear the value pointer
(`value → nil`) to achieve a logical delete, insert a marker node on the next
pointer, and then help predecessors swing past the marker. Helping ensures that
long chains of markers are collapsed during subsequent traversals.

Benchmarking support is exposed via `InsertCASStats`, which reports retries and
successful CAS operations at level 0 so that contention can be observed directly
in benchmark output.

## Operation guarantees

* **Insert (`Put`)** linearizes at the level-0 CAS that links the new node into
  the base list. Presence checks (e.g., `Get`, `Contains`, and iterator
  traversals) validate their results against the bottom level to guarantee that
  they observe only fully inserted nodes.
* **Delete** transitions `value → nil`, splices in a marker node, and unlinks the
  node with helping from concurrent operations. Because a pointer never reverts
  to a previous value (`node → marker → successor`), the algorithm avoids ABA
  while remaining compatible with the Go runtime’s garbage collector.

## Memory management

This implementation targets Go's garbage-collected runtime. Nodes are never
manually freed; once a node (or temporary marker node created during deletion)
is no longer reachable from the head sentinel, it becomes eligible for
collection by the Go GC. This sidesteps the hazard-pointer or epoch-based
reclamation schemes required in lock-free skip lists written for manual-memory
management languages.

To avoid the ABA problem while still cooperating with the GC, deletions follow
the marker-node pattern described in the research. A node transitions from a
live value to a logically deleted state by setting its value pointer to `nil`.
Before the node is physically unlinked, we splice in a dedicated marker node.
Each predecessor CAS observes a monotonic sequence of pointer values: live node
→ marker → successor. Because a pointer never reverts to an earlier value, CAS
operations do not suffer from ABA even though memory is reclaimed lazily by the
runtime. In manual-memory environments (e.g., C/C++), the same algorithm would
pair naturally with hazard pointers or epoch-based reclamation to ensure that
deleted nodes remain protected until no goroutine retains a reference.
