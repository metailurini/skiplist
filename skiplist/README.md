# skiplist

A lock-free skip list implementation in Go.

## Memory reclamation

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
runtime.
