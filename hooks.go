package skiplist

// Test hooks (kept separate so instrumentation doesn't clutter logic).
var (
	getAfterFindHook func(node any) bool
	ensureMarkerHook func(node any)
	putLevelCASHook  func(level int, pred any, expected any, newNodePtr any)
)
