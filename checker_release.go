//go:build !indexpool_debug

package indexpool

// debugChecks tells the tests whether Release checks for double releases. Normal builds set it to false; code that
// depends on it compiles away.
const debugChecks = false

// checker is empty in normal builds so the hot path has no per-index state to touch; the compiler inlines these away.
type checker struct{}

func newChecker(int) checker { return checker{} }

func (checker) acquire(int) {}

func (checker) release(int) error { return nil }
