//go:build !indexpool_debug

package indexpool

// checker is empty in normal builds so the hot path has no per-index state to touch; the compiler inlines these away.
type checker struct{}

func newChecker(int) checker { return checker{} }

func (checker) acquire(int) {}

func (checker) release(int) error { return nil }
