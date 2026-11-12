//go:build indexpool_debug

package indexpool

import "sync/atomic"

// checker tracks which indexes are acquired so Release can reject releasing one that isn't. It costs a Store per
// Acquire and a CAS per Release, which is why it exists only under -tags indexpool_debug. A bare int can't carry a
// generation, so a stale holder releasing an index that has since been re-acquired looks like a valid release and isn't
// caught.
type checker struct {
	held []atomic.Bool
}

func newChecker(count int) checker {
	return checker{held: make([]atomic.Bool, count)}
}

func (this checker) acquire(index int) {
	this.held[index].Store(true)
}

func (this checker) release(index int) error {
	if !this.held[index].CompareAndSwap(true, false) {
		return errNotHeld
	}
	return nil
}
