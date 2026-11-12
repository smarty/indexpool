//go:build indexpool_debug

package indexpool

import (
	"errors"
	"testing"
)

func TestReleaseRejectsDoubleRelease(t *testing.T) {
	pool, _ := New(2)
	index := pool.Acquire()

	if err := pool.Release(index); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := pool.Release(index); !errors.Is(err, errNotHeld) {
		t.Fatalf("double release: expected errNotHeld, got %v", err)
	}

	// The rejected release must not have added a phantom index.
	_ = pool.Acquire()
	_ = pool.Acquire()
	if pool.TryAcquire() >= 0 {
		t.Fatal("a rejected release added an index")
	}
}

// The documented gap: a stale holder's release of an index that has since been re-acquired looks valid.
func TestReleaseCannotTellStaleHolderFromCurrent(t *testing.T) {
	pool, _ := New(1) // one index, so the second Acquire is certain to get the first one's
	first := pool.Acquire()
	_ = pool.Release(first)

	second := pool.Acquire()
	if second != first {
		t.Fatalf("expected the same index back, got %d and %d", first, second)
	}
	if err := pool.Release(first); err != nil {
		t.Fatalf("expected the stale release to go undetected, got %v", err)
	}
}
