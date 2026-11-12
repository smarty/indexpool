package indexpool

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRejectsCountBelowOne(t *testing.T) {
	for _, count := range []int{0, -1} {
		if _, err := New(count); !errors.Is(err, errCount) {
			t.Errorf("count %d: expected errCount, got %v", count, err)
		}
	}
}

func TestReleaseRejectsUnknownIndex(t *testing.T) {
	pool, _ := New(2)
	for _, index := range []int{-1, 2} {
		if err := pool.Release(index); !errors.Is(err, errUnknownIndex) {
			t.Errorf("index %d: expected errUnknownIndex, got %v", index, err)
		}
	}
}

func TestFailedTryAcquireCannotReleaseIndexZero(t *testing.T) {
	pool, _ := New(1)
	held := pool.Acquire()

	failed := pool.TryAcquire()
	if failed != -1 {
		t.Fatalf("expected -1 with the only index held, got %d", failed)
	}
	if err := pool.Release(failed); !errors.Is(err, errUnknownIndex) {
		t.Fatalf("releasing a failed index: expected errUnknownIndex, got %v", err)
	}
	if pool.TryAcquire() >= 0 {
		t.Fatal("releasing a failed index freed the held one")
	}
	_ = pool.Release(held)
}

func TestIndexesAreDenseAndEachHandedOutOnce(t *testing.T) {
	const count = 16
	pool, _ := New(count)

	seen := make([]bool, count)
	for range count {
		index := pool.TryAcquire()
		if index < 0 || index >= count || seen[index] {
			t.Fatalf("unexpected acquire: index=%d", index)
		}
		seen[index] = true
	}
	if pool.TryAcquire() >= 0 {
		t.Fatal("expected every index acquired")
	}
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	pool, _ := New(1)
	held := pool.Acquire()

	result := make(chan int)
	go func() { result <- pool.Acquire() }()

	select {
	case index := <-result:
		t.Fatalf("acquire returned %d while the only index was held", index)
	case <-time.After(50 * time.Millisecond):
	}

	_ = pool.Release(held)
	select {
	case index := <-result:
		if index != held {
			t.Fatalf("expected %d, got %d", held, index)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

// The liveness case upstream's owner-only private slot would fail: each index is released into the private slot of
// whatever P its goroutine ran on, and those goroutines then go idle. Every index must still be reachable from here.
// It is probabilistic: Go has no public way to put a goroutine on a chosen P, so if the scheduler happens to run every
// release on this test's own P, the test passes without exercising a steal from another P's private slot.
func TestIndexesInIdlePsPrivateSlotsAreReachable(t *testing.T) {
	const count = 8
	pool, _ := New(count)

	held := make([]int, count)
	for index := range held {
		held[index] = pool.Acquire()
	}
	var waiter sync.WaitGroup
	for _, index := range held {
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			_ = pool.Release(index)
		}()
	}
	waiter.Wait()

	done := make(chan struct{})
	go func() {
		for range count {
			_ = pool.Acquire()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("an index released on another P was unreachable")
	}
}

// The pool is built at GOMAXPROCS=1, so raising it makes every new P grow locals through pinSlow; shrinking it again
// leaves locals with no P, whose indexes must still be stolen.
func TestIndexesSurviveGarbageCollectionAndGOMAXPROCSChanges(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer func() { _ = runtime.GOMAXPROCS(previous) }()

	const count = 32
	pool, _ := New(count)
	for _, procs := range []int{8, 2, max(previous, 8)} {
		_ = runtime.GOMAXPROCS(procs)
		held := make([]int, count)
		for index := range held {
			held[index] = pool.Acquire()
		}
		for _, index := range held {
			_ = pool.Release(index)
		}
		runtime.GC()
		runtime.GC()
	}

	for range count {
		if pool.TryAcquire() < 0 {
			t.Fatal("an index was lost")
		}
	}
	if pool.TryAcquire() >= 0 {
		t.Fatal("more indexes than exist")
	}
}

// Each holder marks its index in use and checks no one else had it. Few indexes and many workers force stealing and
// blocking, and a GC every 500 iterations checks nothing depends on GC timing. Afterwards exactly the original indexes
// remain.
func TestConcurrentHoldersNeverShareAnIndex(t *testing.T) {
	for _, count := range []int{1, 3, 64} {
		pool, _ := New(count)
		workers := 4 * runtime.GOMAXPROCS(0)
		inUse := make([]atomic.Bool, count)

		var failures atomic.Int64
		var waiter sync.WaitGroup
		for worker := range workers {
			waiter.Add(1)
			go func() {
				defer waiter.Done()
				for iteration := range 100_000 / workers {
					index := pool.Acquire()
					if !inUse[index].CompareAndSwap(false, true) {
						_ = failures.Add(1)
					}
					runtime.Gosched()
					inUse[index].Store(false)
					if err := pool.Release(index); err != nil {
						_ = failures.Add(1)
					}
					if worker == 0 && iteration%500 == 0 {
						runtime.GC()
					}
				}
			}()
		}
		waiter.Wait()

		if total := failures.Load(); total > 0 {
			t.Fatalf("count=%d: %d acquires returned an index already held or failed to release", count, total)
		}
		for range count {
			if pool.TryAcquire() < 0 {
				t.Fatalf("count=%d: an index was lost", count)
			}
		}
		if pool.TryAcquire() >= 0 {
			t.Fatalf("count=%d: more indexes than exist", count)
		}
	}
}

// The tests below exercise the double-release checker, which exists only under -tags indexpool_debug; in normal builds
// they skip. They compile in every build, so they can't rot behind a tag nobody builds.

func TestReleaseRejectsDoubleRelease(t *testing.T) {
	if !debugChecks {
		t.Skip("needs -tags indexpool_debug")
	}
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
	if !debugChecks {
		t.Skip("needs -tags indexpool_debug")
	}
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

var benchmarkCounts = []int{16, 64, 256, 1024}

func BenchmarkIndexPool(b *testing.B) {
	for _, count := range benchmarkCounts {
		b.Run(fmt.Sprintf("indexes=%d", count), func(b *testing.B) {
			pool, _ := New(count)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = pool.Release(pool.Acquire())
				}
			})
		})
	}
}

// A buffered channel of indexes: the standard-library alternative README.md recommends at modest throughput.
func BenchmarkChannel(b *testing.B) {
	for _, count := range benchmarkCounts {
		b.Run(fmt.Sprintf("indexes=%d", count), func(b *testing.B) {
			indexes := make(chan int, count)
			for index := range count {
				indexes <- index
			}
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					indexes <- <-indexes
				}
			})
		})
	}
}

// sync.Pool holding 64 KiB buffers by pointer (so Put doesn't box a slice header): what indexpool replaces. It drops
// idle buffers at GC.
func BenchmarkSyncPool(b *testing.B) {
	const bufferSize = 64 << 10
	pool := sync.Pool{New: func() any { return new([bufferSize]byte) }}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Put(pool.Get().(*[bufferSize]byte))
		}
	})
}

// Acquire every index, release them all, then two GCs. The pool makes no allocations (0 allocs/op), and a stock
// sync.Pool would have to rebuild its internals after the GCs. A few bytes/op may still show up, amortized from the
// runtime.
func BenchmarkRefillAfterIdle(b *testing.B) {
	const count = 256
	pool, _ := New(count)
	burst := make([]int, count)
	for range b.N {
		for index := range burst {
			burst[index] = pool.Acquire()
		}
		for _, index := range burst {
			_ = pool.Release(index)
		}
		runtime.GC()
		runtime.GC()
	}
}

// These measure the per-round-trip cost of the primitives a private slot could use, single goroutine and
// uncontended, which is what chose Swap. Each iteration is one Get plus one Put.

type paddedWord struct {
	value atomic.Uint64
	plain uint64
	_     [112]byte
}

// plainSlot is a uint64 token in memory, like indexpool's private slot, reached through two package-level pointers to
// the same word. The compiler can't prove they're equal, so it must keep the store, the load, and the second store.
// Through a local variable it deleted the loop body and measured only loop overhead; through the field directly it
// forwarded the load and dropped the first store.
var plainSlot paddedWord

var plainPut, plainGet = &plainSlot.plain, &plainSlot.plain

// What an owner-only private slot (upstream sync.Pool's design) would cost: plain loads and stores.
func BenchmarkPrimitivePlain(b *testing.B) {
	put, get := plainPut, plainGet
	token := uint64(1)
	for range b.N {
		*put = token // Put
		token = *get // Get
		*get = 0     // Get empties the slot
	}
	_ = token
}

// One CAS per operation, as hinted's per-slot flag does.
func BenchmarkPrimitiveCAS(b *testing.B) {
	var flag paddedWord
	for range b.N {
		_ = flag.value.CompareAndSwap(0, 1)
		_ = flag.value.CompareAndSwap(1, 0)
	}
}

// One Swap per operation, as indexpool's private slot does.
func BenchmarkPrimitiveSwap(b *testing.B) {
	var slot paddedWord
	for range b.N {
		_ = slot.value.Swap(1)
		_ = slot.value.Swap(0)
	}
}

// The Dekker owner path per operation: Store busy, Load stealing, plain access to the slot in memory, Store busy=0.
// It avoids a read-modify-write only where atomic Store isn't one (arm64 STLR; amd64 compiles Store to XCHG).
func BenchmarkPrimitiveDekker(b *testing.B) {
	var busy, stealing paddedWord
	put, get := plainPut, plainGet
	token := uint64(1)
	for range b.N {
		busy.value.Store(1) // Put
		if stealing.value.Load() == 0 {
			*put = token
		}
		busy.value.Store(0)

		busy.value.Store(1) // Get
		if stealing.value.Load() == 0 {
			token = *get
			*get = 0
		}
		busy.value.Store(0)
	}
	_ = token
}
