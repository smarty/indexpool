package indexpool

import (
	"sync/atomic"
	"testing"
)

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
