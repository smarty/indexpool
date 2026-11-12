// Portions of this file are derived from Go's src/sync/poolqueue.go:
// Copyright 2019 The Go Authors. All rights reserved.
// Used under the BSD-style license in LICENSE-GO.

package indexpool

import "sync/atomic"

// dequeue is sync's poolDequeue specialized to slot tokens (index+1, so 0 means empty): a fixed-size, lock-free ring
// with a single producer, which pushes and pops the head, and any number of consumers, which pop the tail. Each slot
// is one atomic.Uint64 and doubles as its own ownership flag, replacing upstream's eface type word.
type dequeue struct {
	// headTail packs a 32-bit head index (high bits) and a 32-bit tail index (low bits), both mod 2^32. The live
	// slots are tail ≤ i < head. One word means both ends are read and claimed in a single atomic step.
	headTail atomic.Uint64
	slots    []atomic.Uint64 // length is a power of two
}

const dequeueBits = 32

// dequeueLimit keeps head and tail from wrapping into each other: the gap between them must fit in dequeueBits.
const dequeueLimit = (1 << dequeueBits) / 4

func unpack(headTail uint64) (head, tail uint32) {
	return uint32(headTail >> dequeueBits), uint32(headTail)
}

func pack(head, tail uint32) uint64 {
	return uint64(head)<<dequeueBits | uint64(tail)
}

// pushHead adds token at the head. It returns false when the ring is full, including when the tail slot is still
// being cleared by a popTail that has claimed it but not yet released it. Producer only.
func (this *dequeue) pushHead(token uint64) bool {
	head, tail := unpack(this.headTail.Load())
	if tail+uint32(len(this.slots)) == head {
		return false
	}
	slot := &this.slots[head&uint32(len(this.slots)-1)]
	if slot.Load() != empty {
		return false // a popTail claimed this slot and hasn't cleared it yet
	}
	slot.Store(token)

	// Publish the slot. Only the producer moves head, so Add can't race another head update; it can race a popTail
	// moving tail, which Add tolerates and Store wouldn't.
	_ = this.headTail.Add(1 << dequeueBits)
	return true
}

// popHead removes the token at the head, or returns empty. Producer only.
func (this *dequeue) popHead() uint64 {
	for {
		headTail := this.headTail.Load()
		head, tail := unpack(headTail)
		if tail == head {
			return empty
		}

		// Claim the slot before reading it; a concurrent popTail may be after the same last token.
		head--
		if this.headTail.CompareAndSwap(headTail, pack(head, tail)) {
			return this.slots[head&uint32(len(this.slots)-1)].Swap(empty)
		}
	}
}

// popTail removes the token at the tail, or returns empty. Any number of consumers may call it.
func (this *dequeue) popTail() uint64 {
	for {
		headTail := this.headTail.Load()
		head, tail := unpack(headTail)
		if tail == head {
			return empty
		}

		if this.headTail.CompareAndSwap(headTail, pack(head, tail+1)) {
			// The Swap is also the release: pushHead won't reuse the slot until it reads empty here.
			return this.slots[tail&uint32(len(this.slots)-1)].Swap(empty)
		}
	}
}

// chain is sync's poolChain: a list of dequeues, each twice the size of the one before, so a P's shared queue grows
// without copying. The producer works the newest dequeue (head); consumers drain from the oldest (tail) and drop
// dequeues they empty. With a fixed slot count the chain stops growing once a dequeue is longer than the slot count:
// a dequeue exactly as long as the slot count can still fill and trigger one more doubling.
type chain struct {
	// head is touched only by the owning P, but successive goroutines on that P are ordered by the scheduler's P
	// handoff, which the race detector can't see; sync.Pool hides that with race.Disable, which a fork can't import.
	// Atomic Load and Store make the ordering visible at the cost of no read-modify-write.
	head atomic.Pointer[chainElement]
	tail atomic.Pointer[chainElement]
}

type chainElement struct {
	dequeue

	// next is written by the producer and read by consumers; it only ever goes from nil to non-nil. prev is written
	// by consumers and read by the producer; it only ever goes from non-nil to nil.
	next, prev atomic.Pointer[chainElement]
}

func (this *chain) pushHead(token uint64) {
	element := this.head.Load()
	if element == nil {
		const initialSize = 8 // must be a power of 2
		element = &chainElement{dequeue: dequeue{slots: make([]atomic.Uint64, initialSize)}}
		this.head.Store(element)
		this.tail.Store(element)
	}
	if element.pushHead(token) {
		return
	}

	// The current dequeue is full: start a new one twice the size.
	grown := &chainElement{dequeue: dequeue{slots: make([]atomic.Uint64, min(2*len(element.slots), dequeueLimit))}}
	grown.prev.Store(element)
	this.head.Store(grown)
	element.next.Store(grown)
	_ = grown.pushHead(token) // a fresh dequeue always has room
}

func (this *chain) popHead() uint64 {
	for element := this.head.Load(); element != nil; element = element.prev.Load() {
		if token := element.popHead(); token != empty {
			return token
		}
	}
	return empty
}

func (this *chain) popTail() uint64 {
	element := this.tail.Load()
	if element == nil {
		return empty
	}

	for {
		// Load next before popping: if this dequeue is empty and next was already set, every push to it happened
		// before next was published, so empty here really means drained and it can be dropped.
		next := element.next.Load()
		if token := element.popTail(); token != empty {
			return token
		}
		if next == nil {
			return empty
		}

		// Drop the drained dequeue so the producer stops looking at it and GC can take it. Whoever wins the CAS
		// unlinks it; losers move on all the same.
		if this.tail.CompareAndSwap(element, next) {
			next.prev.Store(nil)
		}
		element = next
	}
}
