// Portions of this file (local, pin, pinSlow, and the steal loop) are derived from Go's src/sync/pool.go:
// Copyright 2013 The Go Authors. All rights reserved.
// Used under the BSD-style license in LICENSE-GO.

// Package indexpool tracks which slots of a slice are available. It hands out the integers 0..count-1, each to at most
// one holder at a time, and blocks when every slot is taken. The slice is the caller's: a []T of reusable objects,
// connections, or workers, or the slots of one byte buffer. The pool only moves integers, so pairing it with a []T
// gives a typed object pool that, unlike sync.Pool, never drops an object at GC.
//
// Its free list is a clone of sync.Pool holding indexes, with two changes. There is no GC hook, so no index is ever
// dropped and nothing is re-allocated or rescanned after a GC. And each P's private slot is taken and filled with a
// single Swap instead of plain loads and stores, so a blocked Acquire can steal from any P; upstream's owner-only
// private slot would strand an index on an idle P forever.
//
// When an index is acquired and released on the same P, each is one Swap: no CAS, no per-index flag. When they happen
// on different Ps, Release also pushes the token it displaced onto its shared queue (one atomic Add), and Acquire takes
// from a shared queue (one CAS) instead of its private slot. Build with -tags indexpool_debug to add double-release
// detection, which needs a per-index CAS on every Release.
package indexpool

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	_ "unsafe" // go:linkname
)

// Pool hands out the integers 0..count-1, each to at most one holder at a time.
type Pool struct {
	count   int
	checker checker // no-op unless built with indexpool_debug

	// locals holds one *local per P. Growing replaces the slice but keeps every existing *local, so nothing is ever
	// abandoned the way upstream's pinSlow abandons its array when GOMAXPROCS changes.
	locals atomic.Pointer[[]*local]
	grow   sync.Mutex

	// waiters is read on every Release but written only when an Acquire finds nothing, so in steady state its line
	// stays shared in every core's cache and costs a plain load.
	waiters atomic.Int64
	mutex   sync.Mutex
	wake    *sync.Cond
}

type local struct {
	private atomic.Uint64 // a token (index+1) or empty; owner and stealers alike take it with Swap
	shared  chain         // the owning P pushes and pops the head; anyone steals from the tail
	_       [128 - 24]byte
}

// empty is the token for "no index"; index i travels as token i+1 so the zero value of every atomic means empty.
const empty = 0

var (
	errCount        = errors.New("indexpool: count must be at least 1")
	errUnknownIndex = errors.New("indexpool: index out of range")
	errNotHeld      = errors.New("indexpool: index is not acquired") // only with -tags indexpool_debug
)

// New returns a pool of count indexes, all free. It fails only if count < 1. The indexes are dealt round-robin across
// the Ps, so each P starts with a share instead of every P stealing from one.
func New(count int) (*Pool, error) {
	if count < 1 {
		return nil, errCount
	}

	this := &Pool{count: count, checker: newChecker(count)}
	this.wake = sync.NewCond(&this.mutex)

	locals := make([]*local, runtime.GOMAXPROCS(0))
	for index := range locals {
		locals[index] = new(local)
	}
	// Nothing else can see the pool yet, so filling other Ps' heads from this goroutine is safe.
	for index := range count {
		target := locals[index%len(locals)]
		if target.private.Load() == empty {
			target.private.Store(uint64(index) + 1)
		} else {
			target.shared.pushHead(uint64(index) + 1)
		}
	}
	this.locals.Store(&locals)
	return this, nil
}

// Acquire returns a free index, blocking until one is released.
func (this *Pool) Acquire() int {
	token := this.take()
	if token == empty {
		token = this.acquireSlow()
	}
	return this.acquired(token)
}

// TryAcquire returns a free index without blocking, or -1 if none is free just now, as strings.Index and slices.Index
// do for "not found". It can return -1 while an index is free: a Release moves the token it displaces from its private
// slot to its shared queue, and a steal racing that move can miss it. Acquire covers that window with the waiter
// protocol; TryAcquire has none. Releasing -1 returns an error, and indexing a slice with it panics.
func (this *Pool) TryAcquire() int {
	token := this.take()
	if token == empty {
		return -1
	}
	return this.acquired(token)
}

// take tries this P's private slot, then its shared queue, then steals from every other P.
func (this *Pool) take() uint64 {
	local, pid := this.pin()
	token := local.private.Swap(empty)
	if token == empty {
		token = local.shared.popHead()
	}
	runtime_procUnpin()

	if token == empty {
		token = this.steal(pid)
	}
	return token
}

// steal takes from every P's shared tail, then every P's private slot. It needn't be pinned: popTail is safe for any
// consumer, and Swap on a private slot is safe against its owner. Starting after pid spreads stealers out.
func (this *Pool) steal(pid int) uint64 {
	locals := *this.locals.Load()
	for offset := range locals {
		if token := locals[(pid+offset+1)%len(locals)].shared.popTail(); token != empty {
			return token
		}
	}
	for offset := range locals {
		if token := locals[(pid+offset+1)%len(locals)].private.Swap(empty); token != empty {
			return token
		}
	}
	return empty
}

// acquireSlow registers as a waiter before its last steal. Release publishes its token before it reads waiters, and
// sequentially consistent atomics mean either that steal sees the token or Release sees the waiter and signals. The
// mutex is held from registration to Wait, so the signal can't fall into the gap between them.
//
// One token can be briefly invisible: Release swaps its own token into the private slot and only then pushes the one
// it displaced onto the shared queue. A steal in that window misses the displaced token, but it can still find the new
// one, and Release reads waiters only after the push, so a waiter that missed it is signalled.
func (this *Pool) acquireSlow() uint64 {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	_ = this.waiters.Add(1)
	defer func() { _ = this.waiters.Add(-1) }()

	for {
		if token := this.steal(0); token != empty {
			return token
		}
		this.wake.Wait()
	}
}

// Release returns index to the pool. It fails if index is out of range. A double release is not detected unless built
// with -tags indexpool_debug: it would put the index in the pool twice and let two holders share it. Even then, only
// the release of an index that isn't acquired is caught; a stale holder releasing an index that has since been
// re-acquired is not.
func (this *Pool) Release(index int) error {
	if index < 0 || index >= this.count {
		return errUnknownIndex
	}
	if err := this.checker.release(index); err != nil {
		return err
	}

	local, _ := this.pin()
	if displaced := local.private.Swap(uint64(index) + 1); displaced != empty {
		local.shared.pushHead(displaced)
	}
	runtime_procUnpin()

	if this.waiters.Load() == 0 {
		return nil
	}
	this.mutex.Lock()
	this.wake.Signal()
	this.mutex.Unlock()
	return nil
}

func (this *Pool) acquired(token uint64) int {
	index := int(token - 1)
	this.checker.acquire(index)
	return index
}

// pin pins the goroutine to its P and returns that P's local. The caller must call runtime_procUnpin when done and
// must not block in between: the shared queue's head is single-producer, and pinning is what makes this P's goroutine
// the only producer.
func (this *Pool) pin() (*local, int) {
	pid := runtime_procPin()
	if locals := *this.locals.Load(); pid < len(locals) {
		return locals[pid], pid
	}
	return this.pinSlow()
}

// pinSlow grows locals to cover the current P after GOMAXPROCS grows. It must unpin to take the mutex, may come back
// on a different P, and keeps every existing *local so nothing is dropped.
func (this *Pool) pinSlow() (*local, int) {
	runtime_procUnpin()
	this.grow.Lock()
	defer this.grow.Unlock()

	pid := runtime_procPin()
	current := *this.locals.Load()
	if pid < len(current) {
		return current[pid], pid
	}

	// GOMAXPROCS can't change while pinned, so pid < GOMAXPROCS(0) here; sizing to it at least covers pid.
	grown := make([]*local, max(runtime.GOMAXPROCS(0), pid+1))
	_ = copy(grown, current)
	for index := len(current); index < len(grown); index++ {
		grown[index] = new(local)
	}
	this.locals.Store(&grown)
	return grown[pid], pid
}

// runtime.procPin and procUnpin are on the runtime's sanctioned linkname list (go.dev/issue/67401), kept stable because
// bytedance/gopkg and others depend on them, so this needs no -checklinkname=0.
//
//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin()
