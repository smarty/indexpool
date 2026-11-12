# indexpool

`indexpool` tracks which slots of a slice are available. It hands out the indexes `0..count-1`, each to at most one
holder at a time, and blocks when every slot is taken. The slice is yours: reusable objects, connections, workers, or
the slots of one byte buffer. Pair a `[]T` with an `indexpool` and you have a typed object pool that, unlike
`sync.Pool`, never drops an object at GC and never boxes one in an interface.

- **Close to `sync.Pool`'s speed.** At 10 cores a round trip measured 0.89–0.94 ns against `sync.Pool`'s 0.84–0.86 ns
  (Apple M5; see [Performance](#performance)).
- **Never loses a slot.** Nothing is dropped at GC, and nothing is allocated after `New`.
- **Blocks when every slot is in use,** which gives backpressure. `TryAcquire` sheds load instead.
- **Only an integer moves around.** The free list holds no pointers, so the GC never scans it, and an index can cross
  a channel, a ring buffer, or the kernel boundary.

```bash
go get github.com/smarty/indexpool
```

## API

```go
func New(count int) (*Pool, error)     // count indexes, all free; fails only if count < 1
func (*Pool) Acquire() int             // a free index, blocking until one is released
func (*Pool) TryAcquire() int          // a free index without blocking, or -1 if none is free just now
func (*Pool) Release(index int) error  // fails if index is out of range
```

That's the whole public surface.

- **Indexes are dense,** `0..count-1`, so they address your slice directly: `&items[pool.Acquire()]`.
- **`TryAcquire` returns `-1` for none,** as `strings.Index` and `slices.Index` do. Indexing a slice with an unchecked
  `-1` panics, and releasing it returns an error, so a forgotten check fails loudly. It can return `-1` while a slot is
  free, if it races a release on another core; treat `-1` as "none free just now".
- **There's no ordering or fairness.** A blocked `Acquire` can be overtaken, and it can't time out or be cancelled.

### Rules the caller owns

- **Release each index exactly once, and stop using its slot once you do.** Another goroutine may already hold it.
  - **A normal build doesn't detect a double release.** It would put the index in the pool twice, and two holders
    would then share the slot.
  - **Build or test with `-tags indexpool_debug`** to make `Release` fail for an index that isn't acquired. It can't
    catch a stale holder releasing an index that has since been re-acquired, because a bare `int` carries no
    generation.
- **Never wait for a second slot while holding one.** If every holder does that, they deadlock: a fixed pool can't
  satisfy nested acquires once every slot is out. Take what you need in one place, or use `TryAcquire` for the
  extras.
- **Size the pool to peak concurrent use.** That's the most that can be in flight. Past it, `Acquire` blocks.

## Examples

Both are in `example_test.go`, so `go test` checks their output.

### Reusable objects in a slice

Twenty concurrent requests share four reusable sessions. Each request acquires a slot, uses the session in it, and
releases the slot; a request that finds all four in use blocks until one is released.

```go
type session struct {
	buffer bytes.Buffer // reused across requests, so its storage is allocated once and kept
	uses   int
}
sessions := make([]session, 4)
pool, _ := indexpool.New(len(sessions))

for request := range 20 {
	go func() {
		index := pool.Acquire() // blocks while all 4 sessions are in use
		current := &sessions[index]
		current.buffer.Reset()
		fmt.Fprintf(&current.buffer, "request %d", request)
		current.uses++
		_ = pool.Release(index) // the session is off-limits from here on
	}()
}
// Output: 20 requests served by 4 sessions
```

### Slots of one byte buffer

The slice can be one byte buffer cut into fixed slots. The pool knows nothing about the buffer: slot `index` is
`backing[index*slotSize : (index+1)*slotSize]`, computed by the caller. A producer frames 100 messages directly into
acquired slots and sends only the index over a channel. Four workers recover the bytes from the index, process them,
and release it.

```go
const slotSize, slotCount = 64 << 10, 16 // 1 MiB of backing
backing := make([]byte, slotCount*slotSize)
slot := func(index int) []byte {
	start := index * slotSize
	return backing[start : start+slotSize : start+slotSize] // capped, so append can't spill into the next slot
}

pool, _ := indexpool.New(slotCount)
indexes := make(chan int, slotCount)

for range 4 { // workers
	go func() {
		for index := range indexes {
			bytes := slot(index)
			length := binary.LittleEndian.Uint32(bytes)
			process(bytes[4 : 4+length])
			_ = pool.Release(index) // the slot is off-limits from here on
		}
	}()
}

for message := range 100 { // producer
	index := pool.Acquire() // blocks while all 16 slots are in flight
	bytes := slot(index)
	length := copy(bytes[4:], fmt.Sprintf("message %d", message))
	binary.LittleEndian.PutUint32(bytes, uint32(length))
	indexes <- index
}
// Output: 100 messages, 990 payload bytes, 16 of 16 indexes free
```

```
                 backing buffer: 1 MiB, one allocation, owned by the caller
  ┌────────┬────────┬────────┬─────┬────────┐
  │ slot 0 │ slot 1 │ slot 2 │ ... │ slot 15│   slot(index) = backing[index*64K : (index+1)*64K : (index+1)*64K]
  └────────┴────────┴────────┴─────┴────────┘
       ▲ write in place          ▲ read in place: slot(index)
       │                         │
  producer ── Acquire ──► index ──► chan int ──► worker ── Release(index) ──► indexpool (per-P, Swap)
      ▲                                                                            │
      └────────── blocks here when all 16 slots are in flight ◄─────────────────────┘
```

- **No per-message allocation and no copy between goroutines:** only an `int` crosses the channel.
- **Backpressure:** the producer can't outrun the workers by more than 16 messages.
- **Everything is released:** after the run, all 16 slots are free.

`ExamplePool_TryAcquire` shows the load-shedding alternative: a third request against two slots gets `-1` at once.

### Why one contiguous byte buffer

You own the buffer, so you choose its source and alignment: `make`, `mmap`, or huge pages. Go's `make` page-aligns
large allocations in practice but doesn't promise it, which matters for `O_DIRECT`. An integer index into one block
is useful beyond Go:

- **io_uring.** Register the buffer with `IORING_REGISTER_BUFFERS`, and every fixed read or write skips per-call page
  pinning. Either register it as one iovec and address slot `index` at `base + index*slotSize` with `buf_index` 0,
  or register one iovec per slot so the index *is* the `buf_index`. The second form is capped by the kernel's limit
  on registered buffers.
- **RDMA and `mlock`.** One memory region or one `mlock` covers every slot.
- **Huge pages.** `madvise(MADV_HUGEPAGE)` makes each 2 MiB page physically contiguous (not the whole block) and cuts
  TLB entries: 16 MiB of slots needs 8 entries at 2 MiB pages instead of 4,096 at 4 KiB.
- **The GC** sees one pointer-free object instead of one per buffer.

Contiguity doesn't buy cache-prefetch locality: prefetchers stop at 4 KiB page boundaries.

## When not to use it

| Situation | Use instead | Why |
|---|---|---|
| Objects or buffers vary widely in size | `sync.Pool` per size class | Every slot is one fixed shape. A 100-byte message in a 64 KiB slot wastes 99.8% of it, and a payload over the slot size doesn't fit at all. |
| Memory should shrink back after a burst | `sync.Pool` | Dropping idle items at GC is `sync.Pool`'s feature. A fixed pool keeps its whole slice for life. |
| A miss must never wait, and allocating is acceptable | `sync.Pool` | `Acquire` blocks. `TryAcquire` with a fallback allocation works, but the fallback has no slot. |
| Concurrency can't be bounded, or code holds one slot while acquiring another | `sync.Pool` | A fixed pool deadlocks on nested acquires once every slot is out, and blocks under unbounded load. |
| Throughput is modest (well under millions of acquires per second) and simplicity matters most | A buffered `chan int` of indexes | 13–22 ns per round trip, standard library only, no linkname, and fair. The extra 20 ns doesn't matter at that rate. |
| `go:linkname` to `runtime.procPin` isn't acceptable (policy, gccgo or TinyGo, or a future Go that drops it) | A stock `sync.Pool` caching index hints, with a per-slot atomic flag as the truth | Claim a hint with a CAS on its flag; on a miss, scan the flags. That design measured 1.22–1.30 ns at 10 cores, needs no linkname, and catches a release of a free slot in every build. |
| Waiters need FIFO fairness, a timeout, or `context` cancellation | A channel or `golang.org/x/sync/semaphore` | `Acquire` has none of these. |

## How it works

The free list is a clone of Go 1.27.4's `sync.Pool` (`src/sync/pool.go` and `poolqueue.go`) holding indexes instead
of pointers. Each P (the scheduler's per-core context) has a private slot and a lock-free shared queue. The owner
pushes and pops the queue's head, and any P can steal from its tail.

| Upstream `sync.Pool` | `indexpool` | Why |
|---|---|---|
| Drops idle items at every GC | No GC hook | Nothing is ever lost or re-allocated. |
| Abandons its per-P array when `GOMAXPROCS` grows | Keeps every per-P structure | No index is stranded. Since Go 1.25 the runtime can change `GOMAXPROCS` on its own. |
| Private slot is owner-only (plain loads and stores) | Private slot uses atomic `Swap`, so any P can steal it | In a fixed, blocking pool, an index parked on an idle P would otherwise strand a blocked `Acquire` forever. |
| Stores `any` | Stores `index+1` in an `atomic.Uint64` | No boxing and no pointers for the GC to scan. |

**Cost per operation.** An index acquired and released on the same P costs one `Swap` each. Across Ps, `Release`
also pushes the token its `Swap` displaced onto the shared queue (one atomic `Add`), and `Acquire` takes from a shared
queue (one CAS). A producer handing indexes to workers on other cores pays those on nearly every message.

**Why `Swap`.** Measured per Get+Put pair, single goroutine, on the M5 (`BenchmarkPrimitive*`):

| Option for the private slot | Per Get+Put pair |
|---|---|
| Plain loads and stores (owner-only, upstream's design) | 0.32–0.33 ns |
| Dekker handshake (atomic Store, Load, plain access, Store) | 0.89 ns |
| `Swap`, one per operation | 1.09 ns |
| CAS, one per operation | 2.74–2.76 ns |

`Swap` costs about 0.77 ns over an owner-only slot: that is the price of being stealable. Dekker is about 0.2 ns
cheaper on arm64 only. On amd64 Go compiles atomic `Store` to `XCHG`, so Dekker would need four read-modify-writes
per pair against `Swap`'s two. That's reasoned from the instructions, not measured on amd64.

**Blocking.** It follows Linux's `percpu_ida` tag allocator: register as a waiter, then make a final scan.

1. An `Acquire` that finds nothing takes a mutex, increments a waiter count, and scans every P.
2. If the scan finds nothing, it parks on a `sync.Cond`.
3. `Release` publishes its index, then reads the waiter count, and signals only if it's non-zero.

Go's atomics are sequentially consistent, so either the scan sees the index or `Release` sees the waiter. Holding the
mutex from registration to `Wait` keeps a signal from arriving early. When nobody waits, `Release` pays one load of a
cache line that stays shared.

**Pinning.** The current P comes from `runtime.procPin` via `go:linkname`. It's on the runtime's sanctioned list
(go.dev/issue/67401) and builds without `-checklinkname=0`. Nothing blocks while pinned: stealing and waiting run
unpinned.

## Performance

Apple M5 (10 mixed performance and efficiency cores), macOS, Go 1.27.4. Each benchmark is `Release(Acquire())` in
`b.RunParallel`, with benchmarks run one at a time: 5 runs per cell in one session, at 16, 64, 256, and 1024 indexes.
`sync.Pool` holds 64 KiB buffers by pointer, as it would in the byte-buffer use case. These were measured just before
the rename to `indexpool` and the switch of `TryAcquire` to `-1`; neither changes the `Acquire`/`Release` path.

| | 1 core | 4 cores | 10 cores |
|---|---|---|---|
| **`indexpool`**, 16–1024 indexes | 5.14–5.47 ns | 1.37–1.45 ns typical; 6 of 20 runs 4.86–4.91 ns | 0.89–0.94 ns |
| Buffered `chan int` of indexes | 13.0–13.6 ns | 20.4–22.2 ns | 20.3–21.8 ns |
| `sync.Pool` (drops items at GC) | 4.97–5.10 ns | 1.36–1.42 ns | 0.84–0.86 ns |

- **Compare within a session.** Earlier sessions measured `sync.Pool`'s 1-core time 12% apart, more than some gaps
  between rows.
- **Slow 4-core runs come and go.** They appeared in some sessions and not others. The suspect is the scheduler
  placing work on efficiency cores; it hasn't been profiled.
- **Steady-state benchmarks hide `sync.Pool`'s leak.** It makes 0 allocations while its items are always in use. In
  `BenchmarkRefillAfterIdle` (acquire 256 indexes, release them, run two GCs), `indexpool` makes 0 allocations per
  refill. An earlier design built on a stock `sync.Pool` made 14 per refill, rebuilding its internal structures after
  the GCs.

### Design history

Designs measured on the way here, with one item per goroutine:

| Design | 1 core | 4 cores | 10 cores | Why it lost |
|---|---|---|---|---|
| Buffered channel | 12.6 ns | 24.5 ns | 23 ns | One lock shared by all: flat, but it never scales |
| Vyukov bounded MPMC ring (CAS on positions) | 5 ns | 94 ns | 270 ns | Four contended cache lines per round trip, plus CAS retry storms |
| Same ring with fetch-add tickets | 3.7 ns | 80 ns | ~1100 ns | Strict FIFO: one descheduled ticket holder stalls everyone |
| Tagged Treiber stack | 11 ns | 220 ns | 600 ns | One hot head word |
| `sync.Mutex` + slice stack | 7 ns | 218 ns | 138 ns | One hot lock word |
| Per-P shards of atomic pointer slots | 9.5 ns | 4.5 ns | 13.5 ns | Every slot atomic, plus linear shard scans |
| Stock `sync.Pool` of index hints + per-slot CAS flag | 7.1–7.5 ns | 2.0–2.1 ns | 1.22–1.30 ns | Two CASes per round trip and a rescan after every GC |

Under contention, any design with one shared hot cache line pays at least one cross-core transfer per operation,
about 100 ns or more on this chip. Per-P sharding is what got close to `sync.Pool`.

## License

MIT, Copyright (c) 2026 Smarty, LLC. See [LICENSE](LICENSE).

Portions of `queue.go` and `indexpool.go` are derived from Go's `src/sync/poolqueue.go` and `src/sync/pool.go`,
Copyright The Go Authors, and are used under Go's BSD-style license in [LICENSE-GO](LICENSE-GO). Each of those files
says so in its header.
