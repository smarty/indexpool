# indexpool

User-facing documentation, design, and measurements live in `README.md`. Keep it current with any change to the API,
behavior, or numbers.

## Commands

```bash
go vet ./... && go vet -tags indexpool_debug ./...
go test -race -count=3 ./... && go test -race -count=3 -tags indexpool_debug ./...
go test -run '^$' -bench . -benchmem -cpu 1,4,10 -count 3 .
```

Always test both builds: `checker_release.go` is the default, and `checker_debug.go` replaces it under
`indexpool_debug`.

## Invariants

- **Tokens are `index+1`, and 0 means empty.** That way the zero value of every atomic slot is empty.
- **Only the pinned owner calls `pushHead`/`popHead`.** The shared queue's head is single-producer. `New` may fill
  queues before the pool is published. Steals (`popTail`, private `Swap`) run unpinned.
- **Nothing blocks while pinned.** No mutex, no `Cond`, no channel between `runtime_procPin` and `runtime_procUnpin`.
- **Release publishes its token, including any displaced push, before it reads `waiters`.** The waiter increments
  `waiters` before its final scan, and holds the mutex until `Wait`. Reordering either side reintroduces lost wakeups.
- **Growing `locals` copies the pointers.** Never drop a `*local`.
- **Owner-only fields stay atomic** (`chain.head`), so the race detector sees the ordering; a fork can't use
  `internal/race`.
- **The release build must cost nothing.** `checker_release.go`'s methods must inline (`go build -gcflags=-m`).
- **The public API is five elements:** `Pool`, `New`, `Acquire`, `TryAcquire` (returns `-1` for none), and `Release`.
  Check with `go doc -all .`. Errors stay unexported, and the pool knows nothing about the slice its indexes address.

## Pitfalls

- **Benchmarks:**
  - After editing `primitives_test.go`, confirm with `go tool objdump` that each loop still loads and stores. The
    compiler once deleted the plain loop's body, and once forwarded its load.
  - Compare numbers only within one benchmark session; `sync.Pool`'s own time moved 12% between sessions.
- **Tests:** `TestIndexesSurviveGarbageCollectionAndGOMAXPROCSChanges` sets `GOMAXPROCS`, so don't mark it
  `t.Parallel()`.
- **Upstream:** `indexpool` is a fork of Go 1.27.4's `sync/pool.go` and `poolqueue.go`. Re-diff when upstream changes.
  Keep the attribution headers on `queue.go` and `indexpool.go` and the verbatim `LICENSE-GO`: Go's BSD license
  requires them for the derived code.

## FUTURE

- Profile the intermittent slow 4-core runs. The suspect is placement on efficiency cores.
- Add `context` cancellation to `Acquire`. It needs a channel-based wait, because `sync.Cond` can't drop a waiter.
- Measure throughput when the pool is empty most of the time; every benchmark so far keeps indexes available.
- Measure on amd64.
