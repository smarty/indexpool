package indexpool_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/smarty/indexpool"
)

// The pool tracks which slots of a slice are free; the slice holds the objects. Together they are a typed object pool
// that, unlike sync.Pool, never drops an object at GC and never boxes one in an interface. Here 20 concurrent requests
// share 4 reusable sessions: each request acquires a slot, uses the session in it, and releases the slot. A request
// that finds all 4 in use blocks until one is released.
func Example() {
	type session struct {
		buffer bytes.Buffer // reused across requests, so its storage is allocated once and kept
		uses   int
	}
	sessions := make([]session, 4)

	pool, err := indexpool.New(len(sessions))
	if err != nil {
		_, _ = fmt.Println(err)
		return
	}

	var requests sync.WaitGroup
	for request := range 20 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			index := pool.Acquire() // blocks while all 4 sessions are in use
			current := &sessions[index]
			current.buffer.Reset()
			_, _ = fmt.Fprintf(&current.buffer, "request %d", request)
			current.uses++
			_ = pool.Release(index) // the session is off-limits from here on
		}()
	}
	requests.Wait()

	served := 0
	for index := range sessions {
		served += sessions[index].uses
	}
	_, _ = fmt.Printf("%d requests served by %d sessions\n", served, len(sessions))
	// Output: 20 requests served by 4 sessions
}

// The slice can be the slots of one byte buffer, with indexpool handing out the slot numbers. The pool knows nothing
// about the buffer: slot index is backing[index*slotSize : (index+1)*slotSize], which the caller computes.
//
// A producer frames messages straight into acquired slots and hands only the index to a set of workers. The workers
// recover the bytes from the index, process them, and release the index. With 16 indexes, at most 16 messages are in
// flight: once every index is out, Acquire blocks the producer until a worker releases one, which is backpressure for
// free. Nothing is allocated per message and nothing is copied between producer and worker.
func Example_byteSlots() {
	const (
		slotSize    = 64 << 10 // 64 KiB
		slotCount   = 16       // so 1 MiB of backing
		lengthBytes = 4        // each slot holds a little-endian length, then the payload
	)

	backing := make([]byte, slotCount*slotSize)
	slot := func(index int) []byte {
		start := index * slotSize
		return backing[start : start+slotSize : start+slotSize] // capped, so append can't spill into the next slot
	}

	pool, err := indexpool.New(slotCount)
	if err != nil {
		_, _ = fmt.Println(err)
		return
	}

	indexes := make(chan int, slotCount) // only indexes cross goroutines; the bytes stay in the backing buffer

	var messages, payloadBytes atomic.Int64
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range indexes {
				bytes := slot(index)
				length := binary.LittleEndian.Uint32(bytes)
				payload := bytes[lengthBytes : lengthBytes+length]

				_ = messages.Add(1)
				_ = payloadBytes.Add(int64(len(payload)))

				// After this the worker must not touch bytes or payload: another goroutine may already be writing
				// into them.
				if releaseErr := pool.Release(index); releaseErr != nil {
					_, _ = fmt.Println(releaseErr)
				}
			}
		}()
	}

	for message := range 100 {
		index := pool.Acquire() // blocks while all 16 indexes are in flight
		bytes := slot(index)
		length := copy(bytes[lengthBytes:], fmt.Sprintf("message %d", message))
		binary.LittleEndian.PutUint32(bytes, uint32(length))
		indexes <- index
	}
	close(indexes)
	workers.Wait()

	free := 0
	for {
		if pool.TryAcquire() < 0 {
			break
		}
		free++
	}
	_, _ = fmt.Printf("%d messages, %d payload bytes, %d of %d indexes free\n", messages.Load(), payloadBytes.Load(), free, slotCount)
	// Output: 100 messages, 990 payload bytes, 16 of 16 indexes free
}

// TryAcquire sheds load instead of waiting: a request that finds every index in use gets -1 at once.
func ExamplePool_TryAcquire() {
	pool, _ := indexpool.New(2)

	first := pool.TryAcquire()
	second := pool.TryAcquire()
	if pool.TryAcquire() < 0 {
		_, _ = fmt.Println("third request rejected: all indexes in use")
	}

	_ = pool.Release(first)
	if third := pool.TryAcquire(); third >= 0 {
		_, _ = fmt.Println("after one release, a request gets an index")
		_ = pool.Release(third)
	}
	_ = pool.Release(second)
	// Output:
	// third request rejected: all indexes in use
	// after one release, a request gets an index
}
