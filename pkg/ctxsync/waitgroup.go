package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
)

// NewWaitGroup creates a new WaitGroup instance with the semaphore channel
// initialized. Use this to get a ready-to-use WaitGroup.
func NewWaitGroup() *WaitGroup {
	return &WaitGroup{sema: make(chan struct{})}
}

// A WaitGroup waits for a collection of goroutines to finish.
// The main goroutine calls [WaitGroup.Add] to set the number of
// goroutines to wait for. Then each of the goroutines
// runs and calls [WaitGroup.Done] when finished. At the same time,
// [WaitGroup.Wait] can be used to block until all goroutines have finished.
type WaitGroup struct {
	state atomic.Uint64 // high 32 bits are counter, low 32 bits are waiter count.
	sema  chan struct{}
	m     sync.Mutex
}

// Add adds delta, which may be negative, to the [WaitGroup] counter.
// If the counter becomes zero, all goroutines blocked on [WaitGroup.Wait] are released.
// If the counter goes negative, Add panics.
//
// Note that calls with a positive delta that occur when the counter is zero
// must happen before a Wait. Calls with a negative delta, or calls with a
// positive delta that start when the counter is greater than zero, may happen
// at any time.
// Typically this means the calls to Add should execute before the statement
// creating the goroutine or other event to be waited for.
// If a WaitGroup is reused to wait for several independent sets of events,
// new Add calls must happen after all previous Wait calls have returned.
// See the WaitGroup example.
func (wg *WaitGroup) Add(delta int) {
	state := wg.state.Add(uint64(delta) << 32)
	v := int32(state >> 32)
	w := uint32(state)
	if v < 0 {
		panic("sync: negative WaitGroup counter")
	}
	if w != 0 && delta > 0 && v == int32(delta) {
		panic("sync: WaitGroup misuse: Add called concurrently with Wait")
	}
	if v > 0 || w == 0 {
		return
	}
	// This goroutine has set counter to 0 when waiters > 0.
	// Now there can't be concurrent mutations of state:
	// - Adds must not happen concurrently with Wait,
	// - Wait does not increment waiters if it sees counter == 0.
	// Still do a cheap sanity check to detect WaitGroup misuse.
	if wg.state.Load() != state {
		panic("sync: WaitGroup misuse: Add called concurrently with Wait")
	}
	// Reset waiters count to 0.
	wg.state.Store(0)
	wg.m.Lock()
	close(wg.sema)
	wg.sema = make(chan struct{})
	wg.m.Unlock()
}

// Done decrements the [WaitGroup] counter by one.
func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

// Wait blocks until the WaitGroup counter is zero. Equivalent to calling
// WaitWithContext with a background context.
func (wg *WaitGroup) Wait() {
	_ = wg.WaitWithContext(context.Background())
}

// WaitWithContext blocks until the WaitGroup counter is zero or the context is
// done. Returns an error if the context is canceled or times out before the
// wait completes.
func (wg *WaitGroup) WaitWithContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	wg.m.Lock()
	var err error
L:
	for {
		state := wg.state.Load()
		v := int32(state >> 32)
		if v == 0 {
			// Counter is 0, no need to wait.
			break
		}
		// Increment waiters count.
		if wg.state.CompareAndSwap(state, state+1) {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break L
			case <-wg.sema:
			}
			if wg.state.Load() != 0 {
				wg.m.Unlock()
				panic("sync: WaitGroup is reused before previous Wait has returned")
			}
			break
		}
	}
	wg.m.Unlock()
	return err
}
