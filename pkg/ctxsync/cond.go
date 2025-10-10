package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
)

// Cond implements a condition variable, a rendezvous point for goroutines
// waiting for or announcing the occurrence of an event.
//
// A Cond must not be copied after first use.
type Cond struct {
	sem     atomic.Pointer[chan struct{}]
	L       sync.Locker
	waiters int64
}

// NewCond returns a new Cond.
func NewCond() *Cond {
	ch := make(chan struct{})
	c := &Cond{
		L: &sync.Mutex{},
	}
	c.sem.Store(&ch)
	return c
}

// Wait blocks until awoken by Signal or Broadcast. It is equivalent to
// WaitWithContext(context.Background()).
func (c *Cond) Wait() {
	_ = c.WaitWithContext(context.Background())
}

// WaitWithContext blocks until awoken by Signal, Broadcast, or context
// cancellation. Should be used in a loop that checks the condition.
func (c *Cond) WaitWithContext(ctx context.Context) error {

	sem := *c.sem.Load()
	done := ctx.Done()

	atomic.AddInt64(&c.waiters, 1)
	defer atomic.AddInt64(&c.waiters, -1)

	select {
	case <-done:
		return ctx.Err()
	case <-sem:
		return nil
	}
}

// WaiterCount returns the amount of goroutines that are locked waiting for a
// broadcast. Since there is a small time window between adding the waiter count
// and locking at the select, it might return >0 before the waiter is actually
// locked in the select, but it should generally return a valid value.
func (c *Cond) WaiterCount() int64 {
	return atomic.LoadInt64(&c.waiters)
}

// Broadcast wakes all waiting goroutines, if any.
func (c *Cond) Broadcast() {
	c.L.Lock()
	defer c.L.Unlock()

	newSem := make(chan struct{})
	close(*c.sem.Load())
	c.sem.Store(&newSem)
}
