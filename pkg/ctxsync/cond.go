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
	sem atomic.Pointer[chan struct{}]
	L   sync.Locker
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
	c.WaitWithContext(context.Background())
}

// WaitWithContext blocks until awoken by Signal, Broadcast, or context
// cancellation. Should be used in a loop that checks the condition.
func (c *Cond) WaitWithContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-*c.sem.Load():
		return nil
	}
}

// Signal wakes one waiting goroutine, if any.
// Does not guarantee ordering or priority.
func (c *Cond) Signal() {
	c.L.Lock()
	defer c.L.Unlock()

	select {
	case *c.sem.Load() <- struct{}{}:
	default:
	}
}

// Broadcast wakes all waiting goroutines, if any.
func (c *Cond) Broadcast() {
	c.L.Lock()
	defer c.L.Unlock()

	newSem := make(chan struct{})
	close(*c.sem.Load())
	c.sem.Store(&newSem)
}
