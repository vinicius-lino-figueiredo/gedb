package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Cond implements a condition variable, a rendezvous point
// for goroutines waiting for or announcing the occurrence
// of an event.
//
// Each Cond has an associated Locker L (often a [*Mutex] or [*RWMutex]),
// which must be held when changing the condition and
// when calling the [Cond.WaitWithContext] method.
//
// A Cond must not be copied after first use.
//
// In the terminology of [the Go memory model], Cond arranges that
// a call to [Cond.Broadcast] or [Cond.Signal] “synchronizes before” any Wait call
// that it unblocks.
//
// For many simple use cases, users will be better off using channels than a
// Cond (Broadcast corresponds to closing a channel, and Signal corresponds to
// sending on a channel).
//
// For more on replacements for [sync.Cond], see [Roberto Clapis's series on
// advanced concurrency patterns], as well as [Bryan Mills's talk on concurrency
// patterns].
//
// [the Go memory model]: https://go.dev/ref/mem
// [Roberto Clapis's series on advanced concurrency patterns]: https://blogtitle.github.io/categories/concurrency/
// [Bryan Mills's talk on concurrency patterns]: https://drive.google.com/file/d/1nPdvhB0PutEJzdCq5ms6UI58dp50fcAN/view
type Cond struct {
	noCopy noCopy

	// L is held while observing or changing the condition
	L sync.Locker

	notify  chan struct{}
	waiters atomic.Int64
	checker copyChecker

	chMtx sync.Mutex
}

// NewCond returns a new Cond with Locker l.
func NewCond(l sync.Locker) *Cond {
	return &Cond{L: l, notify: make(chan struct{}, 1)}
}

// Wait releases c.L and blocks until awoken by Signal or Broadcast.
// It is equivalent to WaitWithContext(context.Background()).
func (c *Cond) Wait() {
	c.WaitWithContext(context.Background())
}

// WaitWithContext releases c.L and blocks until awoken by Signal, Broadcast, or
// context cancellation. Reacquires c.L before returning. Should be used in a
// loop that checks the condition.
func (c *Cond) WaitWithContext(ctx context.Context) error {
	c.checker.check()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.waiters.Add(1)
	c.L.Unlock()

	c.chMtx.Lock()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-c.notify:
	}
	c.waiters.Add(-1)
	c.L.Lock()
	c.chMtx.Unlock()
	return err
}

// Signal wakes one waiting goroutine, if any.
// The caller does not need to hold c.L.
// Does not guarantee ordering or priority.
func (c *Cond) Signal() {
	c.checker.check()
	if c.waiters.Load() > 0 {
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}
}

// Broadcast wakes all waiting goroutines, if any.
// The caller does not need to hold c.L.
func (c *Cond) Broadcast() {
	c.checker.check()
	if c.waiters.Load() > 0 {
		c.chMtx.Lock()
		close(c.notify)
		c.notify = make(chan struct{})
		c.chMtx.Unlock()
	}
}

// copyChecker holds back pointer to itself to detect object copying.
type copyChecker uintptr

func (c *copyChecker) check() {
	// Check if c has been copied in three steps:
	// 1. The first comparison is the fast-path. If c has been initialized and not copied, this will return immediately. Otherwise, c is either not initialized, or has been copied.
	// 2. Ensure c is initialized. If the CAS succeeds, we're done. If it fails, c was either initialized concurrently and we simply lost the race, or c has been copied.
	// 3. Do step 1 again. Now that c is definitely initialized, if this fails, c was copied.
	if uintptr(*c) != uintptr(unsafe.Pointer(c)) &&
		!atomic.CompareAndSwapUintptr((*uintptr)(c), 0, uintptr(unsafe.Pointer(c))) &&
		uintptr(*c) != uintptr(unsafe.Pointer(c)) {
		panic("ctxsync.Cond is copied")
	}
}

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
