package ctxsync

import (
	"context"
)

// NewMutex creates a new instance of Mutex.
func NewMutex() *Mutex {
	return &Mutex{
		unlock: make(chan struct{}),
	}
}

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	unlock chan struct{}
}

// Lock locks the mutex with a context.Background()
func (m *Mutex) Lock() {
	_ = m.LockWithContext(context.Background())
}

// LockWithContext locks until Unlock is called or context is cancelled
func (m *Mutex) LockWithContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.unlock <- struct{}{}:
		return nil
	}
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *Mutex) TryLock() bool {
	select {
	case m.unlock <- struct{}{}:
		return true
	default:
		return false
	}
}

// Unlock unlocks m.
func (m *Mutex) Unlock() {
	select {
	case <-m.unlock:
	default:
		panic("ctxsync: unlock of unlocked mutex")
	}
}
