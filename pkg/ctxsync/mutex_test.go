package ctxsync_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

// Multiple goroutines should not be able to acquire the same lock.
func TestLock(t *testing.T) {
	workers := 1000

	n := 0
	mu := ctxsync.NewMutex()

	getReady := sync.WaitGroup{} // called before locking on ch
	add := sync.WaitGroup{}      // called after adding 1 to n

	getReady.Add(workers)
	add.Add(workers)

	ch := make(chan struct{})

	for range workers {
		go func() {
			defer add.Done()
			getReady.Done()
			<-ch // released afer all goroutines are locked here
			mu.Lock()
			defer mu.Unlock()
			n++
		}()
	}

	getReady.Wait()

	time.Sleep(time.Millisecond) // some time for them to get stuck at <-ch
	close(ch)                    // unlock so they all call mu.Lock at once

	add.Wait()

	assert.Equal(t, workers, n)
}

// Goroutines should acquire lock in the same order that they called Lock() .
func TestOrder(t *testing.T) {
	workers := 1000

	n := make([]int, 0, workers)
	mu := ctxsync.NewMutex()
	wg := sync.WaitGroup{}

	mu.Lock()

	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			n = append(n, i)
		}()

		// Just to make sure that the next goroutine won't call Lock
		// before this one
		time.Sleep(time.Millisecond)
	}
	mu.Unlock()
	wg.Wait()
	assert.Len(t, n, workers)
	assert.True(t, slices.IsSorted(n))
}

// Calling LockWithContext with a valid context should not return any errors
func TestContext(t *testing.T) {
	workers := 1000

	var errs []error
	mu := ctxsync.NewMutex()
	wg := sync.WaitGroup{}

	ctx := context.Background()

	mu.Lock()

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := mu.LockWithContext(ctx)

			errs = append(errs, err)

			if err != nil {
				return
			}
			defer mu.Unlock()
		}()

	}

	assert.Len(t, errs, 0)

	// Without this sleep sync.Mutex.Lock would occur before our mutex
	// realize the context have been canceled unless we wait a bit before
	// unlocking.
	time.Sleep(time.Millisecond)

	mu.Unlock()
	wg.Wait()
	assert.Len(t, errs, workers)
	for _, e := range errs {
		assert.NoError(t, e)
	}

}

// Should return error when context is canceled after Lock is called.
func TestCanceling(t *testing.T) {
	const workers = 1000

	var n int

	var errs []error

	mu := ctxsync.NewMutex()
	listMu := sync.Mutex{}

	getReady := sync.WaitGroup{}
	added := sync.WaitGroup{}
	getReady.Add(workers)
	added.Add(workers)

	ctx, cancel := context.WithCancel(context.Background())

	mu.Lock()

	ch := make(chan struct{})

	for range workers {
		go func() {
			defer added.Done()

			getReady.Done()

			<-ch

			err := mu.LockWithContext(ctx)

			listMu.Lock()
			errs = append(errs, err)
			listMu.Unlock()

			if err != nil {
				return
			}

			n++
			mu.Unlock()

		}()

	}

	getReady.Wait()

	time.Sleep(time.Millisecond)

	close(ch)

	time.Sleep(time.Millisecond)

	cancel()

	time.Sleep(time.Millisecond)

	defer cancel()

	added.Wait()
	assert.Len(t, errs, workers)
	assert.Equal(t, 0, n)
	for _, e := range errs {
		assert.Error(t, e)
	}

}

// A Lock called with a canceled context should not affect other Lock calls.
func TestIndependentCancelling(t *testing.T) {
	workers := 1000

	errs := make([]error, workers)
	mu := ctxsync.NewMutex()
	wg := sync.WaitGroup{}

	ctx1 := context.Background()

	mu.Lock()

	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := ctx1
			if i%2 == 0 {
				c, cancel := context.WithCancel(ctx1)
				cancel()
				ctx = c
			}
			if err := mu.LockWithContext(ctx); err != nil {
				errs[i] = err
				return
			}
			defer mu.Unlock()
		}()
	}

	mu.Unlock()
	wg.Wait()
	errCount := 0
	okCount := 0
	for _, err := range errs {
		if err != nil {
			errCount++
		} else {
			okCount++
		}
	}
	assert.Equal(t, workers/2, errCount)
	assert.Equal(t, workers/2, okCount)

}

// Should not wait for lock if passed context is already canceled
func TestCanceledContext(t *testing.T) {
	workers := 1000

	errs := make([]error, workers)
	mu := ctxsync.NewMutex()
	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mu.Lock()

	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mu.LockWithContext(ctx); err != nil {
				errs[i] = err
			}
		}()

	}

	// No need to unlock, they should not even try locking
	wg.Wait()

	for _, e := range errs {
		assert.Error(t, e)
	}

}

// Should panic if Unlock is called before Lock.
func TestUnlockWithoutLock(t *testing.T) {
	mu := ctxsync.NewMutex()
	assert.Panics(t, func() {
		mu.Unlock()
	})
}

// Should not lock locked context.
func TestTryLockLocked(t *testing.T) {

	mu := ctxsync.NewMutex()

	mu.Lock()

	locked := mu.TryLock()

	assert.False(t, locked)

	var canLock atomic.Bool
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		mu.Lock()
		canLock.Store(true)
		group.Done()
	}()

	mu.Unlock()

	group.Wait()
	assert.True(t, canLock.Load())
}

// Should lock unlocked context.
func TestTryLockUnlocked(t *testing.T) {

	mu := ctxsync.NewMutex()

	mu.Lock()
	mu.Unlock()

	locked := mu.TryLock()

	assert.True(t, locked)

	var canLock atomic.Bool
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		mu.Lock()
		canLock.Store(true)
		group.Done()
	}()

	mu.Unlock()

	group.Wait()
	assert.True(t, canLock.Load())
}

// Should panic if Unlock is called twice without another Lock.
func TestDoubleUnlock(t *testing.T) {
	mu := ctxsync.NewMutex()

	ctx := context.Background()
	mu.LockWithContext(ctx)
	mu.Unlock()

	assert.Panics(t, func() {
		mu.Unlock()
	})
}

// BenchmarkLockUnlock tests performance for consecutive Lock/Unlock calls.
func BenchmarkLockUnlock(b *testing.B) {
	mu := ctxsync.NewMutex()
	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.LockWithContext(ctx)
			mu.Unlock()
		}
	})
}

// BenchmarkTimeoutLock tests timeout cancellation performance.
func BenchmarkTimeoutLock(b *testing.B) {
	mu := ctxsync.NewMutex()
	mu.LockWithContext(context.Background()) // Hold the lock

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			mu.LockWithContext(ctx)
			cancel()
		}
	})
}

// BenchmarkLockUnlock tests performance for Lock with canceled context.
func BenchmarkLockCanceledContext(b *testing.B) {
	mu := ctxsync.NewMutex()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.LockWithContext(ctx)
		}
	})
}
