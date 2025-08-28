package ctxsync

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestCondBroadCast(t *testing.T) {
	c := NewCond()
	var n int64 = 2
	awake := make(chan bool, n)

	for range n {
		go func() {
			c.Wait()
			awake <- true
		}()
	}

	for c.WaiterCount() != n {
		runtime.Gosched()
	}

	time.Sleep(time.Microsecond)

	select {
	case <-awake:
		t.Fatal("goroutine not asleep")
	default:
	}

	c.Broadcast()

	time.Sleep(time.Microsecond)

	for range n {
		<-awake
	}
}

func TestCancelling(t *testing.T) {
	c := NewCond()
	var n int64 = 2
	errs := make(chan error, n)

	ctx, cancel := context.WithCancel(context.Background())

	for range n {
		go func() {
			errs <- c.WaitWithContext(ctx)
		}()
	}
	for c.WaiterCount() != n {
		runtime.Gosched()
	}

	select {
	case <-errs:
		t.Fatal("goroutine not asleep")
	default:
	}

	cancel()
	for c.WaiterCount() != 0 {
		runtime.Gosched()
	}
	// it takes a moment for broadcast to reach waiters
	time.Sleep(time.Microsecond)

	for range n {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("context not canceled")
			}
		default:
			t.Fatal("goroutine asleep")
		}
	}
	c.Broadcast()
}

func BenchmarkCond1(b *testing.B) {
	benchmarkCond(b, 1)
}

func BenchmarkCond2(b *testing.B) {
	benchmarkCond(b, 2)
}

func BenchmarkCond4(b *testing.B) {
	benchmarkCond(b, 4)
}

func BenchmarkCond8(b *testing.B) {
	benchmarkCond(b, 8)
}

func BenchmarkCond16(b *testing.B) {
	benchmarkCond(b, 16)
}

func BenchmarkCond32(b *testing.B) {
	benchmarkCond(b, 32)
}

func benchmarkCond(b *testing.B, waiters int64) {
	c := NewCond()

	done := make(chan struct{}, waiters)
	for b.Loop() {
		for range waiters {
			go func() {
				c.Wait()
				done <- struct{}{}
			}()
		}

		// wait until waiter is locked
		for c.WaiterCount() != waiters {
			runtime.Gosched()
		}

		c.Broadcast()
		for range waiters {
			<-done
		}
	}

}
