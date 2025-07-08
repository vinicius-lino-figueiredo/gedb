package lib

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Executor implements gedb.Executor.
type Executor struct {
	mux        sync.Mutex
	ready      atomic.Bool
	bufferExec chan struct{}
	singleExec chan struct{}
	cancelExec chan struct{}
}

// NewExecutor creates a new instance of gedb.Executor.
func NewExecutor() gedb.Executor {
	e := Executor{
		bufferExec: make(chan struct{}, 1),
		singleExec: make(chan struct{}),
		cancelExec: make(chan struct{}),
	}
	e.bufferExec <- struct{}{}
	return &e
}

// Bufferize implements gedb.Executor.
func (e *Executor) Bufferize() {
	e.ready.Store(false)
}

// Push implements gedb.Executor.
func (e *Executor) Push(ctx context.Context, task func(context.Context), forceQueuing bool) error {
	var execCh chan struct{}
	if forceQueuing || e.ready.Load() {
		execCh = e.singleExec
	} else {
		ctx = context.WithoutCancel(ctx)
		execCh = e.bufferExec
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-e.getCancelExec():
		return errors.New("buffer was reset")

	case execCh <- struct{}{}:
		defer func() { <-execCh }()
	}
	task(ctx)
	return nil
}

// GoPush implements gedb.Executor.
func (e *Executor) GoPush(ctx context.Context, task func(context.Context), forceQueuing bool) error {
	task = func(ctx context.Context) {
		go task(ctx)
	}
	return e.Push(ctx, task, forceQueuing)
}

// ProcessBuffer implements gedb.Executor.
func (e *Executor) ProcessBuffer() {
	<-e.bufferExec
	e.ready.Store(true)
}

// ResetBuffer implements gedb.Executor.
func (e *Executor) ResetBuffer() {
	e.mux.Lock()
	defer e.mux.Unlock()
	close(e.cancelExec)
	e.cancelExec = make(chan struct{})
}

// getCancelExec returns a valid instance of a cancellation channel. It has a
// lock to avoid returning a closed channel that has not been replaced.
func (e *Executor) getCancelExec() <-chan struct{} {
	e.mux.Lock()
	defer e.mux.Unlock()
	return e.cancelExec
}
