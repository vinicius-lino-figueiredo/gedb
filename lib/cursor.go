package lib

import (
	"context"
	"fmt"
	"sync"

	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

// Cursor implements gedb.Cursor.
type Cursor struct {
	data      []any
	sort      any
	ctx       context.Context
	mu        *ctxsync.Mutex
	dec       gedb.Decoder
	once      sync.Once
	started   bool
	closed    bool
	storedErr error
}

// NewCursor returns a new implementation of Cursor
func NewCursor(ctx context.Context, data []gedb.Document, options gedb.CursorOptions) (gedb.Cursor, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if options.Matcher == nil {
		options.Matcher = NewMatcher()
	}
	if options.Decoder == nil {
		options.Decoder = NewDecoder()
	}

	if len(data) == 0 || int64(len(data)) < options.Skip {
		return &Cursor{ctx: ctx, mu: ctxsync.NewMutex()}, nil
	}

	matches := make([]any, 0, len(data))

	var skipped int64

	for _, doc := range data {
		if skipped < options.Skip {
			skipped++
			continue
		}
		doesMatch := true
		var err error
		if options.Query != nil {
			doesMatch, err = options.Matcher.Match(doc, options.Query)
			if err != nil {
				return nil, err
			}
		}
		if doesMatch {
			matches = append(matches, doc)
			if int64(len(matches)) == options.Limit {
				break
			}
		}
	}

	cur := &Cursor{
		ctx:  ctx,
		data: matches,
		mu:   ctxsync.NewMutex(),
		dec:  options.Decoder,
	}

	return cur, nil
}

// Err implements gedb.Cursor.
func (c *Cursor) Err() error {
	return c.storedErr
}

// Exec implements gedb.Cursor.
func (c *Cursor) Exec(ctx context.Context, target any) error {
	innerCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case <-ctx.Done():
			cancel(context.Cause(ctx))
		case <-c.ctx.Done():
			cancel(context.Cause(innerCtx))
		case <-innerCtx.Done():
		}
	}()
	if err := c.mu.LockWithContext(innerCtx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.storedErr != nil {
		return c.storedErr
	}
	if !c.started {
		return fmt.Errorf("called Exec before calling Next")
	}
	if len(c.data) == 0 {
		return fmt.Errorf("called Exec on empty Cursor")
	}
	data := c.data[0]
	return c.dec.Decode(data, target)
}

// Close implements gedb.Cursor.
func (c *Cursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) > 0 {
		c.storedErr = fmt.Errorf("cursor is closed")
	}
	c.data = nil
	return nil
}

// Next implements gedb.Cursor.
func (c *Cursor) Next() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) == 0 {
		return false
	}
	if c.started {
		c.data = c.data[1:]
	}
	c.started = true
	return len(c.data) > 0
}
