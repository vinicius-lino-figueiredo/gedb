package lib

import (
	"context"
	"io"

	"github.com/vinicius-lino-figueiredo/nedb"
	"github.com/vinicius-lino-figueiredo/nedb/pkg/errs"
)

// Cursor implements nedb.Cursor.
type Cursor struct {
	data       []any
	index      int64
	limit      int64
	projection any
	sort       any
	skip       int64
	storedErr  error
	query      any
	mapFn      any
	ctx        context.Context
}

// NewCursor returns a new implementation of Cursor
func NewCursor(ctx context.Context, data []any, options cursorOptions) *Cursor {
	c := Cursor{
		ctx:   ctx,
		query: options.query,
		mapFn: options.mapFn,
		data:  data,
	}
	return &c
}

type cursorOptions struct {
	query      any
	mapFn      func(any) any
	limit      int64
	skip       int64
	sort       any
	projection any
}

// Exec implements nedb.Cursor.
func (c *Cursor) Exec(ctx context.Context, target any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if c.storedErr != nil {
		return c.storedErr
	}
	if target == nil {
		return &errs.ErrTargetNil{}
	}
	// TODO: Implement cursor deserialization
	panic("unimplemented")
}

// ID implements nedb.Cursor.
func (c *Cursor) ID() [16]byte {
	// TODO: Implement cursor ID getter
	panic("unimplemented")
}

// Limit implements nedb.Cursor.
func (c *Cursor) Limit(n int64) nedb.Cursor {
	c.limit = n
	return c
}

// Projection implements nedb.Cursor.
func (c *Cursor) Projection(query any) nedb.Cursor {
	c.projection = query
	return c
}

// Skip implements nedb.Cursor.
func (c *Cursor) Skip(n int64) nedb.Cursor {
	c.skip = n
	return c
}

// Sort implements nedb.Cursor.
func (c *Cursor) Sort(query any) nedb.Cursor {
	c.sort = query
	return c
}

// Close implements nedb.Cursor.
func (c *Cursor) Close() {
	// TODO: Implement cursor Close function
	panic("unimplemented")
}

// Next implements nedb.Cursor.
func (c *Cursor) Next(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		c.storedErr = ctx.Err()
		return false
	default:
	}
	c.index++
	if c.index >= int64(len(c.data)) {
		c.storedErr = io.EOF
		return false
	}
	return true
}

var _ nedb.Cursor = (*Cursor)(nil)
