// Package cursor contains the default [domain.Cursor] implementation.
package cursor

import (
	"context"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Cursor implements domain.Cursor.
type Cursor struct {
	data   []domain.Document
	ctx    context.Context
	cancel context.CancelCauseFunc
	dec    domain.Decoder
	index  int64
}

// NewCursor returns a new implementation of Cursor.
func NewCursor(ctx context.Context, dt []domain.Document, options ...domain.CursorOption) (domain.Cursor, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	opts := domain.CursorOptions{
		Decoder: decoder.NewDecoder(),
	}

	for _, option := range options {
		option(&opts)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	cur := &Cursor{
		ctx:    ctx,
		cancel: cancel,
		index:  -1,
		dec:    opts.Decoder,
		data:   dt,
	}

	return cur, nil
}

// Err implements domain.Cursor.
func (c *Cursor) Err() error {
	return context.Cause(c.ctx)
}

// Scan implements domain.Cursor.
func (c *Cursor) Scan(ctx context.Context, target any) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if c.index < 0 {
		return domain.ErrScanBeforeNext
	}
	return c.dec.Decode(c.data[c.index], target)
}

// Close implements domain.Cursor.
func (c *Cursor) Close() error {
	select {
	case <-c.ctx.Done():
		return context.Cause(c.ctx)
	default:
	}
	c.cancel(domain.ErrCursorClosed)
	c.data = nil
	return nil
}

// Next implements domain.Cursor.
func (c *Cursor) Next() bool {
	select {
	case <-c.ctx.Done():
		return false
	default:
	}
	if c.index+1 < int64(len(c.data)) {
		c.index++
		return true
	}
	return false
}
