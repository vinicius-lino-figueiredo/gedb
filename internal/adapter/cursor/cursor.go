// Package cursor contains the default [domain.Cursor] implementation.
package cursor

import (
	"context"
	"fmt"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/decoder"
)

// Cursor implements domain.Cursor.
type Cursor struct {
	data           []domain.Document
	ctx            context.Context
	cancel         context.CancelCauseFunc
	dec            domain.Decoder
	index          int64
	fieldNavigator domain.FieldNavigator
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
	return c.ctx.Err()
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
		return fmt.Errorf("called Scan before calling Next")
	}
	return c.dec.Decode(c.data[c.index], target)
}

// Close implements domain.Cursor.
func (c *Cursor) Close() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	c.cancel(fmt.Errorf("cursor is closed"))
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
