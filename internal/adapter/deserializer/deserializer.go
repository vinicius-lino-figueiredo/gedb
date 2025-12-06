// Package deserializer contains the default [domain.Deserializer]
// implementation.
package deserializer

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
)

// NewDeserializer returns a new instance of domain.Deserializer.
func NewDeserializer(decoder domain.Decoder) domain.Deserializer {
	return &Deserializer{
		decoder: decoder,
	}
}

// Deserializer implements domain.Deserializer.
type Deserializer struct {
	decoder domain.Decoder
}

func (d *Deserializer) convertDates(doc data.M) any {
	for k, v := range doc {
		if k == "$$date" {
			if i, ok := v.(float64); ok {
				return time.UnixMilli(int64(i))
			}
		}
		doc[k] = d.convertAny(v)
	}
	return doc
}

func (d *Deserializer) convertAny(v any) any {
	switch t := v.(type) {
	case data.M:
		return d.convertDates(t)
	case []any:
		newL := make([]any, len(t))
		for n, i := range t {
			newL[n] = d.convertAny(i)
		}
		return newL
	default:
		return v
	}
}

// Deserialize implements domain.Deserializer.
func (d *Deserializer) Deserialize(ctx context.Context, b []byte, target any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if target == nil {
		return domain.ErrTargetNil
	}
	// this should be the only use of Document outside of the context. Here
	// we could use a map[string]any, but Document is slightly faster when
	// unmarshaling json.
	doc := make(data.M)

	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&doc); err != nil {
		return err
	}

	d.convertDates(doc)
	if p, ok := target.(*map[string]any); ok {
		*p = doc
		return nil
	}

	return d.decoder.Decode(doc, target)
}
