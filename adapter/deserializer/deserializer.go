// Package deserializer contains the default [domain.Deserializer]
// implementation.
package deserializer

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// NewDeserializer returns a new instance of domain.Deserializer.
func NewDeserializer(decoder domain.Decoder) domain.Deserializer {
	return &Deserializer{
		decoder: decoder,
	}
}

// Deserializer implements [domain.Deserializer].
type Deserializer struct {
	decoder domain.Decoder
}

// Deserialize implements [domain.Deserializer].
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

	if p, ok := target.(*map[string]any); ok {
		*p = doc
		return nil
	}

	return d.decoder.Decode(doc, target)
}
