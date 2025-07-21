package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// NewDeserializer returns a new instance of gedb.Deserializer.
func NewDeserializer(decoder gedb.Decoder) gedb.Deserializer {
	return &Deserializer{
		decoder: decoder,
	}
}

// Deserializer implements gedb.Deserializer.
type Deserializer struct {
	decoder gedb.Decoder
}

func (d *Deserializer) convertDates(doc Document) any {
	for k, v := range doc {
		if k == "$$date" {
			if i, ok := v.(float64); ok {
				return time.UnixMilli(int64(i))
			}
		}
		if vDoc, ok := v.(Document); ok {
			doc[k] = d.convertDates(vDoc)
		}
	}
	return doc
}

// Deserialize implements gedb.Deserializer.
func (d *Deserializer) Deserialize(ctx context.Context, data []byte, v any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if v == nil {
		return errors.New("nil target")
	}
	// this should be the only use of Document outside of the context. Here
	// we could use a map[string]any, but Document is slightly faster when
	// unmarshaling json.
	doc := make(Document)

	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&doc); err != nil {
		return err
	}

	d.convertDates(doc)
	if p, ok := v.(*map[string]any); ok {
		*p = doc
		return nil
	}

	return d.decoder.Decode(doc, v)
}
