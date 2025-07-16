package lib

import (
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

// Deserialize implements gedb.Deserializer.
func (d Deserializer) Deserialize(ctx context.Context, data []byte, v any) error {
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
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	for k, v := range doc {
		if k == "$$date" {
			if i, ok := v.(int64); ok {
				doc[k] = time.Unix(i, 0)
			}
		}
		if d, ok := v.(map[string]any); ok && d["$$date"] != nil {
			doc[k] = d["date"]
		}
	}
	if p, ok := v.(*map[string]any); ok {
		*p = doc
		return nil
	}

	return d.decoder.Decode(doc, v)
}
