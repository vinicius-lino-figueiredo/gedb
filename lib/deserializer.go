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

func (d Deserializer) asMap(doc gedb.Document) map[string]any {
	res := make(map[string]any)
	for k, v := range doc.Iter() {
		v2, ok := v.(gedb.Document)
		if ok {
			v = d.asMap(v2)
		}
		res[k] = v
	}
	return res
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
		if d, ok := v.(gedb.Document); ok && d.Get("$$date") != nil {
			doc[k] = d.Get("date")
		}
	}
	if p, ok := v.(*Document); ok {
		*p = doc
		return nil
	}

	m := d.asMap(doc)

	return d.decoder.Decode(m, v)
}
