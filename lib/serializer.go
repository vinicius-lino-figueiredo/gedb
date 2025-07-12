package lib

import (
	"context"
	"encoding/json"

	"github.com/vinicius-lino-figueiredo/gedb"
)

type Serializer struct{}

func NewSerializer() gedb.Serializer {
	return Serializer{}
}

func (s Serializer) Serialize(ctx context.Context, obj any) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if doc, ok := obj.(gedb.Document); ok {
		for k, v := range doc.Iter() {
			if err := checkKey(k, v); err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(obj)
}
