package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Serializer implements gedb.Serializer.
type Serializer struct {
	comparer gedb.Comparer
}

// NewSerializer returns a new implementation of gedb.Serializer.
func NewSerializer(comparer gedb.Comparer) gedb.Serializer {
	return &Serializer{
		comparer: comparer,
	}
}

// Serialize implements gedb.Serializer.
func (s *Serializer) Serialize(ctx context.Context, obj any) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if doc, ok := obj.(gedb.Document); ok {
		for k, v := range doc.Iter() {
			if err := s.checkKey(k, v); err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(obj)
}

func (s *Serializer) checkKey(k string, v any) error {
	if strings.ContainsRune(k, '.') {
		return errors.New("field names cannot contain a '.'")
	}
	if !strings.HasPrefix(k, "$") {
		return nil
	}

	foundErr := true
	switch k {
	case "$$date":
		foundErr = !s.isNumber(v)
	case "$$deleted":
		foundErr = !s.isTrue(v)
	case "$$indexCreated", "$$indexRemoved":
		foundErr = false
	default:
	}
	if foundErr {
		return fmt.Errorf("field names cannot start with the $ character")
	}
	return nil
}

func (s *Serializer) isNumber(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case
		uint, uint8, uint16, uint32, uint64,
		int, int8, int16, int32, int64,
		float32, float64:
		return true
	default:
		return false
	}
}

func (s *Serializer) isTrue(v any) bool {
	if v == nil {
		return false
	}
	if vt, ok := v.(bool); ok {
		return vt
	}
	return false
}
