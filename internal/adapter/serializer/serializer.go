// Package serializer contains the default [domain.Serializer] implementation.
package serializer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Serializer implements domain.Serializer.
type Serializer struct {
	comparer        domain.Comparer
	documentFactory func(any) (domain.Document, error)
}

// NewSerializer returns a new implementation of domain.Serializer.
func NewSerializer(comparer domain.Comparer, documentFactory func(any) (domain.Document, error)) domain.Serializer {
	return &Serializer{
		comparer:        comparer,
		documentFactory: documentFactory,
	}
}

func (s *Serializer) copyDoc(doc domain.Document) (domain.Document, error) {
	res, err := s.documentFactory(nil)
	if err != nil {
		return nil, err
	}

	for k, v := range doc.Iter() {
		copied, err := s.copyAny(v)
		if err != nil {
			return nil, err
		}
		res.Set(k, copied)
	}
	return res, nil
}

func (s *Serializer) copyAny(v any) (any, error) {
	switch t := v.(type) {
	case domain.Document:
		return s.copyDoc(t)
	case []any:
		newList := make([]any, len(t))
		for n, itm := range t {
			newV, err := s.copyAny(itm)
			if err != nil {
				return nil, err
			}
			newList[n] = newV
		}
		return newList, nil
	case time.Time:
		return s.documentFactory(map[string]int64{"$$date": t.UnixMilli()})
	default:
		return v, nil
	}
}

// Serialize implements domain.Serializer.
func (s *Serializer) Serialize(ctx context.Context, obj any) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if doc, ok := obj.(domain.Document); ok {
		cp, err := s.copyDoc(doc)
		if err != nil {
			return nil, err
		}
		for k, v := range cp.Iter() {
			if err := s.checkKey(k, v); err != nil {
				return nil, err
			}
		}
		obj = cp
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
