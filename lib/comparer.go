package lib

import (
	"cmp"
	"math/big"
	"slices"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Comparer implements gedb.Comparer.
type Comparer struct{}

// NewComparer returns a new implementation of gedb.Comparer.
func NewComparer() gedb.Comparer {
	return &Comparer{}
}

// Compare implements gedb.Comparer.
func (c *Comparer) Compare(a any, b any) (int, error) {

	// nil (undefined/null)
	if a == nil {
		if b == nil {
			return 0, nil
		}
		return -1, nil
	}
	if b == nil {
		return 1, nil // no need to test if a == nil
	}

	// Numbers
	if a, ok := asNumber(a); ok {
		// Using big.Float to safely compare float64 and int64 without
		// precision loss
		if b, ok := asNumber(b); ok {
			return a.Cmp(b), nil
		}
		return -1, nil
	}
	if _, ok := asNumber(b); ok {
		return 1, nil
	}

	// Strings
	if a, ok := a.(string); ok {
		if b, ok := b.(string); ok {
			return cmp.Compare(a, b), nil
		}
		return -1, nil
	}
	if _, ok := b.(string); ok {
		return 1, nil
	}

	// Booleans
	if a, ok := a.(bool); ok {
		if b, ok := b.(bool); ok {
			return c.compareBool(a, b), nil
		}
		return -1, nil
	}
	if _, ok := b.(bool); ok {
		return 1, nil
	}

	// Arrays
	if a, ok := a.([]any); ok {
		if b, ok := b.([]any); ok {
			return c.compareArray(a, b)
		}
		return -1, nil
	}
	if _, ok := b.(bool); ok {
		return 1, nil
	}

	// Objects
	da := a.(gedb.Document)
	db := b.(gedb.Document)

	aKeys := slices.Collect(da.Keys())
	bKeys := slices.Collect(db.Keys())
	slices.Sort(aKeys)
	slices.Sort(bKeys)

	var comp int
	var err error
	for i := range min(len(aKeys), len(bKeys)) {
		comp, err = c.Compare(da.Get(aKeys[i]), db.Get(bKeys[i]))
		if err != nil {
			return 0, err
		}

		if comp != 0 {
			return comp, nil
		}
	}

	return cmp.Compare(da.Len(), db.Len()), nil
}

func (c *Comparer) compareArray(a, b []any) (int, error) {
	minLength := min(len(a), len(b))

	var comp int
	var err error
	for i := range minLength {
		comp, err = c.Compare(a[i], b[i])
		if err != nil {
			return 0, err
		}

		if comp != 0 {
			return comp, nil
		}
	}

	// Common section was identical, longest one wins
	return cmp.Compare(len(a), len(b)), nil
}

func (c *Comparer) compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return 1
	}
	return -1
}

func asNumber(v any) (*big.Float, bool) {
	r := big.NewFloat(0)
	switch n := v.(type) {
	case int:
		r.SetInt64(int64(n))
	case int8:
		r.SetInt64(int64(n))
	case int16:
		r.SetInt64(int64(n))
	case int32:
		r.SetInt64(int64(n))
	case int64:
		r.SetInt64(n)
	case uint:
		r.SetUint64(uint64(n))
	case uint8:
		r.SetUint64(uint64(n))
	case uint16:
		r.SetUint64(uint64(n))
	case uint32:
		r.SetUint64(uint64(n))
	case uint64:
		r.SetUint64(n)
	case float32:
		r.SetFloat64(float64(n))
	case float64:
		r.SetFloat64(n)
	default:
		return nil, false
	}
	return r, true
}
