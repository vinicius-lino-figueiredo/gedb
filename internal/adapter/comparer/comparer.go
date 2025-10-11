package comparer

import (
	"cmp"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Comparer implements domain.Comparer.
type Comparer struct{}

// NewComparer returns a new implementation of domain.Comparer.
func NewComparer() domain.Comparer {
	return &Comparer{}
}

// Comparable implements domain.Comparer.
func (c *Comparer) Comparable(a, b any) bool {
	if !c.isSet(a) || !c.isSet(b) {
		return false
	}
	a, b = c.getVal(a), c.getVal(b)

	equal := false
	if _, ok := c.asNumber(a); ok {
		_, equal = c.asNumber(b)
		return equal
	}

	switch a.(type) {
	case string:
		_, equal = b.(string)
	case time.Time:
		_, equal = b.(time.Time)
	default:
		return false
	}
	return equal
}

// Compare implements domain.Comparer.
func (c *Comparer) Compare(a any, b any) (int, error) {

	// [domain.Getter]. Equivalent to js undefined
	if c, ok, err := c.checkUndefined(a, b); err != nil || ok {
		return c, err
	}

	a, b = c.getVal(a), c.getVal(b)

	// [nil] (null)
	if c, ok := c.checkNil(a, b); ok {
		return c, nil
	}

	// Numbers
	if c, ok := c.checkNumbers(a, b); ok {
		return c, nil
	}

	// Strings
	if c, ok := c.checkStrings(a, b); ok {
		return c, nil
	}

	// Booleans
	if c, ok := c.checkBooleans(a, b); ok {
		return c, nil
	}

	// Dates
	if c, ok := c.checkTime(a, b); ok {
		return c, nil
	}

	// Arrays
	if c, ok, err := c.checkArrays(a, b); err != nil || ok {
		return c, err
	}

	// Objects
	if c, ok, err := c.checkDocs(a, b); err != nil || ok {
		return c, err
	}

	return 0, fmt.Errorf("cannot compare unexpected types %T and %T", a, b)
}

func (c *Comparer) checkUndefined(a, b any) (int, bool, error) {
	// [domain.Getter]
	if !c.isSet(a) {
		if !c.isSet(b) {
			return 0, true, nil
		}
		return -1, true, nil
	}
	if !c.isSet(b) {
		return 1, true, nil
	}
	return 0, false, nil
}

func (c *Comparer) checkNil(a, b any) (int, bool) {
	if a == nil {
		if b == nil {
			return 0, true
		}
		return -1, true
	}
	if b == nil {
		return 1, true // no need to test if a == nil
	}
	return 0, false
}

func (c *Comparer) checkNumbers(a, b any) (int, bool) {
	if a, ok := c.asNumber(a); ok {
		// Using big.Float to safely compare float64 and int64 without
		// precision loss
		if b, ok := c.asNumber(b); ok {
			return a.Cmp(b), true
		}
		return -1, true
	}
	if _, ok := c.asNumber(b); ok {
		return 1, true
	}
	return 0, false
}

func (c *Comparer) checkStrings(a, b any) (int, bool) {
	if a, ok := a.(string); ok {
		if b, ok := b.(string); ok {
			return cmp.Compare(a, b), true
		}
		return -1, true
	}
	if _, ok := b.(string); ok {
		return 1, true
	}
	return 0, false
}

func (c *Comparer) checkBooleans(a, b any) (int, bool) {
	if a, ok := a.(bool); ok {
		if b, ok := b.(bool); ok {
			return c.compareBool(a, b), true
		}
		return -1, true
	}
	if _, ok := b.(bool); ok {
		return 1, true
	}
	return 0, false
}

func (c *Comparer) checkTime(a, b any) (int, bool) {
	if a, ok := a.(time.Time); ok {
		if b, ok := b.(time.Time); ok {
			return a.Compare(b), true
		}
		return -1, true
	}
	if _, ok := b.(time.Time); ok {
		return 1, true
	}
	return 0, false
}

func (c *Comparer) checkArrays(a, b any) (int, bool, error) {
	if a, ok := a.([]any); ok {
		if b, ok := b.([]any); ok {
			comp, err := c.compareArray(a, b)
			return comp, true, err
		}
		return -1, true, nil
	}
	if _, ok := b.([]any); ok {
		return 1, true, nil
	}
	return 0, false, nil
}

func (c *Comparer) checkDocs(a, b any) (int, bool, error) {
	if a, ok := a.(domain.Document); ok {
		if b, ok := b.(domain.Document); ok {
			comp, err := c.compareDoc(a, b)
			return comp, true, err
		}
		return -1, true, nil
	}
	if _, ok := b.(domain.Document); ok {
		return 1, true, nil
	}
	return 0, false, nil
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

func (c *Comparer) compareDoc(a domain.Document, b domain.Document) (int, error) {
	aKeys := slices.Collect(a.Keys())
	bKeys := slices.Collect(b.Keys())
	slices.Sort(aKeys)
	slices.Sort(bKeys)

	var comp int
	var err error
	for i := range min(len(aKeys), len(bKeys)) {
		comp, err = c.Compare(a.Get(aKeys[i]), b.Get(bKeys[i]))
		if err != nil {
			return 0, err
		}

		if comp != 0 {
			return comp, nil
		}
	}

	if comp := cmp.Compare(a.Len(), b.Len()); comp != 0 {
		return comp, nil
	}

	aKeysAny := make([]any, len(aKeys))
	for n, v := range aKeys {
		aKeysAny[n] = v
	}
	bKeysAny := make([]any, len(bKeys))
	for n, v := range bKeys {
		bKeysAny[n] = v
	}

	return c.compareArray(aKeysAny, bKeysAny)
}

func (c *Comparer) asNumber(v any) (*big.Float, bool) {
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

func (c *Comparer) isSet(v any) bool {
	if g, ok := v.(domain.Getter); ok {
		_, isSet := g.Get()
		return isSet
	}
	return true
}

func (c *Comparer) getVal(v any) any {
	if g, ok := v.(domain.Getter); ok {
		val, _ := g.Get()
		return val
	}
	return v
}
