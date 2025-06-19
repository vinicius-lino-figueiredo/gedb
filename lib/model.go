package lib

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
)

// Number represents a JSON number
type Number interface {
	float32 | float64 |
		int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64
}

/**
 * Utility functions for comparing things
 * Assumes type checking was already done (a and b already have the same type)
 * compareNSB works for numbers, strings and booleans
 * @param {number|string|boolean} a
 * @param {number|string|boolean} b
 * @return {number} 0 if a == b, 1 i a > b, -1 if a < b
 * @private
 */
func compareNSB(a, b *big.Float) int {
	return a.Cmp(b)
}

func newBigAny(v any) (*big.Float, bool) {
	bf := new(big.Float)
	switch v := v.(type) {
	case float64:
		bf.SetFloat64(v)
	case int64:
		bf.SetInt64(v)
	default:
		return nil, false
	}
	return bf, true
}

func newBig[T Number](t T) *big.Float {
	bf := new(big.Float)
	switch v := any(t).(type) {
	case float64:
		bf.SetFloat64(v)
	case int64:
		bf.SetInt64(v)
	}
	return bf
}

func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return 1
	}
	return -1
}

// - Compare { things U undefined }
// - Things are defined as any native types (string, number, boolean, null, date) and objects
// - We need to compare with undefined as it will be used in indexes
// - In the case of objects and arrays, we deep-compare
// - If two objects dont have the same type, the (arbitrary) type hierarchy is: undefined, null, number, strings, boolean, dates, arrays, objects
func compareThings(a, b any, _compareStrings func(a, b string) int) int {
	compareStrings := _compareStrings
	if _compareStrings == nil {
		compareStrings = strings.Compare
	}

	// nil (undefined/null)
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1 // no need to test if a == nil
	}

	// Numbers
	if a, ok := newBigAny(a); ok {
		// Using big.Float to safely compare float64 and int64 without
		// precision loss
		if b, ok := newBigAny(b); ok {
			return compareNSB(a, b)
		}
		return -1
	}
	if _, ok := newBigAny(b); ok {
		return 1
	}

	// Strings
	if a, ok := a.(string); ok {
		if b, ok := b.(string); ok {
			return compareStrings(a, b)
		}
		return -1
	}
	if _, ok := b.(string); ok {
		return 1
	}

	// Booleans
	if a, ok := a.(bool); ok {
		if b, ok := b.(bool); ok {
			return compareBool(a, b)
		}
		return -1
	}
	if _, ok := b.(bool); ok {
		return 1
	}

	// Arrays
	if a, ok := a.(list); ok {
		if b, ok := b.(list); ok {
			return a.compare(b, compareStrings)
		}
		return -1
	}
	if _, ok := b.(bool); ok {
		return 1
	}

	// Objects
	da := a.(Document)
	db := b.(Document)

	aKeys := slices.Collect(maps.Keys(da))
	bKeys := slices.Collect(maps.Keys(db))
	slices.Sort(aKeys)
	slices.Sort(bKeys)

	var comp int
	for i := range min(len(aKeys), len(bKeys)) {
		comp = compareThings(da[aKeys[i]], db[bKeys[i]], compareStrings)

		if comp != 0 {
			return comp
		}
	}

	return compareNSB(newBig(int64(len(da))), newBig(int64(len(db))))
}

func compareThingsFunc(compareStrings func(a, b string) int) func(a, b any) int {
	return func(a, b any) int {
		return compareThings(a, b, compareStrings)
	}
}

// ==============================================================
// Finding documents
// ==============================================================

func getDotValue(object any, fields ...string) (any, error) {
	if object == nil {
		return nil, nil
	}

	curr := object
	var err error
	for n, part := range fields {
		for nestedField := range strings.SplitSeq(part, ".") {
			switch v := curr.(type) {
			case Document:
				curr = v[nestedField]
			case list:
				curr, err = getDotValueList(v, fields[n:]...)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return curr, nil
}

func getDotValueList(v list, fieldParts ...string) (any, error) {

	i, err := strconv.Atoi(fieldParts[0])
	if err != nil {
		m := make(list, len(v))
		for n, el := range v {
			m[n], err = getDotValue(el, fieldParts...)
			if err != nil {
				return nil, err
			}
		}
		return m, nil
	}
	if i >= len(v) {
		return nil, fmt.Errorf("value %d out of bounds", i)
	}
	return v[i], nil

}

// Get dot values for either a bunch of fields or just one.
func getDotValues(obj any, fields []string) (any, error) {
	if len(fields) <= 1 {
		return getDotValue(obj, fields...)
	}
	key := make(Document)
	var err error
	for _, field := range fields {
		key[field], err = getDotValue(obj, field)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func asDoc(v any) (Document, error) {
	var d Document
	err := mapstructure.Decode(v, &d)
	if err != nil {
		return nil, err
	}
	return d, nil
}
