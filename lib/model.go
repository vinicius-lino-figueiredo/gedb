package lib

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

func isNumber(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case int64, float64:
		return true
	default:
		return false
	}
}

func is[T comparable](v any, c T) bool {
	if v == nil {
		return false
	}
	if vt, ok := v.(T); ok {
		return vt == c
	}
	return false
}

func checkKey(k string, v any) error {
	if strings.HasPrefix(k, "$") && !(k == "$$date" && isNumber(v)) && !(k == "$$deleted" && is(v, true)) && k != "$$indexCreated" && k != "$$indexRemoved" {
		return errors.New("field names cannot start with the $ character")
	}
	if strings.ContainsRune(k, '.') {
		return errors.New("field names cannot contain a '.'")
	}
	return nil
}

func Map(d gedb.Document) map[string]any {
	m := make(Document, d.Len())
	for k, v := range d.Iter() {
		if doc, ok := v.(gedb.Document); ok {
			v = Map(doc)
		}
		m[k] = v
	}
	return m
}

func asMap(doc gedb.Document) map[string]any {
	res := make(map[string]any)
	for k, v := range doc.Iter() {
		v2, ok := v.(gedb.Document)
		if ok {
			v = asMap(v2)
		}
		res[k] = v
	}
	return res
}

//	const serialize = obj => {
//	  return JSON.stringify(obj, function (k, v) {
//	    checkKey(k, v)
//
//	    if (v === undefined) return undefined
//	    if (v === null) return null
//
//	    // Hackish way of checking if object is Date (this way it works between execution contexts in node-webkit).
//	    // We can't use value directly because for dates it is already string in this function (date.toJSON was already called), so we use this
//	    if (typeof this[k].getTime === 'function') return { $$date: this[k].getTime() }
//
//	    return v
//	  })
//	}
//
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
	if a, ok := a.([]any); ok {
		if b, ok := b.([]any); ok {
			return compareArray(a, b)
		}
		return -1
	}
	if _, ok := b.(bool); ok {
		return 1
	}

	// Objects
	da := a.(gedb.Document)
	db := b.(gedb.Document)

	aKeys := slices.Collect(da.Keys())
	bKeys := slices.Collect(db.Keys())
	slices.Sort(aKeys)
	slices.Sort(bKeys)

	var comp int
	for i := range min(len(aKeys), len(bKeys)) {
		comp = compareThings(da.Get(aKeys[i]), db.Get(bKeys[i]), compareStrings)

		if comp != 0 {
			return comp
		}
	}

	return compareNSB(newBig(int64(da.Len())), newBig(int64(db.Len())))
}

func compareArray(a, b []any) int {
	minLength := min(len(a), len(b))

	var comp int
	for i := range minLength {
		comp = compareThings(a[i], b[i], nil)

		if comp != 0 {
			return comp
		}
	}

	// Common section was identical, longest one wins
	return compareNSB(newBig(int64(len(a))), newBig(int64(len(b))))
}

// const compareArrays = (a, b) => {
//   const minLength = Math.min(a.length, b.length)
//   for (let i = 0; i < minLength; i += 1) {
//     const comp = compareThings(a[i], b[i])
//
//     if (comp !== 0) return comp
//   }
//
//   // Common section was identical, longest one wins
//   return compareNSB(a.length, b.length)
// }

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
			case []any:
				curr, err = getDotValueList(v, fields[n:]...)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return curr, nil
}

func getDotValueList(v []any, fieldParts ...string) (any, error) {

	i, err := strconv.Atoi(fieldParts[0])
	if err != nil {
		m := make([]any, len(v))
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
