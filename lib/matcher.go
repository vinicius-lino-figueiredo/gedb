package lib

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Matcher implements gedb.Matcher.
type Matcher struct {
	documentFactory func(any) (gedb.Document, error)
	comparer        gedb.Comparer
	fieldGetter     gedb.FieldGetter
}

// NewMatcher returns a new implementation of gedb.Matcher.
func NewMatcher(documentFctory func(any) (gedb.Document, error), comparer gedb.Comparer, fieldGetter gedb.FieldGetter) gedb.Matcher {
	// return nil
	return &Matcher{
		documentFactory: documentFctory,
		// model:           model,
		comparer:    comparer,
		fieldGetter: fieldGetter,
	}
}

func (m *Matcher) and(obj gedb.Document, query any) (bool, error) {
	q, ok := query.([]any)
	if !ok {
		return false, fmt.Errorf("$and operator used without an array")
	}
	for _, i := range q {
		match, err := m.match(obj, i)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func (m *Matcher) elemMatch(_ gedb.Document, a any, _ string, b any) (bool, error) {
	aArr, ok := a.([]any)
	if !ok {
		return false, nil
	}

	for _, el := range aArr {
		matches, err := m.match(el, b)
		if matches || err != nil {
			return matches, err
		}
	}
	return false, nil
}

func (m *Matcher) exists(obj gedb.Document, _ any, field string, b any) (bool, error) {
	var bBool bool
	if b != nil {
		if n, ok := asNumber(b); ok {
			bBool = n.Cmp(big.NewFloat(0)) != 0
		} else {
			nb, ok := b.(bool)
			bBool = nb == ok
		}
	}

	return obj.Has(field) == bBool, nil
}

func (m *Matcher) gt(_ gedb.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp > 0, nil
}

func (m *Matcher) gte(_ gedb.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp >= 0, nil
}

func (m *Matcher) in(_ gedb.Document, a any, _ string, b any) (bool, error) {
	bArr, ok := b.([]any)
	if !ok {
		return false, fmt.Errorf("$in operator called with a non-array")
	}
	for _, el := range bArr {
		found, err := m.comparer.Compare(a, el)
		if err != nil {
			return false, err
		}
		if found == 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) lt(_ gedb.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp < 0, nil
}

func (m *Matcher) lte(_ gedb.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp <= 0, nil
}

// Match implements gedb.Matcher.
func (m *Matcher) Match(o gedb.Document, q gedb.Document) (bool, error) {
	return m.match(o, q)
}

func (m *Matcher) match(o any, q any) (bool, error) {
	if q == nil {
		return true, nil
	}
	obj, okObj := o.(gedb.Document)
	query, okQuery := q.(gedb.Document)
	if !okObj {
		var err error
		obj, err = m.documentFactory(map[string]any{"needAKey": obj})
		if err != nil {
			return false, err
		}
	}
	if !okQuery {
		var err error
		query, err = m.documentFactory(query)
		if err != nil {
			return false, err
		}
	}
	for queryKey, queryValue := range query.Iter() {
		if !strings.HasPrefix(queryKey, "$") {
			matches, err := m.matchQueryPart(obj, queryKey, queryValue, false)
			if !matches || err != nil {
				return matches, err
			}
			continue
		}

		matches, err := m.useLogicalOperators(queryKey, obj, queryValue)
		if !matches || err != nil {
			return matches, err
		}
	}
	return true, nil
}

func (m *Matcher) matchQueryPart(obj gedb.Document, queryKey string, queryValue any, treatObjAsValue bool) (bool, error) {
	objValues, fieldExists, err := m.fieldGetter.GetField(obj, queryKey)
	if err != nil {
		return false, err
	}
	if !fieldExists {
		return false, nil
	}
	objValue := any(objValues)
	if len(objValues) == 1 {
		objValue = objValues[0]
	}
	if objValueArray, ok := objValue.([]any); ok && !treatObjAsValue {
		if _, ok := queryValue.([]any); ok {
			return m.matchQueryPart(obj, queryKey, queryValue, true)
		}
		if queryValueObj, ok := queryValue.(gedb.Document); ok {
			for key := range queryValueObj.Iter() {
				switch key {
				case "$size", "$elemMatch":
					return m.matchQueryPart(obj, queryKey, queryValueObj, true)
				default:
				}
			}
		}
		for _, el := range objValueArray {
			elObj, err := m.documentFactory(map[string]any{"k": el})
			if err != nil {
				return false, err
			}
			matches, err := m.matchQueryPart(elObj, "k", queryValue, false)
			if err != nil {
				return false, err
			}
			if matches {
				return true, nil
			}
		}
		return false, nil
	}

	if queryValueObj, ok := queryValue.(gedb.Document); ok {
		dollarFields := 0
		for key := range queryValueObj.Keys() {
			if strings.HasPrefix(key, "$") {
				dollarFields++
			}
		}
		if dollarFields != 0 && dollarFields != queryValueObj.Len() {
			return false, fmt.Errorf("you cannot mix operators and normal fields")
		}

		if dollarFields > 0 {
			for key, value := range queryValueObj.Iter() {
				matches, err := m.useComparisonFunc(obj, key, queryKey, objValue, value)
				if !matches || err != nil {
					return false, err
				}
			}
			return true, nil
		}
	}

	if queryValueRegex, ok := queryValue.(*regexp.Regexp); ok {
		return m.regex(nil, objValue, "", queryValueRegex)
	}

	comp, err := m.comparer.Compare(objValue, queryValue)
	if err != nil {
		return false, err
	}

	return comp == 0, nil
}

func (m *Matcher) ne(_ gedb.Document, a any, _ string, b any) (bool, error) {
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp != 0, nil
}

func (m *Matcher) nin(_ gedb.Document, a any, _ string, b any) (bool, error) {
	in, err := m.in(nil, a, "", b)
	if err != nil {
		return false, err
	}
	return !in, nil
}

func (m *Matcher) not(obj gedb.Document, query any) (bool, error) {
	match, err := m.match(obj, query)
	if err != nil {
		return false, err
	}
	return !match, nil
}

func (m *Matcher) or(obj gedb.Document, query any) (bool, error) {
	q, ok := query.([]any)
	if !ok {
		return false, fmt.Errorf("$or operator used without an array")
	}
	for _, i := range q {
		match, err := m.match(obj, i)
		if err != nil {
			return false, nil
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) regex(_ gedb.Document, a any, _ string, b any) (bool, error) {
	rgx, ok := b.(*regexp.Regexp)
	if !ok {
		return false, fmt.Errorf("$regex operator called with non regular expression")
	}
	str, ok := a.(string)
	if !ok {
		return false, nil
	}
	return rgx.MatchString(str), nil
}

func (m *Matcher) size(_ gedb.Document, a any, _ string, b any) (bool, error) {
	aArr, ok := a.([]any)
	if !ok {
		return false, nil
	}

	num, ok := asNumber(b)
	if !ok || !num.IsInt() {
		return false, fmt.Errorf("$size operator called without an integer")
	}

	comp, err := m.comparer.Compare(len(aArr), b)
	if err != nil {
		return false, err
	}

	return comp == 0, nil
}

func (m *Matcher) useComparisonFunc(obj gedb.Document, key string, field string, a any, b any) (bool, error) {
	comp := map[string]func(gedb.Document, any, string, any) (bool, error){
		"$lt":        m.lt,
		"$lte":       m.lte,
		"$gt":        m.gt,
		"$gte":       m.gte,
		"$ne":        m.ne,
		"$in":        m.in,
		"$nin":       m.nin,
		"$regex":     m.regex,
		"$exists":    m.exists,
		"$size":      m.size,
		"$elemMatch": m.elemMatch,
	}
	if fn, ok := comp[key]; ok {
		return fn(obj, a, field, b)
	}
	return false, fmt.Errorf("Unknown comparison function %q", key)
}

func (m *Matcher) useLogicalOperators(k string, a gedb.Document, b any) (bool, error) {
	op := map[string]func(gedb.Document, any) (bool, error){
		"$and":   m.and,
		"$not":   m.not,
		"$or":    m.or,
		"$where": m.where,
	}
	if fn, ok := op[k]; ok {
		return fn(a, b)
	}
	return false, fmt.Errorf("unknown logic operator %q", k)

}

func (m *Matcher) where(obj gedb.Document, query any) (bool, error) {
	fn, ok := query.(func(gedb.Document) (bool, error))
	if !ok {
		return false, fmt.Errorf("$where operator used without a func(gedb.Document) (bool, error)")
	}

	return fn(obj)
}
