package matcher

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldgetter"
)

// Matcher implements domain.Matcher.
type Matcher struct {
	documentFactory func(any) (domain.Document, error)
	comparer        domain.Comparer
	fieldGetter     domain.FieldGetter
}

// NewMatcher returns a new implementation of domain.Matcher.
func NewMatcher(options ...domain.MatcherOption) domain.Matcher {

	opts := domain.MatcherOptions{
		DocumentFactory: data.NewDocument,
		Comparer:        comparer.NewComparer(),
		FieldGetter:     fieldgetter.NewFieldGetter(),
	}
	for _, option := range options {
		option(&opts)
	}

	return &Matcher{
		documentFactory: opts.DocumentFactory,
		comparer:        opts.Comparer,
		fieldGetter:     opts.FieldGetter,
	}
}

func (m *Matcher) and(obj domain.Document, query any) (bool, error) {
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

func (m *Matcher) elemMatch(_ domain.Document, a any, _ string, b any) (bool, error) {
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

func (m *Matcher) exists(obj domain.Document, _ any, field string, b any) (bool, error) {
	var bBool bool
	if b != nil {
		if n, ok := m.asNumber(b); ok {
			bBool = n.Cmp(big.NewFloat(0)) != 0
		} else {
			nb, ok := b.(bool)
			bBool = nb == ok
		}
	}

	return obj.Has(field) == bBool, nil
}

func (m *Matcher) gt(_ domain.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp > 0, nil
}

func (m *Matcher) gte(_ domain.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp >= 0, nil
}

func (m *Matcher) in(_ domain.Document, a any, _ string, b any) (bool, error) {
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

func (m *Matcher) lt(_ domain.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp < 0, nil
}

func (m *Matcher) lte(_ domain.Document, a any, _ string, b any) (bool, error) {
	if !m.comparer.Comparable(a, b) {
		return false, nil
	}
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp <= 0, nil
}

// Match implements domain.Matcher.
func (m *Matcher) Match(o domain.Document, q domain.Document) (bool, error) {
	return m.match(o, q)
}

func (m *Matcher) match(o any, q any) (bool, error) {
	if q == nil {
		return true, nil
	}
	obj, okObj := o.(domain.Document)
	query, okQuery := q.(domain.Document)
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

func (m *Matcher) matchQueryPart(obj domain.Document, queryKey string, queryValue any, treatObjAsValue bool) (bool, error) {
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
		if queryValueObj, ok := queryValue.(domain.Document); ok {
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

	if queryValueObj, ok := queryValue.(domain.Document); ok {
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

func (m *Matcher) ne(_ domain.Document, a any, _ string, b any) (bool, error) {
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp != 0, nil
}

func (m *Matcher) nin(_ domain.Document, a any, _ string, b any) (bool, error) {
	in, err := m.in(nil, a, "", b)
	if err != nil {
		return false, err
	}
	return !in, nil
}

func (m *Matcher) not(obj domain.Document, query any) (bool, error) {
	match, err := m.match(obj, query)
	if err != nil {
		return false, err
	}
	return !match, nil
}

func (m *Matcher) or(obj domain.Document, query any) (bool, error) {
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

func (m *Matcher) regex(_ domain.Document, a any, _ string, b any) (bool, error) {
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

func (m *Matcher) size(_ domain.Document, a any, _ string, b any) (bool, error) {
	aArr, ok := a.([]any)
	if !ok {
		return false, nil
	}

	num, ok := m.asNumber(b)
	if !ok || !num.IsInt() {
		return false, fmt.Errorf("$size operator called without an integer")
	}

	comp, err := m.comparer.Compare(len(aArr), b)
	if err != nil {
		return false, err
	}

	return comp == 0, nil
}

func (m *Matcher) useComparisonFunc(obj domain.Document, key string, field string, a any, b any) (bool, error) {
	comp := map[string]func(domain.Document, any, string, any) (bool, error){
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
	return false, fmt.Errorf("unknown comparison function %q", key)
}

func (m *Matcher) useLogicalOperators(k string, a domain.Document, b any) (bool, error) {
	op := map[string]func(domain.Document, any) (bool, error){
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

func (m *Matcher) where(obj domain.Document, query any) (bool, error) {
	fn, ok := query.(func(domain.Document) (bool, error))
	if !ok {
		return false, fmt.Errorf("$where operator used without a func(domain.Document) (bool, error)")
	}

	return fn(obj)
}

func (m *Matcher) asNumber(v any) (*big.Float, bool) {
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
