package matcher

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
)

type oper func(domain.Document, []string, any) (bool, error)

type matchFn func(any, any) (bool, error)

// Matcher implements [domain.Matcher].
type Matcher struct {
	documentFactory func(any) (domain.Document, error)
	comparer        domain.Comparer
	fieldNavigator  domain.FieldNavigator
	compFuncs       map[string]oper
	logicOps        map[string]func(domain.Document, any) (bool, error)
}

// NewMatcher returns a new implementation of domain.Matcher.
func NewMatcher(options ...domain.MatcherOption) domain.Matcher {

	docFac := data.NewDocument
	opts := domain.MatcherOptions{
		DocumentFactory: docFac,
		Comparer:        comparer.NewComparer(),
		FieldNavigator:  fieldnavigator.NewFieldNavigator(docFac),
	}
	for _, option := range options {
		option(&opts)
	}

	m := &Matcher{
		documentFactory: opts.DocumentFactory,
		comparer:        opts.Comparer,
		fieldNavigator:  opts.FieldNavigator,
	}
	m.logicOps = map[string]func(domain.Document, any) (bool, error){
		"$and":   m.and,
		"$not":   m.not,
		"$or":    m.or,
		"$where": m.where,
	}
	m.compFuncs = map[string]oper{
		"$regex":     m.regex,
		"$nin":       m.nin,
		"$lt":        m.lt,
		"$gte":       m.gte,
		"$lte":       m.lte,
		"$gt":        m.gt,
		"$ne":        m.ne,
		"$in":        m.in,
		"$exists":    m.exists,
		"$size":      m.size,
		"$elemMatch": m.elemMatch,
	}

	return m
}

// Match implements [domain.Matcher].
func (m *Matcher) Match(val any, qry any) (bool, error) {
	if qry == nil {
		return true, nil
	}
	doc, ok := val.(domain.Document)
	if !ok {
		return m.nonDocMatch(val, qry)
	}

	query, ok := qry.(domain.Document)
	if !ok {
		// if val is not a doc, there is no non-doc value that can match
		return false, nil
	}

	return m.matchDocs(doc, query)

}

func (m *Matcher) nonDocMatch(val any, qry any) (bool, error) {
	valDoc, err := m.documentFactory(nil)
	if err != nil {
		return false, err
	}
	qryDoc, err := m.documentFactory(nil)
	if err != nil {
		return false, err
	}

	valDoc.Set("needAKey", val)
	qryDoc.Set("needAKey", qry)

	matches, err := m.matchDocs(valDoc, qryDoc)
	if err != nil {
		return false, err
	}
	return matches, nil
}

func (m *Matcher) matchDocs(obj, qry domain.Document) (bool, error) {

	qryMap, hasOps, err := m.mapQuery(qry)
	if err != nil {
		return false, err
	}

	matchFunction := m.matchSimpleField
	if hasOps {
		matchFunction = m.matchDollarField
	}

	for field, value := range qryMap {
		matches, err := matchFunction(obj, field, value)
		if err != nil || !matches {
			return false, err
		}
	}
	return true, nil

}

func (m *Matcher) matchDollarField(obj domain.Document, field string, value any) (bool, error) {
	fn, ok := m.logicOps[field]
	if !ok {
		return false, fmt.Errorf("unknown logical operator %s", field)
	}
	return fn(obj, value)
}

func (m *Matcher) matchSimpleField(obj domain.Document, field string, value any) (bool, error) {
	addr, err := m.fieldNavigator.GetAddress(field)
	if err != nil {
		return false, err
	}

	valueDoc, ok := value.(domain.Document)
	if !ok {
		return m.eq(obj, addr, value)
	}

	qryMap, hasOps, err := m.mapQuery(valueDoc)
	if err != nil {
		return false, err
	}

	if !hasOps {
		return m.eq(obj, addr, value)
	}

	for op := range qryMap {
		_, ok := m.compFuncs[op]
		if !ok {
			return false, fmt.Errorf("unknown comparison function %s", op)
		}
	}

	for op, arg := range qryMap {
		matches, err := m.compFuncs[op](obj, addr, arg)
		if err != nil || !matches {
			return false, err
		}
	}

	return true, nil
}

func (m *Matcher) mapQuery(qry domain.Document) (map[string]any, bool, error) {
	queryMap := make(map[string]any, qry.Len())
	totalFields := 0
	dollarFields := 0
	for field, value := range qry.Iter() {
		totalFields++
		if strings.HasPrefix(field, "$") {
			dollarFields++
		}
		if dollarFields > 0 && totalFields != dollarFields {
			return nil, false, fmt.Errorf("you cannot mix operators and normal fields")
		}
		// We are saving the values to a map. There is no way to know
		// how costly it is to iterate over the query fields, and match
		// should check the commands before executing them.
		queryMap[field] = value
	}
	return queryMap, dollarFields != 0, nil
}

func (m *Matcher) and(obj domain.Document, value any) (bool, error) {
	value, _ = m.getValue(value)
	arr, ok := value.([]any)
	if !ok {
		return false, fmt.Errorf("$and operator used without an array")
	}
	for _, item := range arr {
		matches, err := m.Match(obj, item)
		if err != nil || !matches {
			return false, err
		}
	}
	return true, nil
}

func (m *Matcher) not(obj domain.Document, value any) (bool, error) {
	matches, err := m.Match(obj, value)
	if err != nil {
		return false, err
	}
	return !matches, nil
}

func (m *Matcher) or(obj domain.Document, value any) (bool, error) {
	value, _ = m.getValue(value)
	arr, ok := value.([]any)
	if !ok {
		return false, fmt.Errorf("$or operator used without an array")
	}
	for _, item := range arr {
		matches, err := m.Match(obj, item)
		if err != nil || matches {
			return matches, err
		}
	}
	return false, nil
}

func (m *Matcher) where(obj domain.Document, value any) (bool, error) {
	value, _ = m.getValue(value)

	switch where := value.(type) {
	case func(domain.Document) bool:
		return where(obj), nil

	case func(domain.Document) (bool, error):
		return where(obj)

	default:
		// original code would check the return type but we are not
		// doing that because it is not worth the cost
		return false, fmt.Errorf("$where operator used without a function")
	}

}

func (m *Matcher) matchList(obj domain.Document, addr []string, value any, fn matchFn) (bool, error) {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return false, err
	}

	for _, field := range fields {
		matches, err := m.matchGetter(field, value, fn)
		if err != nil || matches {
			return matches, err
		}
	}

	return false, nil
}

func (m *Matcher) matchGetter(field domain.Getter, value any, fn matchFn) (bool, error) {
	fieldVal, _ := field.Get()
	arr, ok := fieldVal.([]any)
	if !ok {
		arr = []any{field}
	}
	for _, item := range arr {
		matches, err := fn(item, value)
		if err != nil || matches {
			return matches, err
		}
	}
	return false, nil
}

func (m *Matcher) eq(obj domain.Document, addr []string, value any) (bool, error) {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return false, err
	}
	var matches bool
	for _, field := range fields {
		matches, err = m.compare(field, value)
		if err != nil {
			return false, err
		}
		if matches {
			break
		}
	}
	return matches, nil
}

func (m *Matcher) compare(field domain.Getter, value any) (bool, error) {
	if rgx, ok := value.(*regexp.Regexp); ok {
		return m.regexValues(field, rgx)
	}
	fieldValue, _ := field.Get()
	arr, ok := fieldValue.([]any)
	if !ok {
		c, err := m.comparer.Compare(field, value)
		return c == 0, err
	}

	valueConcrete, _ := m.getValue(value)
	if valueArr, ok := valueConcrete.([]any); ok {
		c, err := m.comparer.Compare(arr, valueArr)
		if err != nil {
			return false, err
		}
		return c == 0, nil
	}

	for _, item := range arr {
		c, err := m.comparer.Compare(item, value)
		if err != nil || c == 0 {
			return c == 0, err
		}
	}
	return false, nil
}

func (m *Matcher) getValue(v any) (any, bool) {
	if g, ok := v.(domain.Getter); ok {
		return g.Get()
	}
	return v, true
}

func (m *Matcher) asInt(v any) (int, bool) {
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
		return 0, false
	}
	if !r.IsInt() {
		return 0, false
	}
	i64, _ := r.Int64()
	return int(i64), true
}

func (m *Matcher) regex(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		rgx, ok := param.(*regexp.Regexp)
		if !ok {
			return false, fmt.Errorf("$regex operator called with non regular expression")
		}
		return m.regexValues(value, rgx)
	})
}

func (m *Matcher) regexValues(a any, rgx *regexp.Regexp) (bool, error) {
	val, defined := m.getValue(a)
	if !defined {
		return false, nil
	}
	if str, ok := val.(string); ok {
		return rgx.MatchString(str), nil
	}
	return false, nil
}

func (m *Matcher) nin(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		concrete, _ := m.getValue(param)
		arr, ok := concrete.([]any)
		if !ok {
			return false, fmt.Errorf("$nin operator called with a non-array")
		}
		for _, item := range arr {
			found, err := m.comparer.Compare(item, value)
			if err != nil {
				return false, err
			}
			if found == 0 {
				return false, nil
			}
		}
		return true, nil
	})
}

func (m *Matcher) lt(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		if !m.comparer.Comparable(value, param) {
			return false, nil
		}
		c, err := m.comparer.Compare(value, param)
		if err != nil {
			return false, err
		}
		return c < 0, nil
	})
}

func (m *Matcher) gte(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		if !m.comparer.Comparable(value, param) {
			return false, nil
		}
		c, err := m.comparer.Compare(value, param)
		if err != nil {
			return false, err
		}
		return c >= 0, nil
	})
}

func (m *Matcher) lte(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		if !m.comparer.Comparable(value, param) {
			return false, nil
		}
		c, err := m.comparer.Compare(value, param)
		if err != nil {
			return false, err
		}
		return c <= 0, nil
	})
}

func (m *Matcher) gt(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		if !m.comparer.Comparable(value, param) {
			return false, nil
		}
		c, err := m.comparer.Compare(value, param)
		if err != nil {
			return false, err
		}
		return c > 0, nil
	})
}

func (m *Matcher) ne(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		if !m.comparer.Comparable(value, param) {
			return false, nil
		}
		c, err := m.comparer.Compare(value, param)
		if err != nil {
			return false, err
		}
		return c != 0, nil
	})
}

func (m *Matcher) in(obj domain.Document, addr []string, b any) (bool, error) {
	return m.matchList(obj, addr, b, func(value, param any) (bool, error) {
		concrete, _ := m.getValue(param)
		arr, ok := concrete.([]any)
		if !ok {
			return false, fmt.Errorf("$in operator called with a non-array")
		}
		for _, item := range arr {
			found, err := m.comparer.Compare(item, value)
			if err != nil {
				return false, err
			}
			if found == 0 {
				return true, nil
			}
		}
		return false, nil
	})
}

func (m *Matcher) exists(obj domain.Document, addr []string, b any) (bool, error) {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return false, err
	}

	wantExistent, err := m.isTruthy(b)
	if err != nil {
		return false, err
	}

	for _, field := range fields {
		if _, defined := field.Get(); defined {
			return wantExistent, nil
		}
	}
	return !wantExistent, nil
}

func (m *Matcher) isTruthy(value any) (bool, error) {
	value, _ = m.getValue(value)
	if value == nil {
		return false, nil
	}

	c, err := m.comparer.Compare(value, 0)
	if err != nil || c == 0 {
		return c != 0, err
	}

	c, err = m.comparer.Compare(value, false)

	return c != 0, err
}

func (m *Matcher) size(obj domain.Document, addr []string, b any) (bool, error) {
	fields, expanded, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return false, err
	}

	num, ok := m.asInt(b)
	if !ok {
		return false, fmt.Errorf("$size operator called without an integer")
	}

	if expanded {
		return len(fields) == num, nil
	}

	value, _ := fields[0].Get()
	if value == nil {
		return false, nil
	}

	if arr, ok := value.([]any); ok {
		return len(arr) == num, nil
	}

	return false, nil
}

func (m *Matcher) elemMatch(obj domain.Document, addr []string, b any) (bool, error) {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return false, err
	}

	for _, field := range fields {
		value, _ := field.Get()
		arr, ok := value.([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			matches, err := m.Match(item, b)
			if err != nil || matches {
				return matches, err
			}
		}
	}

	return false, nil
}
