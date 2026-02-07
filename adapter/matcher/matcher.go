// Package matcher contains the default implementation of [domain.Matcher]
// using basic mongo-like match API.
package matcher

import (
	"errors"
	"fmt"
	"iter"
	"regexp"
	"slices"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/structure"
)

var (
	// ErrMixedOperators is returned when user provides a query with mixed
	// use of normal fields and operators.
	ErrMixedOperators = errors.New("cannot mix operators and normal fields")
)

// ErrUnknownOperator is returned when user provides an unknown dollar field.
type ErrUnknownOperator struct {
	Operator string
}

// Error implements [error].
func (e ErrUnknownOperator) Error() string {
	return fmt.Sprintf("unknown operator %q", e.Operator)
}

// ErrUnknownComparison is returned when an unknown compare field is provided.
type ErrUnknownComparison struct {
	Comparison string
}

// Error implements [error].
func (e ErrUnknownComparison) Error() string {
	return fmt.Sprintf("unknown comparison %q", e.Comparison)
}

// ErrCompArgType is returned when a comparison operator is called with an
// argument of invalid type.
type ErrCompArgType struct {
	Comp   string
	Want   string
	Actual any
}

// Error implements [error].
func (e ErrCompArgType) Error() string {
	return fmt.Sprintf(
		"%s value should be of type %s, got %T",
		e.Comp, e.Want, e.Actual,
	)
}

// Matcher implements [domain.Matcher].
type Matcher struct {
	documentFactory domain.DocumentFactory
	comparer        domain.Comparer
	fieldNavigator  domain.FieldNavigator
	query           Query
	doc             domain.Document
}

// NewMatcher returns a new implementation of domain.Matcher.
func NewMatcher(options ...Option) domain.Matcher {

	m := &Matcher{
		documentFactory: data.NewDocument,
		comparer:        comparer.NewComparer(),
		fieldNavigator: fieldnavigator.NewFieldNavigator(
			data.NewDocument,
		),
	}

	for _, option := range options {
		option(m)
	}

	return m
}

// SetQuery implements [domain\.Matcher].
func (m *Matcher) SetQuery(query any) error {
	qry, err := m.makeQuery(query)
	if err == nil {
		m.query = qry
	}
	return err
}

func (m *Matcher) makeQuery(query any) (qry Query, err error) {
	if query == nil {
		return qry, nil
	}
	i, l, err := structure.Seq2(query)
	if err != nil {
		qry = Query{Sub: true, Lo: []LogicOp{
			{Type: And, Rules: []FieldRule{
				{Addr: []string{"needAKey"}, Conds: []Cond{
					{Op: Eq, Val: query},
				}},
			}},
		}}
		return qry, nil
	}

	mapping, dollar, err := m.ensureNotMixed(i, l)
	if err != nil {
		return qry, err
	}

	if dollar > 0 {
		qry.Lo, qry.Sub, err = m.dollarCond(mapping, true, make([]LogicOp, 0, l))
		if err != nil {
			return qry, err
		}
		return qry, nil
	}

	qry.Lo = make([]LogicOp, 1)
	qry.Lo[0], err = m.equalCond(mapping)
	if err != nil {
		return qry, err
	}
	m.query = qry
	return qry, nil
}

func (m *Matcher) ensureNotMixed(i iter.Seq2[string, any], l int) (map[string]any, int, error) {
	mapping := make(map[string]any, l)
	var dollar, total int
	for k, v := range i {
		total++
		if len(k) != 0 && k[0] == '$' {
			dollar++
		}
		if dollar > 0 && dollar != total {
			return nil, dollar, ErrMixedOperators
		}
		mapping[k] = v
	}
	return mapping, dollar, nil
}

func (m *Matcher) makeLogicOp(typ uint8, name string, v any) (LogicOp, error) {
	lo := LogicOp{Type: typ}
	items, l, err := structure.Seq(v)
	if err != nil {
		return lo, fmt.Errorf("%w: %w", ErrCompArgType{Comp: name, Want: "list", Actual: v}, err)
	}
	if l == 0 {
		return lo, nil
	}
	lo.Sub = make([]LogicOp, 0, l)

	var i iter.Seq2[string, any]

	for item := range items {
		i, l, err = structure.Seq2(item)
		if err != nil {
			return lo, err
		}
		lo.Sub, err = m.subOp(i, l, lo.Sub)
		if err != nil {
			return lo, err
		}
	}
	return lo, nil
}

func (m *Matcher) subOp(i iter.Seq2[string, any], l int, sub []LogicOp) (_ []LogicOp, err error) {

	mapping, dollar, err := m.ensureNotMixed(i, l)
	if err != nil {
		return nil, err
	}

	if dollar == 0 {
		sub = append(sub, LogicOp{})
		sub[len(sub)-1], err = m.equalCond(mapping)
		return sub, err
	}

	sub, _, err = m.dollarCond(mapping, false, sub)

	return sub, err
}

func (m *Matcher) dollarCond(mapping map[string]any, root bool, target []LogicOp) (_ []LogicOp, invalid bool, err error) {
	var found bool

	for key, value := range mapping {
		switch key {
		case "$and":
			or, err := m.makeLogicOp(And, "$and", value)
			if err != nil {
				return nil, false, err
			}
			target = append(target, or)
			return target, false, nil
		case "$or":
			or, err := m.makeLogicOp(Or, "$or", value)
			if err != nil {
				return target, false, err
			}
			target = append(target, or)
			return target, false, nil
		case "$not":
			i, l, err := structure.Seq2(value)
			if err != nil {
				return nil, false, err
			}
			not, err := m.subOp(i, l, make([]LogicOp, 0, 1))
			if err != nil {
				return nil, false, err
			}
			target = append(target, LogicOp{Type: Not, Sub: not})
			return target, invalid, err
		case "$where":
			where, ok := value.(func(any) (bool, error))
			if !ok {
				return target, false, ErrCompArgType{Comp: "$where", Want: "func(any) (bool, error)", Actual: value}
			}
			target = append(target, LogicOp{Type: Where, Where: &where})
			return target, invalid, nil
		default:
			if !root {
				return target, false, ErrUnknownComparison{Comparison: key}
			}

			target = make([]LogicOp, 1)
			target[0].Rules = make([]FieldRule, 1)
			target[0].Rules[0].Addr = []string{"needAKey"}
			target[0].Rules[0].Conds = make([]Cond, 0, len(mapping))

			var cond Cond
			var err error
			for k, v := range mapping {
				cond, found, err = m.makeCond(k, v)
				if !found {
					return target, false, ErrUnknownOperator{Operator: k}
				}
				if err != nil {
					return target, false, err
				}
				target[0].Rules[0].Conds = append(target[0].Rules[0].Conds, cond)
			}
			return target, true, nil
		}
	}
	return target, invalid, err
}

func (m *Matcher) equalCond(mapping map[string]any) (lo LogicOp, err error) {
	lo.Rules = make([]FieldRule, 0, len(mapping))

	var fr FieldRule
	for key, value := range mapping {
		fr, err = m.makeFieldRule(key, value)
		if err != nil {
			return lo, err
		}

		lo.Rules = append(lo.Rules, fr)
	}

	return lo, nil
}

func (m *Matcher) makeFieldRule(field string, obj any) (fr FieldRule, err error) {
	addr, err := m.fieldNavigator.GetAddress(field)
	if err != nil {
		return fr, err
	}

Loop:
	for {
		switch t := obj.(type) {
		case *regexp.Regexp:
			return FieldRule{Addr: addr, Conds: []Cond{{Op: Regex, Val: t}}}, nil
		case time.Time:
			return FieldRule{Addr: addr, Conds: []Cond{{Val: t}}}, nil
		case domain.Getter:
			if actual, ok := t.Get(); ok {
				obj = actual
				continue
			}
			return FieldRule{Addr: addr, Conds: []Cond{{Val: t}}}, nil
		default:
			break Loop
		}
	}

	i, l, err := structure.Seq2(obj)
	if err != nil {
		return FieldRule{Addr: addr, Conds: []Cond{{Val: obj}}}, nil
	}

	if l == 0 {
		return fr, nil
	}

	mapping, dollar, err := m.ensureNotMixed(i, l)
	if err != nil {
		return fr, err
	}

	if dollar > 0 {
		return m.makeDollarRule(addr, mapping)
	}

	doc, err := m.documentFactory(obj)
	if err != nil {
		return fr, err
	}

	return FieldRule{Addr: addr, Conds: []Cond{{Val: doc}}}, nil

}

func (m *Matcher) makeDollarRule(addr []string, mapping map[string]any) (fr FieldRule, err error) {
	rule := FieldRule{
		Addr:  addr,
		Conds: make([]Cond, 0, len(mapping)),
	}

	var cond Cond
	for key, value := range mapping {
		if cond, _, err = m.makeCond(key, value); err != nil {
			return fr, err
		}
		rule.Conds = append(rule.Conds, cond)
	}

	return rule, nil
}

func (m *Matcher) makeCond(k string, v any) (cond Cond, found bool, err error) {
	switch k {
	case "$regex":
		return m.makeRegex(v)
	case "$nin":
		return m.makeNin(v)
	case "$lt":
		return Cond{Op: Lt, Val: v}, true, nil
	case "$gte":
		return Cond{Op: Gte, Val: v}, true, nil
	case "$lte":
		return Cond{Op: Lte, Val: v}, true, nil
	case "$gt":
		return Cond{Op: Gt, Val: v}, true, nil
	case "$ne":
		return Cond{Op: Ne, Val: v}, true, nil
	case "$in":
		return m.makeIn(v)
	case "$exists":
		return m.makeExists(v)
	case "$size":
		return m.makeSize(v)
	case "$elemMatch":
		return m.makeElemMatch(v)
	default:
		return cond, false, ErrUnknownComparison{Comparison: k}
	}
}

func (m *Matcher) makeRegex(v any) (cond Cond, found bool, err error) {
	if r, ok := v.(*regexp.Regexp); ok {
		return Cond{Op: Regex, Val: r}, true, nil
	}
	return cond, true, ErrCompArgType{Comp: "$regex", Want: "regex", Actual: v}
}

func (m *Matcher) makeNin(v any) (cond Cond, found bool, err error) {
	seq, l, err := structure.Seq(v)
	if err != nil {
		return cond, true, ErrCompArgType{Comp: "$nin", Want: "list", Actual: v}
	}
	return Cond{Op: Nin, Val: seq, size: l}, true, nil
}

func (m *Matcher) makeIn(v any) (cond Cond, found bool, err error) {
	seq, l, err := structure.Seq(v)
	if err != nil {
		return cond, true, ErrCompArgType{Comp: "$in", Want: "list", Actual: v}
	}
	return Cond{Op: In, Val: seq, size: l}, true, nil
}

func (m *Matcher) makeExists(v any) (Cond, bool, error) {
	value, _ := m.getConcrete(v)
	if value == nil {
		return Cond{Op: Exists, Val: false}, true, nil
	}
	if exists, ok := value.(bool); ok {
		return Cond{Op: Exists, Val: exists}, true, nil
	}
	if c, err := m.comparer.Compare(value, 0); err != nil || c == 0 {
		return Cond{Op: Exists, Val: c != 0}, true, err
	}
	return Cond{Op: Exists, Val: true}, true, nil
}

func (m *Matcher) makeSize(v any) (cond Cond, found bool, err error) {
	i, ok := structure.AsInteger(v)
	if !ok {
		return cond, true, ErrCompArgType{Comp: "$size", Want: "integer", Actual: v}
	}
	return Cond{Op: Size, Val: i}, true, nil
}

func (m *Matcher) makeElemMatch(v any) (cond Cond, found bool, err error) {
	qry, err := m.makeQuery(v)
	if err != nil {
		return cond, true, err
	}
	return Cond{Op: ElemMatch, Val: qry}, true, nil
}

// Match implements [domain.Matcher].
func (m *Matcher) Match(value any) (matches bool, err error) {
	return m.matchQuery(value, m.query)
}

func (m *Matcher) matchQuery(value any, query Query) (bool, error) {
	doc, ok := value.(domain.Document)

	var err error
	if !ok || m.query.Sub {
		if m.doc == nil {
			if m.doc, err = m.documentFactory(nil); err != nil {
				return false, err
			}
		}
		doc = m.doc
		doc.Set("needAKey", value)
	}

	var matches bool
	for _, lo := range query.Lo {
		matches, err = m.matchLogicOp(doc, lo)
		if err != nil || !matches {
			return matches, err
		}
	}
	return true, nil
}

func (m *Matcher) matchLogicOp(doc domain.Document, lo LogicOp) (bool, error) {
	var matches bool
	var err error
	switch lo.Type {
	case And:
		for _, sub := range lo.Sub {
			matches, err = m.matchLogicOp(doc, sub)
			if err != nil || !matches {
				return matches, err
			}
		}
		for _, rule := range lo.Rules {
			matches, err = m.matchRule(doc, rule)
			if err != nil || !matches {
				return matches, err
			}
		}
		return true, nil
	case Or:
		for _, sub := range lo.Sub {
			matches, err = m.matchLogicOp(doc, sub)
			if err != nil || matches {
				return matches, err
			}
		}
		return false, nil
	case Not:
		matches, err = m.matchLogicOp(doc, lo.Sub[0])
		if err != nil {
			return false, err
		}
		return !matches, nil
	case Where:
		return (*lo.Where)(doc)
	default:
		return false, nil
	}
}

func (m *Matcher) matchRule(doc domain.Document, rule FieldRule) (bool, error) {
	values, expanded, err := m.fieldNavigator.GetField(doc, rule.Addr...)
	if err != nil {
		return false, err
	}

	var matches bool
	for _, cond := range rule.Conds {
		matches, err = m.matchCond(values, expanded, &cond)
		if err != nil || !matches {
			return matches, err
		}
	}
	return true, nil

}

func (m *Matcher) matchCond(values []domain.GetSetter, expanded bool, cond *Cond) (bool, error) {
	switch cond.Op {
	case Eq:
		return m.eq(values, expanded, cond)
	case Regex:
		return m.regex(values, cond)
	case Nin:
		return m.nin(values, cond)
	case Lt:
		return m.lt(values, cond)
	case Gte:
		return m.gte(values, cond)
	case Lte:
		return m.lte(values, cond)
	case Gt:
		return m.gt(values, cond)
	case Ne:
		return m.ne(values, cond)
	case In:
		return m.in(values, cond)
	case Exists:
		return m.exists(values, cond)
	case Size:
		return m.size(values, expanded, cond)
	case ElemMatch:
		return m.elemMatch(values, cond)
	default:
		return false, nil
	}
}

func (m *Matcher) eq(values []domain.GetSetter, expanded bool, cond *Cond) (bool, error) {
	var actual any
	var c int
	var err error
	var ok bool
	var arr []any
	var end bool

	if expanded {
		ok, end, err = m.eqExpanded(values, cond)
		if end {
			return ok, err
		}
	}

	for _, value := range values {
		if actual, ok = m.getConcrete(value); !ok {
			continue
		}

		if arr, ok = actual.([]any); ok {
			concrete, _ := m.getConcrete(cond.Val)
			ok, err = structure.Contains(arr, concrete, m.compare)
			if err != nil || ok {
				return ok, err
			}
		}
		c, err = m.comparer.Compare(actual, cond.Val)
		if err != nil {
			return false, err
		}
		if c == 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) eqExpanded(values []domain.GetSetter, cond *Cond) (bool, bool, error) {
	var concrete any
	var ok bool
	var arr []any
	var c int
	var err error
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					continue
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, true, err
				}
				if c == 0 {
					return true, true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			continue
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, true, err
		}
		if c == 0 {
			return true, true, nil
		}
	}
	return false, false, nil
}

func (m *Matcher) getConcrete(v any) (res any, ok bool) {
	var g domain.Getter
	res = v
	for {
		if g, ok = res.(domain.Getter); !ok {
			return res, true
		}
		if res, ok = g.Get(); !ok {
			return nil, false
		}
	}
}

func (m *Matcher) compare(a, b any) (bool, error) {
	comp, err := m.comparer.Compare(a, b)
	if err != nil {
		return false, err
	}
	return comp == 0, nil
}

func (m *Matcher) regex(values []domain.GetSetter, cond *Cond) (bool, error) {
	rgx := cond.Val.(*regexp.Regexp)
	var actual any
	var ok bool
	var str string
	for _, value := range values {
		if actual, ok = m.getConcrete(value); !ok {
			return false, nil
		}
		if str, ok = actual.(string); !ok {
			return false, nil
		}
		if !rgx.MatchString(str) {
			return false, nil
		}
	}
	return true, nil
}

func (m *Matcher) nin(values []domain.GetSetter, cond *Cond) (bool, error) {
	var arr []any
	if !cond.Ok {
		arr = make([]any, 0, cond.size)
		arr = slices.AppendSeq(arr, cond.Val.(iter.Seq[any]))
		cond.Val = arr
		cond.Ok = true
	}

	var actual any
	var ok bool
	var err error
	for _, value := range values {
		if actual, ok = value.Get(); !ok {
			continue
		}
		if ok, err = structure.Contains(arr, actual, m.compare); err != nil || ok {
			return false, err
		}
	}
	return true, nil
}

func (m *Matcher) lt(values []domain.GetSetter, cond *Cond) (bool, error) {
	var c int
	var concrete any
	var err error
	var ok bool
	var arr []any
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					return false, nil
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, err
				}
				if c < 0 {
					return true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			return false, nil
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, err
		}
		if c < 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) gte(values []domain.GetSetter, cond *Cond) (bool, error) {
	var c int
	var concrete any
	var err error
	var ok bool
	var arr []any
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					return false, nil
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, err
				}
				if c >= 0 {
					return true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			return false, nil
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, err
		}
		if c >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) lte(values []domain.GetSetter, cond *Cond) (bool, error) {
	var c int
	var concrete any
	var err error
	var ok bool
	var arr []any
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					return false, nil
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, err
				}
				if c <= 0 {
					return true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			return false, nil
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, err
		}
		if c <= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) gt(values []domain.GetSetter, cond *Cond) (bool, error) {
	var c int
	var concrete any
	var err error
	var ok bool
	var arr []any
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					return false, nil
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, err
				}
				if c > 0 {
					return true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			return false, nil
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, err
		}
		if c > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) ne(values []domain.GetSetter, cond *Cond) (bool, error) {
	var c int
	var concrete any
	var err error
	var ok bool
	var arr []any
	for _, value := range values {
		if concrete, ok = m.getConcrete(value); !ok {
			continue
		}
		if arr, ok = concrete.([]any); ok {
			var item any
			for _, item = range arr {
				item, ok = m.getConcrete(item)
				if !ok {
					continue
				}
				if !m.comparer.Comparable(item, cond.Val) {
					return false, nil
				}
				c, err = m.comparer.Compare(item, cond.Val)
				if err != nil {
					return false, err
				}
				if c != 0 {
					return true, nil
				}
			}
		}
		if !m.comparer.Comparable(concrete, cond.Val) {
			return false, nil
		}
		c, err = m.comparer.Compare(concrete, cond.Val)
		if err != nil {
			return false, err
		}
		if c != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m *Matcher) in(values []domain.GetSetter, cond *Cond) (bool, error) {
	var arr []any
	if !cond.Ok {
		arr = make([]any, 0, cond.size)
		arr = slices.AppendSeq(arr, cond.Val.(iter.Seq[any]))
		cond.Val = arr
		cond.Ok = true
	}
	var actual any
	var ok bool
	var err error
	for _, value := range values {
		if actual, ok = value.Get(); !ok {
			continue
		}
		if ok, err = structure.Contains(arr, actual, m.compare); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func (m *Matcher) exists(values []domain.GetSetter, cond *Cond) (bool, error) {
	exists := false
	for _, value := range values {
		if _, ok := value.Get(); ok {
			exists = true
			break
		}
	}
	return exists == cond.Val.(bool), nil
}

func (m *Matcher) size(values []domain.GetSetter, expanded bool, cond *Cond) (bool, error) {
	size := cond.Val.(int)
	var actual any
	var ok bool
	var arr []any
	if expanded {
		return len(values) == size, nil
	}

	if actual, _ = values[0].Get(); actual == nil {
		return false, nil
	}

	if arr, ok = actual.([]any); !ok {
		return false, nil
	}

	return len(arr) == size, nil
}

func (m *Matcher) elemMatch(values []domain.GetSetter, cond *Cond) (bool, error) {
	query := cond.Val.(Query)
	var actual any
	var ok bool
	var arr []any
	var err error

	for _, value := range values {
		if actual, ok = value.Get(); !ok {
			continue
		}
		if arr, ok = actual.([]any); !ok {
			arr = arr[:0]
			arr = append(arr, actual)
		}
		for _, elem := range arr {
			ok, err = m.matchQuery(elem, query)
			if err != nil || ok {
				return ok, err
			}
		}
	}
	return false, nil
}
