package modifier

import (
	"fmt"
	"maps"
	"math/big"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

type modFunc func(domain.Document, []string, any) error

type sliceProps struct {
	each       []any
	hasEach    bool
	slice      int
	hasSlice   bool
	usedFields int
}

// Modifier implements [domain.Modifier].
type Modifier struct {
	comp           domain.Comparer
	docFac         func(any) (domain.Document, error)
	fieldNavigator domain.FieldNavigator
	matcher        domain.Matcher
	mods           map[string]modFunc
}

// NewModifier implements [domain.Modifier].
func NewModifier(docFac func(any) (domain.Document, error), comp domain.Comparer, fn domain.FieldNavigator, matcher domain.Matcher) domain.Modifier {
	m := &Modifier{
		comp:           comp,
		docFac:         docFac,
		fieldNavigator: fn,
		matcher:        matcher,
	}

	m.mods = map[string]modFunc{
		"$set":      m.set,
		"$unset":    m.unset,
		"$inc":      m.inc,
		"$push":     m.push,
		"$addToSet": m.addToSet,
		"$pop":      m.pop,
		"$pull":     m.pull,
		"$max":      m.max,
	}

	return m
}

// Modify implements [domain.Modifier].
func (m *Modifier) Modify(obj domain.Document, updateQuery domain.Document) (domain.Document, error) {
	modQry, replace, err := m.modQuery(obj, updateQuery)
	if err != nil {
		return nil, err
	}

	if replace {
		return m.replaceMod(obj, modQry)
	}

	return m.dollarMod(obj, modQry)
}

func (m *Modifier) modQuery(obj domain.Document, updateQuery domain.Document) (map[string]any, bool, error) {
	dollarFields, total := 0, 0

	query := make(map[string]any, updateQuery.Len())
	for k, v := range updateQuery.Iter() {
		total++
		if err := m.checkMod(obj, k, v); err != nil {
			return nil, false, err
		}
		if strings.HasPrefix(k, "$") {
			dollarFields++
		}
		if dollarFields != 0 && dollarFields != total {
			return nil, false, fmt.Errorf("you cannot mix modifiers and normal fields")
		}
		query[k] = v
	}
	return query, dollarFields == 0, nil
}

func (m *Modifier) checkMod(obj domain.Document, key string, value any) error {
	if key != "_id" {
		return nil
	}
	c, err := m.comp.Compare(value, obj.ID())
	if err != nil {
		return err
	}
	if c != 0 {
		return fmt.Errorf("you cannot change a document's _id")
	}
	return nil
}

func (m *Modifier) replaceMod(obj domain.Document, qry map[string]any) (domain.Document, error) {
	newDoc, err := m.docFac(nil)
	if err != nil {
		return nil, err
	}

	for k, v := range qry {
		newDoc.Set(k, v)
	}

	newDoc.Set("_id", obj.ID())

	return newDoc, nil
}

func (m *Modifier) dollarMod(obj domain.Document, qry map[string]any) (domain.Document, error) {

	type modCall struct {
		fn   modFunc
		args map[string]any
	}

	calls := make(map[string]modCall, len(qry))

	for modName, arg := range qry {
		mod, ok := m.mods[modName]
		if !ok {
			return nil, fmt.Errorf("unknown modifier %s", modName)
		}
		d, ok := arg.(domain.Document)
		if !ok {
			return nil, fmt.Errorf("Modifier %s's argument must be an object", modName)
		}

		calls[modName] = modCall{
			fn:   mod,
			args: maps.Collect(d.Iter()),
		}
	}

	docCopy, err := m.copyDoc(obj)
	if err != nil {
		return nil, err
	}

	for _, call := range calls {
		for key, arg := range call.args {
			addr, err := m.fieldNavigator.GetAddress(key)
			if err != nil {
				return nil, err
			}
			if err := call.fn(docCopy, addr, arg); err != nil {
				return nil, err
			}
		}
	}

	if obj.ID() != docCopy.ID() {
		return nil, fmt.Errorf("you can't change a document's _id")
	}

	return docCopy, nil
}

func (m *Modifier) copyDoc(doc domain.Document) (domain.Document, error) {
	res, err := m.docFac(nil)
	if err != nil {
		return nil, err
	}

	for k, v := range doc.Iter() {
		if strings.HasPrefix(k, "$") {
			continue
		}
		copied, err := m.copyAny(v)
		if err != nil {
			return nil, err
		}
		res.Set(k, copied)
	}
	return res, nil
}

func (m *Modifier) copyAny(v any) (any, error) {
	switch t := v.(type) {
	case domain.Document:
		return m.copyDoc(t)
	case []any:
		newList := make([]any, len(t))
		for n, itm := range t {
			newV, err := m.copyAny(itm)
			if err != nil {
				return nil, err
			}
			newList[n] = newV
		}
		return newList, nil
	default:
		return v, nil
	}
}

func (m *Modifier) asNumber(v any) (*big.Float, bool) {
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

func (m *Modifier) set(obj domain.Document, addr []string, arg any) error {
	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if _, defined := field.Get(); defined {
			field.Set(arg)
		}
	}
	return nil
}

func (m *Modifier) unset(obj domain.Document, addr []string, _ any) error {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if _, defined := field.Get(); defined {
			field.Unset()
		}
	}
	return nil
}

func (m *Modifier) inc(obj domain.Document, addr []string, v any) error {
	incNum, ok := m.asNumber(v)
	if !ok {
		return fmt.Errorf("%v must be a number", v)
	}
	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}
	for _, field := range fields {
		value, defined := field.Get()
		if !defined {
			continue
		}
		if value == nil { // nil can be incremented too
			value = 0.0
		}
		num, ok := m.asNumber(value)
		if !ok {
			return fmt.Errorf("Don't use the $inc modifier on non-number fields")
		}
		sum := num.Add(num, incNum)
		sumFloat, _ := sum.Float64()
		field.Set(sumFloat)
	}
	return nil
}

func (m *Modifier) push(obj domain.Document, addr []string, v any) error {

	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}
	for _, field := range fields {
		value, defined := field.Get()
		if !defined {
			continue
		}
		if value == nil {
			value = []any{}
		}
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("Can't $push an element on non-array values")
		}

		values := append(array, v)
		if d, ok := v.(domain.Document); ok {
			values, err = m.getPushItems(d, array)
			if err != nil {
				return err
			}
		}

		field.Set(values)
	}
	return nil
}

func (m *Modifier) getSliceProperties(d domain.Document) (*sliceProps, error) {

	res := &sliceProps{
		hasEach:  d.Has("$each"),
		hasSlice: d.Has("$slice"),
	}

	var each any = []any{d}
	if res.hasEach {
		res.usedFields++
		res.hasEach = true
		each = d.Get("$each")
	}

	var ok bool
	if res.each, ok = each.([]any); !ok {
		return nil, fmt.Errorf("$each requires an array value")
	}

	if s, ok := m.asNumber(d.Get("$slice")); ok && s.IsInt() {
		res.usedFields++
		s, _ := s.Int64()
		res = &sliceProps{
			each:       res.each,
			hasEach:    res.hasEach,
			slice:      int(s),
			hasSlice:   true,
			usedFields: res.usedFields,
		}
	}

	return res, nil
}

func (m *Modifier) getPushItems(d domain.Document, array []any) ([]any, error) {
	props, err := m.getSliceProperties(d)
	if err != nil {
		return nil, err
	}

	if d.Len() > props.usedFields {
		return nil, fmt.Errorf("Can only use $slice in cunjunction with $each when $push to array")
	}

	res := append(array, props.each...)

	if !props.hasSlice {
		return res, nil
	}

	if props.slice >= 0 {
		return res[:min(props.slice, len(res))], nil
	}

	slice := max(props.slice, -len(res))

	return res[len(res)+slice:], nil
}

func (m *Modifier) addToSet(obj domain.Document, addr []string, v any) error {
	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}

	for _, field := range fields {
		value, defined := field.Get()
		if !defined {
			continue
		}
		if value == nil {
			value = []any{}
		}
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("Can't $addToSet an element on non-array values")
		}
		values := []any{v}
		if d, ok := v.(domain.Document); ok {
			props, err := m.getSliceProperties(d)
			if err != nil {
				return err
			}
			if props.hasEach && d.Len() > 1 {
				return fmt.Errorf("Can't use another field in conjunction with $each")
			}
			values = props.each
		}

		for _, value := range values {
			shouldAdd := true
			for _, item := range array {
				c, err := m.comp.Compare(value, item)
				if err != nil {
					return err
				}
				if c == 0 {
					shouldAdd = false
					break
				}
			}
			if shouldAdd {
				array = append(array, value)
			}
		}
		field.Set(array)
	}

	return nil
}

func (m *Modifier) pop(obj domain.Document, addr []string, v any) error {

	bigN, ok := m.asNumber(v)
	if !ok || !bigN.IsInt() {
		return fmt.Errorf("%v isn't an integer, can't use it with $pop", v)
	}

	num64, _ := bigN.Int64()

	if num64 == 0 {
		return nil
	}

	num := int(num64)

	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return err
	}

	for _, field := range fields {
		value, _ := field.Get()

		// not checking defined because unset fields should fail too

		l, ok := value.([]any)
		if !ok {
			return fmt.Errorf("Can't $pop an element from non-array values")
		}

		start, end := 0, max(0, len(l)-1) // do not grow larger than l
		if num < 0 {
			// do not start after l end s
			start, end = min(1, len(l)), len(l)
		}

		field.Set(l[start:end])
	}
	return nil
}

func (m *Modifier) pull(obj domain.Document, addr []string, v any) error {
	fields, _, err := m.fieldNavigator.GetField(obj, addr...)
	if err != nil {
		return err
	}

	for _, field := range fields {
		value, _ := field.Get()

		l, ok := value.([]any)
		if !ok {
			return fmt.Errorf("Can't $pop an element from non-array values")
		}

		res := make([]any, 0, len(l))
		for _, item := range l {
			matches, err := m.matcher.Match(item, v)
			if err != nil {
				return err
			}
			if !matches {
				res = append(res, item)
			}
		}
		field.Set(res)

	}
	return nil
}

func (m *Modifier) max(obj domain.Document, addr []string, v any) error {
	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}

	for _, field := range fields {
		comp, err := m.comp.Compare(field, v)
		if err != nil {
			return err
		}
		if comp < 0 {
			field.Set(v)
		}
	}

	return nil
}
