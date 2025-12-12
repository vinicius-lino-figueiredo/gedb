// Package modifier contains a [domain.Modifier] implementation to apply changes
// to a doc based on a mongo-like API.
package modifier

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var (
	// ErrMixedOperators is returned when user provides an update query with
	// mixed use of normal fields and dollar fields.
	ErrMixedOperators = errors.New("cannot mix modifiers and normal fields")
	// ErrNonObject is returned when a modifier value passed by user is not
	// an object.
	ErrNonObject = errors.New("modifier value must be an object")
	// ErrInvalidPushField is returned when user passes some field other
	// than $slice and $each when using $push modifier.
	ErrInvalidPushField = errors.New("can only use $slice in conjunction with $each when $push to array")
	// ErrInvalidAddToSetField is returned when user passes some field other
	// than $each when using $push modifier.
	ErrInvalidAddToSetField = errors.New("cannot use another field in conjunction with $each")
)

// ErrModFieldType is returned when a modification function runs on a document
// field of a type that is not accepted.
type ErrModFieldType struct {
	Mod    string
	Want   string
	Actual any
}

// Error implements [error].
func (e ErrModFieldType) Error() string {
	return fmt.Sprintf("%s expects %s field, got %T", e.Mod, e.Want, e.Actual)
}

// ErrModArgType is returned when a modification function is called with an
// argument of a type that is not accepted.
type ErrModArgType struct {
	Mod    string
	Want   string
	Actual any
}

// Error implements [error].
func (e ErrModArgType) Error() string {
	return fmt.Sprintf("%s expects %s arg, got %T", e.Mod, e.Want, e.Actual)
}

// ErrModQuery is returned when provided modification query does not match the
// general expected mongo-like structure.
type ErrModQuery struct {
	Reason string
}

// Error implements [error].
func (e ErrModQuery) Error() string {
	return fmt.Sprintf("invalid modification query: %s", e.Reason)
}

// ErrUnknownModifier is returned when the user specifies a modification query
// with a modification procedure that is not known by the current implementation
// of [Modifier].
type ErrUnknownModifier struct {
	Name string
}

// Error implements [error].
func (e ErrUnknownModifier) Error() string {
	return fmt.Sprintf("unknown modifier %q", e.Name)
}

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
	docFac         domain.DocumentFactory
	fieldNavigator domain.FieldNavigator
	matcher        domain.Matcher
	mods           map[string]modFunc
}

// NewModifier implements [domain.Modifier].
func NewModifier(docFac domain.DocumentFactory, comp domain.Comparer, fn domain.FieldNavigator, matcher domain.Matcher) domain.Modifier {
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
		"$min":      m.min,
	}

	return m
}

// Modify implements [domain.Modifier].
func (m *Modifier) Modify(obj domain.Document, mod domain.Document) (domain.Document, error) {
	modQry, replace, err := m.modQuery(obj, mod)
	if err != nil {
		return nil, err
	}

	if replace {
		return m.replaceMod(obj, modQry)
	}

	return m.dollarMod(obj, modQry)
}

func (m *Modifier) modQuery(obj domain.Document, mod domain.Document) (map[string]any, bool, error) {
	dollarFields, total := 0, 0

	query := make(map[string]any, mod.Len())
	for k, v := range mod.Iter() {
		total++
		if err := m.checkMod(obj, k, v); err != nil {
			return nil, false, err
		}
		if strings.HasPrefix(k, "$") {
			dollarFields++
		}
		if dollarFields != 0 && dollarFields != total {
			return nil, false, ErrMixedOperators
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
		return domain.ErrCannotModifyID
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
			return nil, ErrUnknownModifier{Name: modName}
		}
		d, ok := arg.(domain.Document)
		if !ok {
			return nil, ErrNonObject
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
				return nil, fmt.Errorf("modifying field %q: %w", key, err)
			}
		}
	}

	if obj.ID() != docCopy.ID() {
		return nil, domain.ErrCannotModifyID
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
		return ErrModArgType{Mod: "$inc", Want: "number", Actual: v}
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
			return ErrModFieldType{Mod: "$inc", Want: "number", Actual: value}
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
			return ErrModFieldType{Mod: "$push", Want: "array", Actual: value}
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
		return nil, ErrModArgType{Mod: "$each", Want: "array", Actual: each}
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
		return nil, fmt.Errorf("getting properties for $push: %w", err)
	}

	if d.Len() > props.usedFields {
		return nil, ErrInvalidPushField
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
			return ErrModFieldType{Mod: "$addToSet", Want: "array", Actual: value}
		}
		values := []any{v}
		if d, ok := v.(domain.Document); ok {
			props, err := m.getSliceProperties(d)
			if err != nil {
				return fmt.Errorf("getting properties for $addToSet: %w", err)
			}
			if props.hasEach && d.Len() > 1 {
				return ErrInvalidAddToSetField
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
		return ErrModArgType{Mod: "$pop", Want: "integer", Actual: v}
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
			return ErrModFieldType{Mod: "$pop", Want: "array", Actual: value}
		}

		start, end := 0, max(0, len(l)-1) // do not grow larger than l
		if num < 0 {
			// do not start after l ends
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
			return ErrModFieldType{Mod: "$pull", Want: "array", Actual: value}
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

func (m *Modifier) min(obj domain.Document, addr []string, v any) error {
	fields, err := m.fieldNavigator.EnsureField(obj, addr...)
	if err != nil {
		return err
	}

	for _, field := range fields {
		value, _ := field.Get()
		if value == nil {
			// if the value is new (created as nil by EnsureField)
			// or it was already nil before ensuring, we can replace
			// it either way
			field.Set(v)
			continue
		}
		comp, err := m.comp.Compare(value, v)
		if err != nil {
			return err
		}
		if comp > 0 {
			field.Set(v)
		}
	}

	return nil
}
