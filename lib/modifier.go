package lib

import (
	"fmt"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Modifier implements gedb.Modifier.
type Modifier struct {
	documentFactory func(any) (gedb.Document, error)
}

// NewModifier returns a new implementation of gedb.Modifier.
func NewModifier(documentFactory func(any) (gedb.Document, error)) gedb.Modifier {
	return &Modifier{
		documentFactory: documentFactory,
	}
}

func (m *Modifier) modifierFunc(mod string, obj gedb.Document, fields []string, value any) error {
	curr := obj
	last := len(fields) - 1
	for n, field := range fields {
		if n == last {
			return m.lastStepModifierFunc(mod, curr, field, value)
		}
		subObj := curr.D(field)
		if subObj == nil {
			newDoc, err := m.documentFactory(nil)
			if err != nil {
				return err
			}
			subObj = newDoc
		}
		curr = subObj
	}
	return nil
}

// Modify implements gedb.Modifier.
func (m *Modifier) Modify(obj gedb.Document, updateQuery gedb.Document) (gedb.Document, error) {

	var dollarFisrtChars, firstChars int
	modifiers := make(map[string]any, updateQuery.Len())
	for item, value := range updateQuery.Iter() {
		if item == "_id" {
			return nil, fmt.Errorf("you cannot change a document's _id")
		}
		firstChars++
		if strings.HasPrefix(item, "$") {
			dollarFisrtChars++
		}
		if dollarFisrtChars > 0 && dollarFisrtChars != firstChars {
			return nil, fmt.Errorf("you cannot mix modifiers and normal fields")
		}
		modifiers[item] = value
	}

	var newDoc gedb.Document
	var err error
	if dollarFisrtChars == 0 {
		if newDoc, err = m.deepCopy(updateQuery); err != nil {
			return nil, err
		}
		newDoc.Set("_id", obj.ID())
	} else {
		if newDoc, err = m.deepCopy(obj); err != nil {
			return nil, err
		}
		for mod, modifier := range modifiers {
			modDoc, ok := modifier.(gedb.Document)
			if !ok {
				return nil, fmt.Errorf("modifier %s's argument must be an object", mod)
			}
			for k := range modDoc.Iter() {
				if err := m.modifierFunc(mod, newDoc, strings.Split(k, "."), modDoc.Get(k)); err != nil {
					return nil, err
				}
			}
		}
	}
	// TODO: add obj check
	if obj.ID() != newDoc.ID() {
		return nil, fmt.Errorf("you can't change a document's _id")
	}
	return newDoc, nil
}

func (m *Modifier) lastStepModifierFunc(mod string, obj gedb.Document, field string, value any) error {
	switch mod {
	case "$set":
		obj.Set(field, value)
	case "$inc":
		valueNum, ok := asNumber(value)
		if !ok {
			return fmt.Errorf("%v must be a number", value)
		}
		if !obj.Has(field) {
			f, _ := valueNum.Float64()
			obj.Set(field, f)
			return nil
		}
		objValNum, ok := asNumber(obj.Get(field))
		if !ok {
			return fmt.Errorf("don't use the $inc modifier on non-number")
		}
		f, _ := objValNum.Add(objValNum, valueNum).Float64()
		obj.Set(field, f)
	default:
		return fmt.Errorf("unknown modifier %s", mod)
	}
	return nil
}

func (m *Modifier) deepCopy(doc gedb.Document) (gedb.Document, error) {
	res, err := m.documentFactory(nil)
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
	case gedb.Document:
		return m.deepCopy(t)
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
