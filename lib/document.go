package lib

import (
	"fmt"
	"iter"
	"maps"
	"reflect"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// Document implements gedb.Document
type Document map[string]any

// NewDocument returns a new instance of gedb.Document.
func NewDocument(v any) (gedb.Document, error) {
	if v == nil {
		return nil, nil
	}

	d, err := evaluate(v)
	if err != nil {
		return nil, err
	}

	if doc, ok := d.(gedb.Document); ok {
		return doc, nil
	}

	return nil, fmt.Errorf("expected type struct or map, got %T", v)
}

func evaluate(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	val := reflect.ValueOf(&v).Elem().Elem()
	typ := val.Type()
	res := make(Document)
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("invalid map key type")
		}
		for _, key := range val.MapKeys() {
			m, err := evaluate(val.MapIndex(key).Interface())
			if err != nil {
				return nil, err
			}
			res[key.String()] = m
		}
		return res, nil
	}
	if typ.Kind() == reflect.Struct {
		if val.Type() == reflect.TypeFor[time.Time]() {
			return val.Interface(), nil
		}
		for numField := range val.NumField() {
			field := val.Field(numField)
			name := typ.Field(numField).Name
			fieldValue, err := evaluate(field.Interface())
			if err != nil {
				return nil, err
			}
			res[name] = fieldValue
		}
		return res, nil
	}
	return v, nil
}

// ID implements gedb.Document
func (d Document) ID() string {
	i := d["_id"]
	if s, ok := i.(string); ok {
		return s
	}
	return ""
}

// Get implements gedb.Document
func (d Document) Get(key string) any {
	return d[key]
}

// Set implements gedb.Document
func (d Document) Set(key string, value any) {
	d[key] = value
}

// D implements gedb.Document
func (d Document) D(key string) gedb.Document {
	r := d[key]
	if r == nil {
		return nil
	}
	if doc, ok := r.(gedb.Document); ok {
		return doc
	}
	return nil
}

// Iter implements gedb.Document.
func (d Document) Iter() iter.Seq2[string, any] {
	return maps.All(d)
}

// Keys implements gedb.Document.
func (d Document) Keys() iter.Seq[string] {
	return maps.Keys(d)
}

// Len implements gedb.Document.
func (d Document) Len() int {
	return len(d)
}

// Values implements gedb.Document.
func (d Document) Values() iter.Seq[any] {
	return maps.Values(d)
}

// Has implements gedb.Document.
func (d Document) Has(key string) bool {
	_, has := d[key]
	return has
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Document) UnmarshalJSON(input []byte) error {
	doc := &parser{data: input, n: len(input)}
	v, err := doc.parse()
	if err != nil {
		return err
	}
	obj, ok := v.(Document)
	if !ok {
		return fmt.Errorf("expected Document, received %T", v)
	}
	*d = obj
	return nil
}
