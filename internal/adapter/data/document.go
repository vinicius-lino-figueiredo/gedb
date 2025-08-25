package data

import (
	"fmt"
	"iter"
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// M implements domain.Document by using a hashed map. Duplicates replace old
// values.
type M map[string]any

// NewDocument returns a new instance of [domain.Document].
func NewDocument(v any) (domain.Document, error) {
	if v == nil {
		return M{}, nil
	}

	d, err := evaluate(v)
	if err != nil {
		return nil, err
	}

	if doc, ok := d.(domain.Document); ok {
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
	res := make(M)
	for {
		if typ.Kind() != reflect.Pointer {
			break
		}
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
		typ = typ.Elem()
	}
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
	Fields:
		for numField := range val.NumField() {
			field := val.Field(numField)

			var name string
			if tag, ok := typ.Field(numField).Tag.Lookup("gedb"); ok {
				if tag == "-" {
					continue
				}
				parts := strings.Split(tag, ",")
				if len(parts) > 0 {
					if parts[0] == "" {
						name = typ.Field(numField).Name
					} else {
						name = parts[0]
					}
					for _, flag := range parts[1:] {
						if flag == "omitempty" && field.IsNil() {
							continue Fields
						}
						if flag == "omitzero" && field.IsZero() {
							continue Fields
						}
					}
				}
			} else {
				name = typ.Field(numField).Name
			}

			fieldValue, err := evaluate(field.Interface())
			if err != nil {
				return nil, err
			}
			res[name] = fieldValue
		}
		return res, nil
	}

	if typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		list := make([]any, val.Len())
		for i := range val.Len() {
			item := val.Index(i).Interface()
			item, err := evaluate(item)
			if err != nil {
				return nil, err
			}
			list[i] = item
		}
		return list, nil
	}

	return v, nil
}

// ID implements domain.Document
func (d M) ID() any {
	return d["_id"]
}

// Get implements domain.Document
func (d M) Get(key string) any {
	return d[key]
}

// Set implements domain.Document
func (d M) Set(key string, value any) {
	d[key] = value
}

// D implements domain.Document
func (d M) D(key string) domain.Document {
	r := d[key]
	if r == nil {
		return nil
	}
	if doc, ok := r.(domain.Document); ok {
		return doc
	}
	return nil
}

// Iter implements domain.Document.
func (d M) Iter() iter.Seq2[string, any] {
	return maps.All(d)
}

// Keys implements domain.Document.
func (d M) Keys() iter.Seq[string] {
	return maps.Keys(d)
}

// Len implements domain.Document.
func (d M) Len() int {
	return len(d)
}

// Values implements domain.Document.
func (d M) Values() iter.Seq[any] {
	return maps.Values(d)
}

// Has implements domain.Document.
func (d M) Has(key string) bool {
	_, has := d[key]
	return has
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *M) UnmarshalJSON(input []byte) error {
	doc := &parser{data: input, n: len(input)}
	v, err := doc.parse()
	if err != nil {
		return err
	}
	obj, ok := v.(M)
	if !ok {
		return fmt.Errorf("expected Document, received %T", v)
	}
	*d = obj
	return nil
}
