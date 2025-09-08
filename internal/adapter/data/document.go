package data

import (
	"fmt"
	"iter"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	goreflect "github.com/goccy/go-reflect"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

const TagName = "gedb"

var (
	timeTyp = goreflect.TypeOf(*new(time.Time))
)

// M implements domain.Document by using a hashed map. Duplicates replace old
// values.
type M map[string]any

// NewDocument returns a new instance of [domain.Document].
func NewDocument(in any) (domain.Document, error) {
	if in == nil {
		return M{}, nil
	}
	if doc, err := parseSimple(in); err != nil && doc == nil {
		return doc, err
	}

	r := goreflect.ValueNoEscapeOf(in)
	k := r.Kind()
	for k == goreflect.Interface || k == reflect.Pointer {
		if r.IsNil() {
			return M{}, nil
		}
		r = r.Elem()
		k = r.Kind()
	}
	if k != goreflect.Struct && k != goreflect.Map {
		return nil, fmt.Errorf("expected map or struct, got %s", r.Type().String())
	}
	doc, err := parseReflect(r)
	if err != nil {
		return nil, err
	}
	return doc.(domain.Document), nil
}

func parseSimple(v any) (domain.Document, error) {
	switch t := v.(type) {
	case map[string]any:
		return parseMap(t), nil
	case map[string]string:
		return parseMap(t), nil
	case map[string]bool:
		return parseMap(t), nil
	case map[string]int:
		return parseMap(t), nil
	case map[string]int8:
		return parseMap(t), nil
	case map[string]int16:
		return parseMap(t), nil
	case map[string]int32:
		return parseMap(t), nil
	case map[string]int64:
		return parseMap(t), nil
	case map[string]uint:
		return parseMap(t), nil
	case map[string]uint8:
		return parseMap(t), nil
	case map[string]uint16:
		return parseMap(t), nil
	case map[string]uint32:
		return parseMap(t), nil
	case map[string]uint64:
		return parseMap(t), nil
	case map[string]float32:
		return parseMap(t), nil
	case map[string]float64:
		return parseMap(t), nil
	case map[string]time.Time:
		return parseMap(t), nil
	case map[string]time.Duration:
		return parseMap(t), nil
	default:
		return nil, nil
	}
}

func parseMap[T any](v map[string]T) domain.Document {
	res := make(M, len(v))
	for k, v := range v {
		res[k] = v
	}
	return res
}

func parseReflect(r goreflect.Value) (any, error) {
	for r.Kind() == reflect.Pointer || r.Kind() == goreflect.Interface {
		r = r.Elem()
	}
	switch r.Kind() {
	case goreflect.Invalid:
		return nil, nil
	case goreflect.Slice:
		if r.IsNil() {
			return nil, nil
		}
		fallthrough
	case goreflect.Array:
		return parseList(r), nil
	case goreflect.Struct:
		if r.Type() == timeTyp {
			return r.Interface(), nil
		}
		return parseStruct(r)
	case goreflect.Map:
		if r.IsNil() {
			return nil, nil
		}
		return parseMapReflect(r)
	case goreflect.Chan, goreflect.Func, goreflect.Interface:
		if r.IsNil() {
			return nil, nil
		}
		return r.Interface(), nil
	default:
		return r.Interface(), nil
	}
}

func parseStruct(r goreflect.Value) (domain.Document, error) {
	typ := r.Type()
	numField := r.NumField()

	res := make(M, numField)

	for n := range numField {
		field := typ.Field(n)
		if field.PkgPath != "" {
			continue
		}
		fieldValue := r.Field(n)

		fieldInfo, err := parseField(fieldValue, field)
		if err != nil {
			return nil, err
		}

		if fieldInfo == nil {
			continue
		}
		res[fieldInfo.name] = fieldInfo.value
	}
	return res, nil
}

func parseMapReflect(v goreflect.Value) (domain.Document, error) {
	res := make(M, v.Len())
	for _, k := range v.MapKeys() {
		str := k.String()
		var err error
		if res[str], err = parseReflect(v.MapIndex(k)); err != nil {
			return nil, err
		}
	}
	return res, nil
}

type field struct {
	name  string
	value any
}

func parseField(r goreflect.Value, typ goreflect.StructField) (*field, error) {
	name := typ.Name
	var tagSegments []string
	if tag, ok := typ.Tag.Lookup(TagName); ok {
		if tag == "-" {
			return nil, nil
		}
		tagSegments = strings.Split(tag, ",")
		if tagSegments[0] != "" {
			name = tagSegments[0]
		}
		tagSegments = tagSegments[1:]
	}
	if slices.Contains(tagSegments, "omitempty") && isNullable(typ.Type) && r.IsNil() {
		return nil, nil
	}
	if slices.Contains(tagSegments, "omitzero") && r.IsZero() {
		return nil, nil
	}

	value, err := parseReflect(r)
	if err != nil {
		return nil, err
	}

	return &field{name: name, value: value}, nil
}

func parseList(r goreflect.Value) any {
	length := r.Len()
	res := make([]any, length)
	for i := range length {
		res[i] = r.Index(i).Interface()
	}
	return res
}

func isNullable(t goreflect.Type) bool {
	k := t.Kind()
	return k == reflect.Pointer ||
		k == reflect.Slice ||
		k == reflect.Map ||
		k == reflect.Interface ||
		// WARN: these might be removed later
		k == reflect.Func ||
		k == reflect.Chan
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

// Unset implements domain.Document
func (d M) Unset(key string) {
	delete(d, key)
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
