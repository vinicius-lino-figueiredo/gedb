// Package structure contains type-related operations, such as iterating over a
// value of type any and converting numbers.
package structure

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-reflect"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var (
	// ErrNilObj may be returned by [Seq] or [Seq2] when a nil value is
	// passed as argument.
	ErrNilObj = errors.New("nil object")
)

var docReflectType = reflect.TypeOf((*domain.Document)(nil)).Elem()

// ErrorNonObject is returned by [Seq2] when a value that is neither a struct,
// map nor a [domain.Document] is passed as argument.
type ErrorNonObject struct {
	Type reflect.Type
}

func (e ErrorNonObject) Error() string {
	return ""
}

// ErrorNonList is returned by [Seq] when a value that is neither a slice
// nor a array is passed as argument.
type ErrorNonList struct {
	Type reflect.Type
}

func (e ErrorNonList) Error() string {
	return ""
}

// Seq2 returns an iterator over the passed type. This method works for maps
// and implementations of [domain.Document].
func Seq2(obj any) (iter.Seq2[string, any], int, error) {
	if obj == nil {
		return nil, 0, ErrNilObj
	}
	if i, length, err := fastPathStruct(obj); err != nil || i != nil {
		return i, length, err
	}
	return iterReflect(obj)
}

func fastPathStruct(obj any) (iter.Seq2[string, any], int, error) {
	if err := checkPrimitive(obj); err != nil {
		return nil, 0, err
	}
	return checkMaps(obj)
}

func checkPrimitive(obj any) error {
	switch obj.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time, *regexp.Regexp, []byte:
		return ErrorNonObject{Type: reflect.TypeOf(obj)}
	default:
		return nil
	}
}

func checkMaps(obj any) (iter.Seq2[string, any], int, error) {
	switch t := obj.(type) {
	case map[string]string:
		return iterMap(t), len(t), nil
	case map[string]bool:
		return iterMap(t), len(t), nil
	case map[string]int:
		return iterMap(t), len(t), nil
	case map[string]int8:
		return iterMap(t), len(t), nil
	case map[string]int16:
		return iterMap(t), len(t), nil
	case map[string]int32:
		return iterMap(t), len(t), nil
	case map[string]int64:
		return iterMap(t), len(t), nil
	case map[string]uint:
		return iterMap(t), len(t), nil
	case map[string]uint8:
		return iterMap(t), len(t), nil
	case map[string]uint16:
		return iterMap(t), len(t), nil
	case map[string]uint32:
		return iterMap(t), len(t), nil
	case map[string]uint64:
		return iterMap(t), len(t), nil
	case map[string]float32:
		return iterMap(t), len(t), nil
	case map[string]float64:
		return iterMap(t), len(t), nil
	}
	return checkComplexMaps(obj)
}

func checkComplexMaps(obj any) (iter.Seq2[string, any], int, error) {
	switch t := obj.(type) {
	case domain.Document:
		return t.Iter(), t.Len(), nil
	case map[string]any:
		return iterMap(t), len(t), nil
	case map[string]time.Time:
		return iterMap(t), len(t), nil
	case map[string]*regexp.Regexp:
		return iterMap(t), len(t), nil
	case map[string][]byte:
		return iterMap(t), len(t), nil
	}
	return nil, 0, nil
}

func iterReflect(obj any) (iter.Seq2[string, any], int, error) {
	v := reflect.ValueNoEscapeOf(obj)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, 0, ErrNilObj
		}
		v = v.Elem()
	}

	if v.Type().Implements(docReflectType) {
		doc := v.Interface().(domain.Document)
		return doc.Iter(), doc.Len(), nil
	}

	switch v.Kind() {
	case reflect.Map:
	case reflect.Struct:
		i, l := iterReflectStruct(v)
		return i, l, nil
	}
	return nil, 0, ErrorNonObject{Type: v.Type()}
}

func iterReflectStruct(v reflect.Value) (iter.Seq2[string, any], int) {
	fields := make([]struct {
		Key   string
		Value any
	}, 0, v.NumField())
	for k, v := range listStructFields(v) {
		fields = append(fields, struct {
			Key   string
			Value any
		}{Key: k, Value: v})
	}
	return func(yield func(string, any) bool) {
		for _, field := range fields {
			if !yield(field.Key, field.Value) {
				return
			}
		}
	}, len(fields)
}

func listStructFields(v reflect.Value) iter.Seq2[string, any] {
	var tag string
	var ok bool
	var field reflect.StructField
	var omitEmpty bool
	var omitZero bool
	return func(yield func(string, any) bool) {
		typ := v.Type()
		for n := range typ.NumField() {
			omitEmpty, omitZero = false, false
			field = typ.Field(n)

			if field.PkgPath != "" {
				continue
			}

			if tag, ok = field.Tag.Lookup("gedb"); ok {
				found := strings.IndexRune(tag, ',')
				if found >= 0 {
					for sub := range strings.SplitSeq(tag[found:], ",") {
						switch sub {
						case "omitEmpty":
							omitEmpty = true
						case "omitZero":
							omitZero = true
						}
					}
					if tag = tag[:found]; tag == "" {
						tag = field.Name
					}
				}

			} else {
				tag = field.Name
			}
			switch {
			case omitZero:
				if v.Field(n).IsZero() {
					continue
				}
			case omitEmpty:
				switch field.Type.Kind() {
				case reflect.Chan, reflect.Func, reflect.Map,
					reflect.Ptr, reflect.UnsafePointer,
					reflect.Interface, reflect.Slice:
					if v.Field(n).IsNil() {
						continue
					}
				}
			}
			if !yield(tag, v.Field(n).Interface()) {
				return
			}
		}
	}
}

func iterMap[T any](m map[string]T) iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for k, v := range m {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Seq returns an iterator over a slice or array of any type.
func Seq(obj any) (iter.Seq[any], int, error) {
	if obj == nil {
		return nil, 0, ErrNilObj
	}
	if i, length, err := fastPathList(obj); err != nil || i != nil {
		return i, length, err
	}
	return nil, 0, fmt.Errorf("%w: cannot read with reflect yet", errors.ErrUnsupported)
}

func fastPathList(obj any) (iter.Seq[any], int, error) {
	if err := checkPrimitive(obj); err != nil {
		return nil, 0, err
	}
	return checkLists(obj)
}

func checkLists(obj any) (iter.Seq[any], int, error) {
	switch t := obj.(type) {
	case []string:
		return iterSlice(t), len(t), nil
	case []bool:
		return iterSlice(t), len(t), nil
	case []int:
		return iterSlice(t), len(t), nil
	case []int8:
		return iterSlice(t), len(t), nil
	case []int16:
		return iterSlice(t), len(t), nil
	case []int32:
		return iterSlice(t), len(t), nil
	case []int64:
		return iterSlice(t), len(t), nil
	case []uint:
		return iterSlice(t), len(t), nil
	case []uint8:
		return iterSlice(t), len(t), nil
	case []uint16:
		return iterSlice(t), len(t), nil
	case []uint32:
		return iterSlice(t), len(t), nil
	case []uint64:
		return iterSlice(t), len(t), nil
	case []float32:
		return iterSlice(t), len(t), nil
	case []float64:
		return iterSlice(t), len(t), nil
	}
	return checkComplexLists(obj)
}

func checkComplexLists(obj any) (iter.Seq[any], int, error) {
	switch t := obj.(type) {
	case []any:
		return iterSlice(t), len(t), nil
	case []time.Time:
		return iterSlice(t), len(t), nil
	case []*regexp.Regexp:
		return iterSlice(t), len(t), nil
	case [][]byte:
		return iterSlice(t), len(t), nil
	}
	return nil, 0, nil
}

func iterSlice[T any](m []T) iter.Seq[any] {
	return func(yield func(any) bool) {
		for _, v := range m {
			if !yield(v) {
				return
			}
		}
	}
}

// AsInteger converts any built-in number to int and returns a flag that informs
// if the argument is a valid integer.
func AsInteger(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int8:
		return int(t), true
	case int16:
		return int(t), true
	case int32:
		return int(t), true
	case int64:
		return int(t), true
	case uint:
		return int(t), true
	case uint8:
		return int(t), true
	case uint16:
		return int(t), true
	case uint32:
		return int(t), true
	case uint64:
		return int(t), true
	case float32:
		if trunc := math.Trunc(float64(t)); trunc == float64(t) {
			return int(trunc), true
		}
		return 0, false
	case float64:
		if trunc := math.Trunc(t); trunc == t {
			return int(trunc), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// Contains checks if the given value is present in the slice.
func Contains[T any, S ~[]T](s S, t T, fn func(a T, b T) (bool, error)) (bool, error) {
	var ok bool
	var err error
	for _, i := range s {
		if ok, err = fn(i, t); err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}
