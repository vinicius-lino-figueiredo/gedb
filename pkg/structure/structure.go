// Package structure TODO
package structure

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"regexp"
	"time"

	"github.com/goccy/go-reflect"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
)

var (
	// ErrNilObj TODO
	ErrNilObj = errors.New("nil object")
)

// ErrorNonObject TODO
type ErrorNonObject struct {
	Type reflect.Type
}

func (e ErrorNonObject) Error() string {
	return ""
}

// ErrorNonList TODO
type ErrorNonList struct {
	Type reflect.Type
}

func (e ErrorNonList) Error() string {
	return ""
}

// Seq2 TODO
func Seq2(obj any) (iter.Seq2[string, any], int, error) {
	if obj == nil {
		return nil, 0, ErrNilObj
	}
	if i, length, err := fastPathStruct(obj); err != nil || i != nil {
		return i, length, err
	}
	return nil, 0, fmt.Errorf("%w: cannot read with reflect yet", errors.ErrUnsupported)
}

func fastPathStruct(obj any) (iter.Seq2[string, any], int, error) {
	switch t := obj.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time, *regexp.Regexp, []byte:
		return nil, 0, ErrorNonObject{Type: reflect.TypeOf(obj)}
	case data.M:
		return iterMap(t), len(t), nil
	case map[string]any:
		return iterMap(t), len(t), nil
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
	case map[string]time.Time:
		return iterMap(t), len(t), nil
	case map[string]*regexp.Regexp:
		return iterMap(t), len(t), nil
	case map[string][]byte:
		return iterMap(t), len(t), nil
	default:
		return nil, 0, nil
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

// Seq TODO
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
	switch t := obj.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time, *regexp.Regexp:
		return nil, 0, ErrorNonList{Type: reflect.TypeOf(obj)}
	case []any:
		return iterSlice(t), len(t), nil
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
	case []time.Time:
		return iterSlice(t), len(t), nil
	case []*regexp.Regexp:
		return iterSlice(t), len(t), nil
	case [][]byte:
		return iterSlice(t), len(t), nil
	default:
		return nil, 0, nil
	}
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

// AsInteger TODO
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

// Contains TODO
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
