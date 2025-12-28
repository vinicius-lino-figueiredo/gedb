package binary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
	"unsafe"
)

const minSize = 512

func allocate() []byte {
	return make([]byte, 0, minSize)
}

// NewDoc TODO
func NewDoc(v any) (Object, error) {
	if v == nil {
		return allocate(), nil
	}
	if doc := hotPath(v); doc != nil {
		return doc, nil
	}
	return nil, fmt.Errorf("non simple map: %w", errors.ErrUnsupported)
}

func hotPath(v any) Object {
	switch t := v.(type) {
	case map[string]string:
		return primitiveMap(t, allocate())
	case map[string]bool:
		return primitiveMap(t, allocate())
	case map[string]int:
		return primitiveMap(t, allocate())
	case map[string]int8:
		return primitiveMap(t, allocate())
	case map[string]int16:
		return primitiveMap(t, allocate())
	case map[string]int32:
		return primitiveMap(t, allocate())
	case map[string]int64:
		return primitiveMap(t, allocate())
	case map[string]uint:
		return primitiveMap(t, allocate())
	case map[string]uint8:
		return primitiveMap(t, allocate())
	case map[string]uint16:
		return primitiveMap(t, allocate())
	case map[string]uint32:
		return primitiveMap(t, allocate())
	case map[string]uint64:
		return primitiveMap(t, allocate())
	case map[string]float32:
		return primitiveMap(t, allocate())
	case map[string]float64:
		return primitiveMap(t, allocate())
	}
	return nil
}

func primitiveMap[T any](m map[string]T, b []byte) Object {
	initial := len(b) + 1
	b = append(b, byte(Obj), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
	for k, v := range m {
		b = appendString(b, k)
		b = appendPrimitive(b, v)
	}
	binary.LittleEndian.PutUint64(b[initial:], uint64(len(b)-initial))
	return b
}

func appendString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(len(s)))
	return append(b, unsafe.Slice(unsafe.StringData(s), len(s))...)
}

func appendPrimitive(b []byte, p any) []byte {
	if num := appendNumber(b, p); num != nil {
		return num
	}
	switch t := p.(type) {
	case string:
		b = append(b, byte(String))
		return appendString(b, t)
	case *regexp.Regexp:
		b = append(b, byte(Regex))
		return appendString(b, t.String())
	case time.Time:
		b = append(b, byte(Time))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t.UnixMilli())))
	case []byte:
		b = append(b, byte(String))
		return appendString(b, string(t))
	case bool:
		b = append(b, byte(Boolean))
		if t {
			return append(b, 0b1)
		}
		return append(b, 0b0)
	}
	return b
}

func appendNumber(b []byte, p any) []byte {
	switch t := p.(type) {
	case int:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case int8:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case int16:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case int32:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case int64:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case uint:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case uint8:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case uint16:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case uint32:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case uint64:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case float32:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(t)))
	case float64:
		b = append(b, byte(Number))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(t))
	default:
		return nil
	}
}
