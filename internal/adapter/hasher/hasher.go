// Package hasher contains a json based implementation of [domain.Hasher]. Since
// GEDB is based on JSON and it's only used internally after unmarshaling data
// with [domain.Deserializer] or decoding with a document factory function, it
// is prepared to hash documents, arrays, primitive values and nothing else.
// Data types that cannot be marshaled, like channels and functions will still
// be valid, but their hashes will be calculated as if they were any(nil).
package hasher

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"reflect"
	"slices"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Hasher implements [domain.Hasher].
type Hasher struct{}

// NewHasher returns a new implementation of [domain.Hasher].
func NewHasher() domain.Hasher {
	return &Hasher{}
}

// Hash implements domain.Hasher.
func (h *Hasher) Hash(value any) (uint64, error) {
	canonical := h.canonicalize(value)

	b, err := json.Marshal(canonical)
	if err != nil {
		return 0, err
	}

	hasher := fnv.New64a()

	_, _ = hasher.Write(b) // fnv.sum64a.Write never returns error

	return hasher.Sum64(), nil
}

func (h *Hasher) canonicalize(a any) any {
	if h.straightforward(a) {
		return a
	}

	if f, ok := h.fields(a); ok {
		return f
	}

	if i, ok := h.items(a); ok {
		return i
	}

	v := reflect.ValueOf(a)
	if v.IsValid() {
		kind := v.Kind()
		if kind == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			return v.Pointer()
		}
		if kind == reflect.Chan || kind == reflect.Func {
			if v.IsNil() {
				return nil
			}
			return v.Pointer()
		}
	}
	return a
}

func (h *Hasher) fields(a any) (object, bool) {
	if d, ok := a.(domain.Document); ok {
		pairs := make(object, d.Len())
		var n int
		for k, v := range d.Iter() {
			pairs[n] = keyValuePair{key: k, val: h.canonicalize(v)}
			n++
		}
		return pairs, true
	}
	return nil, false
}

func (h *Hasher) items(a any) ([]any, bool) {
	if arr, ok := a.([]any); ok {
		res := make([]any, len(arr))
		for n, v := range arr {
			res[n] = h.canonicalize(v)
		}
		return res, true
	}
	return nil, false
}

func (h *Hasher) straightforward(a any) bool {
	if a == nil {
		return true
	}
	switch a.(type) {
	case
		// simple values
		bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time:
		return true
	default:
		return false
	}
}

type keyValuePair struct {
	key string
	val any
}

type object []keyValuePair

func (o object) MarshalJSON() (r []byte, err error) {
	buf := bytes.NewBuffer(append(make([]byte, 0, 1024), '{'))

	keys := make([]string, len(o))
	kvals := make(map[string]any, len(o))

	for n, item := range o {
		keys[n] = item.key
		kvals[item.key] = item.val
	}
	slices.Sort(keys)

	for n, key := range keys {
		b, _ := json.Marshal(key)
		_, _ = buf.Write(b)
		_, _ = buf.WriteRune(':')
		v, err := json.Marshal(kvals[key])
		if err != nil {
			return nil, err
		}
		_, _ = buf.Write(v)

		if n < len(keys)-1 {
			_, _ = buf.WriteRune(',')
		}
	}
	_, _ = buf.WriteRune('}')

	return buf.Bytes(), nil

}
