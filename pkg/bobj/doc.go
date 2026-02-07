package bobj

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"
	"unsafe"

	"github.com/goccy/go-reflect"
)

const (
	defaultSize = 500
	tagName     = "gedb"
)

var (
	reflectString    = reflect.TypeOf(*new(string))
	reflectMarshaler = reflect.TypeOf((*GEDBMarshaler)(nil)).Elem()
)

var (
	// ErrNonStringKey TODO
	ErrNonStringKey = errors.New("non string key type map")
	// ErrNonObject TODO
	ErrNonObject = errors.New("non string key type map")
	// ErrInvalid TODO
	ErrInvalid = errors.New("")
)

var structCache = sync.Map{}

// GEDBMarshaler FIXME
type GEDBMarshaler interface {
	MarshalGEDB() (Value, error)
}

// NewDocument TODO
func NewDocument(in any) (Object, error) {

	if in == nil {
		return makeDoc(defaultSize), nil
	}

	doc, shouldReturn, err := fastPath(in)
	if shouldReturn {
		return doc, err
	}

	value := reflect.ValueOf(in)

	return readObject(value)

}

type returnTo struct {
	obj  reflect.Value
	kind reflect.Kind
	idx  int
	keys []reflect.Value
	// fields []field
	typ     reflect.Type
	length  int
	sizeIdx int
}

type field struct {
	Name  string
	Index int
}

func readObject(in reflect.Value) (Object, error) {

Loop:
	for {
		if in.Type().Implements(reflectMarshaler) {
			v, err := in.Interface().(GEDBMarshaler).MarshalGEDB()
			if err != nil {
				return nil, err
			}
			if obj, ok := v.AsObject(); ok {
				return obj, nil
			}
			return nil, ErrNonObject
		}

		switch in.Kind() {
		case reflect.UnsafePointer, reflect.Ptr, reflect.Interface:
			if in.IsNil() {
				return makeDoc(defaultSize), nil
			}
			in = in.Elem()
		case reflect.Map:
			if in.Type().Key() != reflectString {
				return nil, ErrNonStringKey
			}
			fallthrough
		default:
			break Loop
		}
	}

	retStack := make([]returnTo, 0, 20)

	retStack = stack(retStack, 1, in)

	doc := makeDoc(defaultSize)

	var itm returnTo
	var size int
	var key reflect.Value
	var fields []field
	var err error
	var f field
ReadLoop:
	for {
		itm = retStack[len(retStack)-1]

		if itm.obj.Type().Implements(reflectMarshaler) {
			v, err := itm.obj.Interface().(GEDBMarshaler).MarshalGEDB()
			if err != nil {
				return nil, err
			}
			return writeBytes(doc, v), nil

		}

		switch itm.kind {
		case reflect.UnsafePointer, reflect.Ptr, reflect.Interface:
			if !itm.obj.IsNil() {
				retStack[len(retStack)-1].obj = itm.obj.Elem()
				retStack[len(retStack)-1].kind = retStack[len(retStack)-1].obj.Kind()
				continue
			}
			doc = writeNil(doc)
		case reflect.Struct:
			fields = getFields(itm.typ)

			for n := itm.idx; n < len(fields); n++ {
				f = fields[n]

				doc = writeString(doc, f.Name)

				val := itm.obj.Field(f.Index)

				doc, size, err = writeReflect(doc, val)
				if err != nil {
					return nil, err
				}
				if size >= 0 {
					retStack[len(retStack)-1].idx = n + 1
					retStack = stack(retStack, size, val)
					continue ReadLoop
				}
			}

			size = len(doc) - itm.sizeIdx + sizeOfSize

			binary.LittleEndian.PutUint64(doc[itm.sizeIdx:], uint64(size))

		case reflect.Map:
			for n := itm.idx; n < len(itm.keys); n++ {
				key = itm.keys[n]
				doc = writeString(doc, key.String())

				val := itm.obj.MapIndex(key)
				doc, size, err = writeReflect(doc, val)
				if err != nil {
					return nil, err
				}
				if size >= 0 {
					retStack[len(retStack)-1].idx = n + 1
					retStack = stack(retStack, size, val)
					continue ReadLoop
				}
			}

			size = len(doc) - itm.sizeIdx - sizeOfLen

			binary.LittleEndian.PutUint64(doc[itm.sizeIdx:], uint64(size))
			binary.LittleEndian.PutUint64(doc[itm.sizeIdx+sizeOfSize:], uint64(itm.length))
		case reflect.Array, reflect.Slice:
			for n := itm.idx; n < itm.length; n++ {
				val := itm.obj.Index(n)
				doc, size, err = writeReflect(doc, val)
				if err != nil {
					return nil, err
				}
				if size >= 0 {
					retStack[len(retStack)-1].idx = n + 1
					retStack = stack(retStack, size, val)
					continue ReadLoop
				}
			}

			size = len(doc) - itm.sizeIdx - sizeOfLen

			binary.LittleEndian.PutUint64(doc[itm.sizeIdx:], uint64(size))
			binary.LittleEndian.PutUint64(doc[itm.sizeIdx+sizeOfSize:], uint64(itm.length))
		}
		retStack = retStack[:len(retStack)-1]
		if len(retStack) == 0 {
			return doc, nil
		}
	}
}

func stack(r []returnTo, idx int, v reflect.Value) []returnTo {

	rt := returnTo{
		obj:     v,
		kind:    v.Kind(),
		typ:     v.Type(),
		sizeIdx: idx,
	}

	switch rt.kind {
	case reflect.Struct:
	case reflect.Map:
		rt.keys = v.MapKeys()
		slices.SortFunc(rt.keys, func(a, b reflect.Value) int {
			return compareString(a.String(), b.String())
		})
	case reflect.Array, reflect.Slice:
		rt.length = v.Len()
	}

	return append(r, rt)
}

func getFields(typ reflect.Type) []field {
	f, ok := structCache.Load(typ)
	if ok {
		return f.([]field)
	}

	nf := typ.NumField()

	fields := make([]field, 0, nf)
	var sf reflect.StructField
	for n := range nf {
		sf = typ.Field(n)

		if sf.PkgPath != "" {
			continue
		}

		f := field{Name: sf.Name, Index: n}

		if f.Name, ok = sf.Tag.Lookup(tagName); !ok {
			f.Name = sf.Name
		}
		if f.Name != "-" {
			fields = append(fields, f)
		}
	}
	slices.SortFunc(fields, func(a, b field) int {
		return compareString(a.Name, b.Name)
	})
	structCache.Store(typ, fields)
	return fields
}

func writeType(doc Object, typ Type) Object {
	doc = append(doc, byte(typ))
	switch typ {
	case TypObject, TypList:
		doc = slices.Grow(doc, sizeOfSize+sizeOfLen)
		return doc[:len(doc)+sizeOfSize+sizeOfLen]
	case TypText, TypRegex:
		doc = slices.Grow(doc, sizeOfSize)
		return doc[:len(doc)+sizeOfSize]
	default:
		return doc
	}
}

func writeBytes[T ~[]byte](doc Object, v T) Object {
	return append(doc, v...)
}

func writeNil(doc Object) Object {
	return append(doc, byte(TypNull))
}

func writeBool(doc Object, v bool) Object {
	var b byte
	if v {
		b = 1
	}
	return append(doc, byte(TypBool), b)
}

func writeString(doc Object, s string) Object {
	doc = append(doc, byte(TypText))
	l := len(s)
	doc = binary.LittleEndian.AppendUint64(doc, uint64(l))
	bs := unsafe.Slice(unsafe.StringData(s), len(s))
	return append(doc, bs...)
}

func writeFloat64(doc Object, f float64) Object {
	u := math.Float64bits(f)
	doc = append(doc, byte(TypNumber))
	return binary.LittleEndian.AppendUint64(doc, u)
}

func writeReflect(doc Object, in reflect.Value) (Object, int, error) {
	kind := in.Kind()
	for kind == reflect.Ptr || kind == reflect.UnsafePointer || kind == reflect.Interface || kind == reflect.Uintptr {
		if in.Type().Implements(reflectMarshaler) {
			v, err := in.Interface().(GEDBMarshaler).MarshalGEDB()
			if err != nil {
				return nil, 0, err
			}
			return writeBytes(doc, v), -1, nil
		}
		if in.IsNil() {
			return writeNil(doc), -1, nil
		}
		in = in.Elem()
		kind = in.Kind()
	}

	switch in.Kind() {
	case reflect.Bool:
		return writeBool(doc, in.Bool()), -1, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return writeFloat64(doc, float64(in.Int())), -1, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return writeFloat64(doc, float64(in.Uint())), -1, nil
	case reflect.Float32, reflect.Float64:
		return writeFloat64(doc, in.Float()), -1, nil
	// case reflect.Complex64:
	// case reflect.Complex128:
	case reflect.Array, reflect.Slice:
		l := len(doc)
		return writeType(doc, TypList), l + 1, nil
	case reflect.Map:
		if in.Type().Key() != reflectString {
			return nil, 0, ErrNonStringKey
		}
		l := len(doc)
		doc = writeType(doc, TypObject)
		return doc, l + 1, nil
	case reflect.Struct:
		l := len(doc)
		return writeType(doc, TypObject), l + 1, nil
	case reflect.String:
		return writeString(doc, in.String()), -1, nil
	case reflect.Chan, reflect.Func:
		return writeNil(doc), -1, nil
	default: // case reflect.Invalid:
		return nil, 0, ErrInvalid
	}

}

func fastPath(any) (Object, bool, error) {
	return nil, false, nil
}

func makeDoc(size int) Object {
	doc := make(Object, headEnd, size)
	doc[0] = byte(TypObject)
	return doc
}
