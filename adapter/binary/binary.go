// Package binary TODO
package binary

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

func readValue(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	switch Type(b[0]) {
	case Obj, Array, String:
		if len(b) < 9 {
			return 0, false
		}
		return binary.LittleEndian.Uint64(b[1:]), true
	case Number, Time:
		return 8, true
	case Boolean:
		return 1, true
	default:
		return 0, false
	}
}

// Type TODO
type Type byte

// TODO
const (
	Obj Type = iota + 1
	Array
	String
	Number
	Boolean
	Time
	Regex
)

// Value TODO
type Value []byte

// Type TODO
func (v Value) Type() Type {
	return Type(v[0])
}

// String TODO
func (v Value) String() string {
	s, _ := v.StringOk()
	return s
}

// StringOk TODO
func (v Value) StringOk() (string, bool) {
	if len(v) > 9 && Type(v[0]) == String {
		return string(v[9:]), true
	}
	return "", false
}

// Element TODO
type Element []byte

// Key TODO
func (e Element) Key() string {
	return string(e.BKey())
}

// BKey TODO
func (e Element) BKey() []byte {
	size := binary.LittleEndian.Uint64(e)
	return e[8 : size+8]
}

// Value TODO
func (e Element) Value() Value {
	size := binary.LittleEndian.Uint64(e)
	return Value(e[size+8:])
}

// Object TODO
type Object []byte

// Get TODO
func (o Object) Get(key string) Value {
	v, _ := o.GetOk(key)
	return v
}

// GetOk TODO
func (o Object) GetOk(key string) (Value, bool) {
	bKey := unsafe.Slice(unsafe.StringData(key), len(key))
	for _, element := range o.Elements() {
		if bytes.Equal(bKey, element.BKey()) {
			return element.Value(), true
		}
	}
	return nil, false
}

// Elements TODO
func (o Object) Elements() []Element {
	if len(o) < 9 {
		return nil
	}
	size := binary.LittleEndian.Uint64(o[1:])
	if len(o) < int(size) {
		return nil
	}

	elements := make([]Element, 0, 16)

	var KSize, VSize uint64
	var ok bool
	rest := o[9:size]
	for len(rest) > 0 {
		KSize = binary.LittleEndian.Uint64(rest)
		VSize, ok = readValue(rest[KSize+8:])
		if !ok {
			return nil
		}
		elements = append(elements, Element(rest[:KSize+VSize+16]))
		rest = rest[min(KSize+VSize+17, uint64(len(rest))):]
	}
	return elements
}
