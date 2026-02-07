package bobj

import (
	"encoding/binary"
	"unsafe"
)

// Value TODO
type Value []byte

// AsBool implements [VVV].
func (v Value) AsBool() (Bool, bool) {
	panic("unimplemented")
}

func (v Value) asBytes() ([]byte, bool) {
	if len(v) < sizeEnd || Type(v[0]) != TypText {
		return nil, false
	}
	end := binary.LittleEndian.Uint64(v[offSize:sizeEnd]) + sizeEnd
	if uint64(len(v)) < end {
		return nil, false
	}
	return v[sizeEnd:end], true
}

// AsObject implements [VVV].
func (v Value) AsObject() (Object, bool) {
	if len(v) < headEnd || Type(v[0]) != TypObject {
		return nil, false
	}

	end := binary.LittleEndian.Uint64(v[offSize:sizeEnd]) + sizeEnd
	if uint64(len(v)) < end {
		return nil, false
	}
	return Object(v[:end]), true
}

// AsList implements [VVV].
func (v Value) AsList() (List, bool) {
	if len(v) < headEnd || Type(v[0]) != TypList {
		return nil, false
	}

	end := binary.LittleEndian.Uint64(v[offSize:sizeEnd]) + sizeEnd
	if uint64(len(v)) < end {
		return nil, false
	}
	return List(v[:end]), true
}

// AsNumber implements [VVV].
func (v Value) AsNumber() (Number, bool) {
	if len(v) != sizeEnd || Type(v[0]) != TypNumber {
		return nil, false
	}
	return Number(v), true
}

// AsRegex implements [VVV].
func (v Value) AsRegex() (Regex, bool) {
	panic("unimplemented")
}

func (v Value) asString() (string, bool) {
	b, ok := v.asBytes()
	if !ok {
		return "", false
	}
	return unsafe.String(unsafe.SliceData(b), len(b)), true
}

// AsText implements [VVV].
func (v Value) AsText() (Text, bool) {
	if len(v) < sizeEnd || v.Type() != TypText {
		return nil, false
	}
	end := binary.LittleEndian.Uint64(v[1:]) + sizeEnd
	if uint64(len(v)) < end {
		return nil, false
	}
	return Text(v[:end]), true
}

// AsTime implements [VVV].
func (v Value) AsTime() (Time, bool) {
	panic("unimplemented")
}

// Bool implements [VVV].
func (v Value) Bool() Bool {
	panic("unimplemented")
}

// Object implements [VVV].
func (v Value) Object() Object {
	o, _ := v.AsObject()
	return o
}

// List implements [VVV].
func (v Value) List() List {
	l, _ := v.AsList()
	return l
}

// Number implements [VVV].
func (v Value) Number() Number {
	n, _ := v.AsNumber()
	return n
}

// Regex implements [VVV].
func (v Value) Regex() Regex {
	panic("unimplemented")
}

// Text implements [VVV].
func (v Value) Text() Text {
	t, _ := v.AsText()
	return t
}

// Time implements [VVV].
func (v Value) Time() Time {
	panic("unimplemented")
}

// Type implements [VVV].
func (v Value) Type() Type {
	if len(v) == 0 {
		return TypUndefined
	}
	return min(Type(v[0]), TypUndefined)
}

// Get TODO
func (v Value) Get(key string) Value {
	return get(v, key)
}

// GetOk TODO
func (v Value) GetOk(key string) (Value, bool) {
	return getOk(v, key)
}
