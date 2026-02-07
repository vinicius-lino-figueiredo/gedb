// Package bobj TODO
package bobj

import (
	"cmp"
	"encoding/binary"
	"math"
)

const (
	// Tamanhos base
	typBytes   = 1 // offType
	sizeOfSize = 8 // pode sugerir: sizeOfSize
	sizeOfLen  = 8 // pode sugerir: sizeOfLen

	// Offsets no header: [type:1][size:8][length:8]
	offSize = typBytes             // = 1, pode sugerir: offSize
	sizeEnd = offSize + sizeOfSize // = 9
	offLen  = sizeEnd              // = 9, pode sugerir: offLen
	offData = offLen + sizeOfLen   // = 17, pode sugerir: offData
	headEnd = offData              // = 17
)

// TODO
const (
	TypObject Type = iota
	TypList
	TypText
	TypNumber
	TypTime
	TypRegex
	TypBool
	TypNull
	TypUndefined
)

const id = "_id"

// Type TODO
type Type byte

// Object TODO
type Object []byte

// Get implements [DDD].
func (d Object) Get(key string) Value {
	return get(d, key)
}

// GetOk implements [DDD].
func (d Object) GetOk(key string) (Value, bool) {
	return getOk(d, key)
}

// Has implements [DDD].
func (d Object) Has(key string) bool {
	panic("unimplemented")
}

// ID implements [DDD].
func (d Object) ID() Value {
	if len(d) < headEnd {
		return nil
	}

	b := d[headEnd:]

	val, ok := skipVal(b)
	if !ok {
		return nil
	}
	s, ok := val.asString()
	if !ok {
		return nil
	}
	if s != id {
		return nil
	}
	b = b[len(val):]
	val, _ = skipVal(b)
	return val
}

// Length implements [DDD].
func (d Object) Length() int {
	panic("unimplemented")
}

// SetIDInto implements [DDD].
func (d Object) SetIDInto(id Value, target []byte) []byte {
	panic("unimplemented")
}

// SetInto implements [DDD].
func (d Object) SetInto(key string, value Value, target []byte) []byte {
	panic("unimplemented")
}

// Type implements [DDD].
func (d Object) Type() Type {
	panic("unimplemented")
}

// UnsetInto implements [DDD].
func (d Object) UnsetInto(key string, value Value, target []byte) []byte {
	panic("unimplemented")
}

type DDD interface { // Document
	// Deve sempre retornar [TypDocument], a não ser que haja algum problema
	// no tamanho. Nesse caso, retornaa [TypUndefined].
	Type() Type

	// Retorna o ID sempre presente no documento.
	ID() Value

	// Retorna o documento como um objeto sem _id
	// Retorna a quantidade de itens do documento. Se houver problemas no
	// documento, retorna -1
	Length() int

	// Encontra um valor por chave. Pode retornar Undefined. O campo de id
	// é rotornado com a chave "_id"
	Get(key string) Value
	// Encontra o valor, mas retorna false se ele não for encontrado.
	GetOk(key string) (Value, bool)
	// Retorna se o valor existe no documento.
	Has(key string) bool

	// Clona a si mesmo num slice de byte, alterando o _id
	SetIDInto(id Value, target []byte) []byte
	// Clona a si mesmo num slice de byte, alterando uma chave. Se a chave
	// passada for _id, causa panic.
	SetInto(key string, value Value, target []byte) []byte
	// Clona a si mesmo num slice de byte, removendo uma chave. Se a chave
	// passada for _id, causa panic.
	UnsetInto(key string, value Value, target []byte) []byte
}

// Subset TODO
type Subset []byte

// Text TODO
type Text []byte

// AsString TODO
func (t Text) AsString() (string, bool) {
	if len(t) < sizeEnd {
		return "", false
	}
	end := binary.LittleEndian.Uint64(t[1:]) + sizeEnd
	if uint64(len(t)) < end {
		return "", false
	}
	return string(t[sizeEnd:end]), true
}

// String TODO
func (t Text) String() string {
	s, _ := t.AsString()
	return s
}

// Number TODO
type Number []byte

// AsFloat64 TODO
func (n Number) AsFloat64() (float64, bool) {
	if len(n) != sizeEnd {
		return math.NaN(), false
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(n[1:])), true
}

// Float64 TODO
func (n Number) Float64() float64 {
	f, _ := n.AsFloat64()
	return f
}

// Time TODO
type Time []byte

// Regex TODO
type Regex []byte

// Bool TODO
type Bool []byte

// Null TODO
type Null []byte

func get(b []byte, key string) Value {
	v, _ := getOk(b, key)
	return v
}

func getOk(b []byte, key string) (Value, bool) {
	if len(b) < headEnd || Type(b[0]) != TypObject {
		return nil, false
	}

	size := binary.LittleEndian.Uint64(b[offSize:sizeEnd]) + sizeEnd

	// if uint64(len(d)) < size {
	// 	return nil, false
	// }

	b = b[headEnd:size]
	var val Value
	var ok bool
	var s string
	for len(b) > 0 {
		val, ok = skipVal(b)
		if !ok {
			return nil, false
		}
		s, ok = val.asString()
		if !ok {
			return nil, false
		}
		b = b[len(val):]
		if s == key {
			return skipVal(b)
		}
		val, ok = skipVal(b)
		if !ok {
			return nil, false
		}
		b = b[len(val):]
	}
	return nil, false
}

func skipVal(b []byte) (Value, bool) {
	if len(b) == 0 {
		return nil, false
	}
	switch Type(b[0]) {
	case TypObject, TypList, TypText, TypRegex:
		if len(b) <= sizeEnd {
			return Value(b), false
		}
		end := binary.LittleEndian.Uint64(b[1:sizeEnd]) + sizeEnd
		if end > uint64(len(b)) {
			return nil, false
		}
		return Value(b[:end]), true
	case TypNumber:
		if len(b) < sizeEnd {
			return Value(b), false
		}
		return Value(b[:sizeEnd]), true
	case TypTime:
		if len(b) < sizeEnd {
			return Value(b), false
		}
		return Value(b[:sizeEnd]), true
	case TypBool:
		if len(b) < 2 {
			return Value(b), false
		}
		return Value(b[:2]), true
	case TypNull:
		if len(b) != 1 {
			return Value(b), false
		}
		return Value(b[:1]), true
	default: // Undefined and any error
		if len(b) != 1 {
			return Value(b), false
		}
		return Value(b[:1]), true
	}
}

func compareString(a, b string) int {
	if a == "_id" {
		if b == "_id" {
			return 0
		}
		return -1
	}
	if b == "_id" {
		return 1
	}
	return cmp.Compare(a, b)
}
