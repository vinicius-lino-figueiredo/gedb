package bobj

import "encoding/binary"

// List TODO
type List []byte

// AppendInto implements [LLL].
func (l List) AppendInto(value Value, target []byte) []byte {
	panic("unimplemented")
}

// DeleteInto implements [LLL].
func (l List) DeleteInto(idx int, target []byte) []byte {
	panic("unimplemented")
}

// Index implements [LLL].
func (l List) Index(idx int) Value {
	if length := l.Length(); length < 0 || length <= idx {
		return nil
	}
	size := binary.LittleEndian.Uint64(l[offSize:sizeEnd]) + sizeOfLen + typBytes
	b := l[offData:size]

	var val Value
	var ok bool
	for ; idx >= 0; idx-- {
		val, ok = skipVal(b)
		if !ok {
			return nil
		}
		b = b[len(val):]
	}
	return val
}

// Length implements [LLL].
func (l List) Length() int {
	if len(l) < offData || Type(l[0]) != TypList {
		return -1
	}
	return int(binary.LittleEndian.Uint64(l[offSize:sizeEnd]))
}

// Set implements [LLL].
func (l List) Set(idx int, value Value) {
	panic("unimplemented")
}

// SubSet implements [LLL].
func (l List) SubSet(start int, end int) Subset {
	panic("unimplemented")
}

// Type implements [LLL].
func (l List) Type() Type {
	panic("unimplemented")
}
