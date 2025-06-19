package lib

import "bytes"

type list []any

func (a list) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := a.marshalJSON(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (a list) marshalJSON(buf *bytes.Buffer) error {
	buf.WriteRune('[')
	var i bool
	var err error
	for _, value := range a {
		if i {
			buf.WriteRune(',')
		}
		err = marshalAny(buf, value)
		if err != nil {
			return err
		}
		i = true
	}
	buf.WriteRune(']')
	return nil
}

func (a list) Compare(b list) int {
	return a.compare(b, nil)
}

func (a list) compare(b list, compareStrings func(a, b string) int) int {
	minLength := min(len(a), len(b))

	var comp int
	for i := range minLength {
		comp = compareThings(a[i], b[i], compareStrings)

		if comp != 0 {
			return comp
		}
	}

	// Common section was identical, longest one wins
	return compareNSB(newBig(int64(len(a))), newBig(int64(len(b))))
}
