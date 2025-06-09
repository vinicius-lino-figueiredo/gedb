package doc

import (
	"bytes"
)

type doc map[string]any

func (d doc) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := d.marshalJSON(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d doc) marshalJSON(buf *bytes.Buffer) error {
	buf.WriteRune('{')
	var i bool
	for key, value := range d {
		if i {
			buf.WriteRune(',')
		}
		buf.WriteRune('"')
		buf.WriteString(key)
		buf.WriteString(`":`)

		marshalAny(buf, value)

		i = true
	}
	buf.WriteRune('}')
	return nil
}
