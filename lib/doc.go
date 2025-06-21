package lib

import (
	"bytes"
)

type Document map[string]any

// ID implements nedb.Document
func (d Document) ID() string {
	i := d["_id"]
	if s, ok := i.(string); ok {
		return s
	}
	return ""
}

func (d Document) Compare(other any) (int, bool) {
	return compareThingsFunc(nil)(d, other), true
}

func (d Document) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := d.marshalJSON(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d Document) marshalJSON(buf *bytes.Buffer) error {
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
