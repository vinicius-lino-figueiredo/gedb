package doc

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
