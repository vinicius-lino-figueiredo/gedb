package lib

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Document map[string]any

// ID implements gedb.Document
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

func (d *Document) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = nil
		return nil
	}
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	if data[0] != byte('{') {
		return fmt.Errorf("expected '{', got '%s'", string(data[0]))
	}
	rest, err := d.unmarshalJSON(data)
	if err != nil {
		return err
	}
	if rest+1 < len(data) {
		return fmt.Errorf("expected EOF, got '%s'", string(data[rest]))
	}
	return nil
}

func (d *Document) unmarshalJSON(data []byte) (int, error) {
	escapable := map[byte]byte{}
	const (
		waitKey = iota
		key
		waitColon
		waitValue
		str
		number
		trueBool
		falseBool
		waitComma
	)
	where := waitKey
	escaping := false
	buf := new(bytes.Buffer)
	var currKey string
	skip := 1
	var thisSkip int
	nd := make(Document)
	var cv any
	skiped := ""
Loop:
	for n, b := range data {
		thisSkip = n
		if skip > 0 {
			skiped += string(b)
			skip--
			continue
		}
		if bytes.Contains([]byte(" \n\r"), []byte{b}) {
			switch where {
			case waitKey, waitColon, waitValue, waitComma:
				continue
			default:
			}
		}
		switch where {
		case waitKey:
			if b == '}' {
				break Loop
			}
			if b != '"' {
				return 0, fmt.Errorf("expected '\"', got '%s'", string(b))
			}
			where = key
		case key:
			if escaping {
				if escaped, ok := escapable[b]; ok {
					_ = buf.WriteByte(escaped)
				} else {
					return 0, fmt.Errorf("unknown escape char '%s'", string(b))
				}
			}
			if b != '"' {
				_ = buf.WriteByte(b)
				continue
			}
			currKey = buf.String()
			buf.Reset()
			if _, exists := nd[currKey]; exists {
				return 0, fmt.Errorf("duplicate key %q", currKey)
			}
			where = waitColon
		case str:
			if escaping {
				if escaped, ok := escapable[b]; ok {
					_ = buf.WriteByte(escaped)
					escaping = false
				} else {
					return 0, fmt.Errorf("unknown escape char '%s'", string(b))
				}
			}
			if b != byte('"') {
				_ = buf.WriteByte(b)
				continue
			}
			cv = buf.String()
			buf.Reset()
			where = waitComma
		case waitColon:
			if b != byte(':') {
				return 0, fmt.Errorf("expected ':', got '%s'", string(b))
			}
			where = waitValue
		case waitComma:
			if b == '}' {
				break Loop
			}
			if b != ',' {
				return 0, fmt.Errorf("expected ',', got '%s'", string(b))
			}
			where = waitKey
		case waitValue:
			switch b {
			case '"':
				where = str
			case '{':
				skiped = ""
				var err error
				innerDoc := make(Document)
				skip, err = innerDoc.unmarshalJSON(data[n:])
				if err != nil {
					return 0, err
				}
				cv = innerDoc
				where = waitComma
			case '[':
				var err error
				buf.Reset()
				var innerList list
				skip, err = innerList.unmarshalJSON(data[n:])
				if err != nil {
					return 0, err
				}
				cv = innerList
				where = waitComma
			case '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0':
				_ = buf.WriteByte(b)
				where = number
			case 't':
				_ = buf.WriteByte(b)
				where = trueBool
			case 'f':
				_ = buf.WriteByte(b)
				where = falseBool
			}
		case number:
		Switch:
			switch b {
			case '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.':
				_ = buf.WriteByte(b)
			case ' ', '\n', '\r':
				var err error
				cv, err = strconv.ParseInt(buf.String(), 10, 64)
				buf.Reset()
				if err != nil {
					return 0, err
				}
				nd[currKey] = cv
				where = waitComma
			case ',':
				var err error
				cv, err = strconv.ParseInt(buf.String(), 10, 64)
				buf.Reset()
				if err != nil {
					return 0, err
				}
				nd[currKey] = cv
				where = waitKey
			case '}':
				var err error
				cv, err = strconv.ParseInt(buf.String(), 10, 64)
				buf.Reset()
				if err != nil {
					return 0, err
				}
				nd[currKey] = cv
				where = waitComma
				break Switch
			default:
				return 0, fmt.Errorf("expected number or '.', got '%s'", string(b))
			}
		case trueBool:
			_ = buf.WriteByte(b)
			tkn := buf.String()
			if strings.HasPrefix("true", tkn) {
				if buf.Len() == 4 {
					buf.Reset()
					cv = true
					where = waitComma
				}
			} else {
				buf.Reset()
				return 0, fmt.Errorf("unrecognized token\"%s\". expected boolean true", tkn)
			}
		}
		if where == waitComma {
			nd[currKey] = cv
			if b == '}' {
				break
			}
		}
	}
	*d = nd
	return thisSkip, nil
}

func (d Document) asMap() map[string]any {

	m := make(map[string]any, len(d))
	for k, v := range d {
		if v == nil {
			m[k] = nil
			continue
		}
		switch t := v.(type) {
		case Document:
			m[k] = t.asMap()
		case list:
			m[k] = t // TODO: implement asSlice
		default:
			m[k] = t
		}
	}

	return m
}

func (d Document) Contains(s string) bool {
	_, ok := d[s]
	return ok
}

func (d Document) CompareKey(s string, b any) int {
	return compareThings(d[s], b, nil)
}

func (d Document) Get(keys ...string) any {
	curr := d
	var data any
	var ok bool
	for n, key := range keys {
		data, ok = curr[key]
		if !ok {
			return nil
		}
		if n == len(keys)-1 {
			break
		}
		if data == nil {
			return nil
		}
		if curr, ok = data.(Document); !ok {
			return nil
		}
	}
	return data
}

func (d Document) Nvl(other any, keys ...string) any {
	r := d.Get(keys...)
	if r != nil {
		return r
	}
	return other
}
