package lib

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type list []any

func (l list) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := l.marshalJSON(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l list) marshalJSON(buf *bytes.Buffer) error {
	buf.WriteRune('[')
	var i bool
	var err error
	for _, value := range l {
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

func (l *list) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*l = nil
		return nil
	}
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	if data[0] != byte('{') {
		return fmt.Errorf("expected '{', got '%s'", string(data[0]))
	}
	rest, err := l.unmarshalJSON(data)
	if err != nil {
		return err
	}
	if rest < len(data) {
		return fmt.Errorf("expected EOF, got '%s'", string(data[rest]))
	}
	return nil
}

func (l *list) unmarshalJSON(data []byte) (int, error) {
	escapable := map[byte]byte{}
	const (
		waitValue = iota
		str
		number
		trueBool
		falseBool
		waitComma
	)
	where := waitValue
	escaping := false
	buf := new(bytes.Buffer)
	skip := 1
	var thisSkip int
	var nd list
	var cv any
	skiped := ""
	for n, b := range data {
		thisSkip = n
		if skip > 0 {
			skiped += string(b)
			skip--
			continue
		}
		if bytes.Contains([]byte(" \n\r"), []byte{b}) {
			switch where {
			case waitValue, waitComma:
				continue
			default:
			}
		}
		switch where {
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
		case waitComma:
			if b != byte(',') {
				return 0, fmt.Errorf("expected ':', got '%s'", string(b))
			}
			where = waitValue
		case waitValue:
			switch b {
			case '"':
				where = str
			case '{':
				var err error
				innerDoc := make(Document)
				skip, err = innerDoc.unmarshalJSON(data[n:])
				if err != nil {
					return 0, err
				}
				cv = innerDoc
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
				where = waitComma
			case ',':
				var err error
				cv, err = strconv.ParseInt(buf.String(), 10, 64)
				buf.Reset()
				if err != nil {
					return 0, err
				}
				where = waitValue
			case ']':
				var err error
				cv, err = strconv.ParseInt(buf.String(), 10, 64)
				if err != nil {
					return 0, err
				}
				where = waitComma
				break Switch
			default:
				return 0, fmt.Errorf("expected number or '.', got '%s'", string(b))
			}
		case trueBool:
			if strings.HasPrefix("true", buf.String()) && buf.Len() == 4 {
				where = waitComma
			}
		}
		if where == waitComma || where == waitValue {
			nd = append(nd, cv)
			if b == ']' {
				break
			}
		}
	}
	*l = nd
	return thisSkip, nil
}

func (l list) Compare(b list) int {
	return l.compare(b, nil)
}

func (l list) compare(b list, compareStrings func(a, b string) int) int {
	minLength := min(len(l), len(b))

	var comp int
	for i := range minLength {
		comp = compareThings(l[i], b[i], compareStrings)

		if comp != 0 {
			return comp
		}
	}

	// Common section was identical, longest one wins
	return compareNSB(newBig(int64(len(l))), newBig(int64(len(b))))
}
