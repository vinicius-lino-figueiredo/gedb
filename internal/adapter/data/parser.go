package data

import (
	"errors"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

type parser struct {
	data []byte
	i    int
	n    int
}

func (p *parser) parse() (any, error) {
	p.skip()
	val, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skip()
	if p.i != p.n {
		return nil, errors.New("trailing data after JSON")
	}
	return val, nil
}

func (p *parser) skip() {
	for p.i < p.n {
		switch p.data[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) value() (any, error) {
	if p.i >= p.n {
		return nil, errors.New("unexpected end of input")
	}
	switch p.data[p.i] {
	case '{':
		return p.obj()
	case '[':
		return p.arr()
	case '"':
		return p.str()
	case 't':
		return p.expect("true", true)
	case 'f':
		return p.expect("false", false)
	case 'n':
		return p.expect("null", nil)
	default:
		return p.num()
	}
}

func (p *parser) obj() (M, error) {
	p.i++ // skip '{'
	p.skip()
	m := make(M)
	if p.i < p.n && p.data[p.i] == '}' {
		p.i++
		return m, nil
	}
	for {
		p.skip()
		key, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skip()
		if p.i >= p.n || p.data[p.i] != ':' {
			return nil, errors.New("expected ':'")
		}
		p.i++
		p.skip()
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		m[key] = val
		p.skip()
		if p.i >= p.n {
			return nil, errors.New("unexpected end of object")
		}
		if p.data[p.i] == '}' {
			p.i++
			break
		}
		if p.data[p.i] != ',' {
			return nil, errors.New("expected ',' in object")
		}
		p.i++
	}
	return m, nil
}

func (p *parser) arr() ([]any, error) {
	p.i++ // skip '['
	p.skip()
	var out []any
	if p.i < p.n && p.data[p.i] == ']' {
		p.i++
		return []any{}, nil
	}
	for {
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		out = append(out, val)
		p.skip()
		if p.i >= p.n {
			return nil, errors.New("unexpected end of array")
		}
		if p.data[p.i] == ']' {
			p.i++
			break
		}
		if p.data[p.i] != ',' {
			return nil, errors.New("expected ',' in array")
		}
		p.i++
		p.skip()
	}
	return out, nil
}

func (p *parser) str() (string, error) {
	if p.data[p.i] != '"' {
		return "", errors.New("expected string")
	}
	for i := p.i + 1; i < p.n; i++ {
		c := p.data[i]
		switch c {
		case '\\':
			i++
		case '"':
			unquoted := p.data[p.i+1 : i]
			s, err := p.decodeString(unquoted)
			if err != nil {
				return "", err
			}
			p.i = i + 1
			return s, nil
		default:
		}
	}
	return "", errors.New("unterminated string")
}

func (p *parser) decodeString(b []byte) (string, error) {

	out := make([]byte, len(b)+2*utf8.UTFMax)

	i := 0 // current byte
	w := 0 // written

	for i < len(b) {
		if w >= len(out)-2*utf8.UTFMax {
			nb := make([]byte, (len(out)+utf8.UTFMax)*2)
			copy(nb, out[0:w])
			out = nb
		}
		switch c := b[i]; {
		case c == '\\':
			i++
			switch b[i] {
			case '"', '\\', '/', '\'':
				out[w] = b[i]
				i++
				w++
			case 'b':
				out[w] = '\b'
				i++
				w++
			case 'f':
				out[w] = '\f'
				i++
				w++
			case 'n':
				out[w] = '\n'
				i++
				w++
			case 'r':
				out[w] = '\r'
				i++
				w++
			case 't':
				out[w] = '\t'
				i++
				w++
			case 'u':
				i--
				si, sw, br, err := p.treatSlashU(b[i:], out[w:])
				if err != nil {
					return "", err
				}
				i += si
				w += sw
				if br {
					break
				}
			default:
				return "", fmt.Errorf("unknown escape character %q", c)
			}

		case c < ' ':
			return "", errors.New("invalid control char")

		case c < utf8.RuneSelf:
			out[w] = c
			i++
			w++

		default:
			rr, size := utf8.DecodeRune(b[i:])
			i += size
			w += utf8.EncodeRune(out[w:], rr)
		}
	}
	return string(out[0:w]), nil
}

func (p *parser) treatSlashU(b []byte, out []byte) (int, int, bool, error) {
	rr := p.getUTF(b)
	if rr < 0 {
		return 0, 0, false, errors.New("invalid utf8 char")
	}
	i := 6
	w := 0
	if utf16.IsSurrogate(rr) {
		rr1 := p.getUTF(b[i:])
		if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
			i += 6
			w += utf8.EncodeRune(out, dec)
			return i, w, true, nil
		}
		rr = unicode.ReplacementChar
	}
	w += utf8.EncodeRune(out, rr)
	return i, w, false, nil
}

func (p *parser) getUTF(b []byte) rune {
	if len(b) < 6 || b[0] != '\\' || b[1] != 'u' {
		return -1
	}

	r, err := strconv.ParseInt(string(b[2:6]), 16, 64)
	if err != nil {
		return -1
	}
	return rune(r)

}

func (p *parser) num() (any, error) {
	start := p.i
	for p.i < p.n {
		c := p.data[p.i]
		if (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E' {
			p.i++
		} else {
			break
		}
	}
	s := string(p.data[start:p.i])
	var v any
	var err error
	v, err = strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", s)
	}
	return v, nil
}

func (p *parser) expect(lit string, val any) (any, error) {
	end := p.i + len(lit)
	if end > p.n || string(p.data[p.i:end]) != lit {
		return nil, errors.New("invalid literal")
	}
	p.i = end
	return val, nil
}
