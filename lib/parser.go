package lib

import (
	"errors"
	"strconv"
	"strings"
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

func (p *parser) obj() (Document, error) {
	p.i++ // skip '{'
	p.skip()
	m := make(Document)
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
		return out, nil
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
	}
	return out, nil
}

func (p *parser) str() (string, error) {
	if p.data[p.i] != '"' {
		return "", errors.New("expected string")
	}
	p.i++
	start := p.i
	var out []byte
	for p.i < p.n {
		c := p.data[p.i]
		if c == '"' {
			out = append(out, p.data[start:p.i]...)
			p.i++
			return string(out), nil
		}
		if c == '\\' {
			out = append(out, p.data[start:p.i]...)
			p.i++
			if p.i >= p.n {
				return "", errors.New("unterminated escape")
			}
			switch p.data[p.i] {
			case '"', '\\', '/':
				out = append(out, p.data[p.i])
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			default:
				return "", errors.New("unsupported escape")
			}
			p.i++
			start = p.i
		} else {
			p.i++
		}
	}
	return "", errors.New("unterminated string")
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
	if strings.ContainsRune(s, '.') {
		v, err = strconv.ParseFloat(s, 64)
	} else {
		v, err = strconv.ParseInt(s, 10, 64)
	}
	if err != nil {
		return nil, errors.New("invalid number")
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
