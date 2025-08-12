package lib

import (
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// ==============================================================
// Finding documents
// ==============================================================

func getDotValuesOk(object any, fields ...string) (any, bool, error) {
	if object == nil {
		return nil, false, nil
	}
	curr := object
	ok := true
	var err error
	for n, part := range fields {
		switch v := curr.(type) {
		case gedb.Document:
			if !v.Has(part) {
				ok = false
			}
			curr = v.Get(part)
		case []any:
			curr, ok, err = getDotValueListOk(v, fields[n:]...)
			if err != nil {
				return nil, false, err
			}
		}
	}
	return curr, ok, nil
}

func getDotValue(object any, fields ...string) (any, error) {
	if object == nil {
		return nil, nil
	}
	curr := object
	var err error
	for _, part := range fields {
		nestedFields := strings.Split(part, ".")
		for m, nestedField := range nestedFields {
			switch v := curr.(type) {
			case gedb.Document:
				curr = v.Get(nestedField)
			case []any:
				curr, err = getDotValueList(v, nestedFields[m:]...)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return curr, nil
}

func getDotValueList(v []any, fieldParts ...string) (any, error) {
	i, err := strconv.Atoi(fieldParts[0])
	if err != nil {
		m := make([]any, len(v))
		for n, el := range v {
			m[n], err = getDotValue(el, fieldParts...)
			if err != nil {
				return nil, err
			}
		}
		return m, nil
	}
	if i >= len(v) {
		return nil, nil
	}
	return v[i], nil

}

func getDotValueListOk(v []any, fieldParts ...string) (any, bool, error) {
	i, err := strconv.Atoi(fieldParts[0])
	ok := true
	if err != nil {
		m := make([]any, len(v))
		for n, el := range v {
			m[n], err = getDotValue(el, fieldParts...)
			if err != nil {
				return nil, false, err
			}
		}
		return m, ok, nil
	}
	if i >= len(v) {
		return nil, false, nil
	}
	return v[i], ok, nil

}

// Get dot values for either a bunch of fields or just one.
func getDotValues(obj any, fields []string) (any, error) {
	if len(fields) <= 1 {
		return getDotValue(obj, fields...)
	}
	key := make(Document)
	var err error
	for _, field := range fields {
		key[field], err = getDotValue(obj, field)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}
