package lib

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb"
)

// ==============================================================
// Finding documents
// ==============================================================

func getDotValue(object any, fields ...string) (any, error) {
	if object == nil {
		return nil, nil
	}
	curr := object
	var err error
	for n, part := range fields {
		for nestedField := range strings.SplitSeq(part, ".") {
			switch v := curr.(type) {
			case gedb.Document:
				curr = v.Get(nestedField)
			case []any:
				curr, err = getDotValueList(v, fields[n:]...)
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
		return nil, fmt.Errorf("value %d out of bounds", i)
	}
	return v[i], nil

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
