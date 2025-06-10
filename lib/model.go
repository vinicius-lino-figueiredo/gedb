package lib

import (
	"fmt"
	"strconv"
	"strings"
)

// ==============================================================
// Finding documents
// ==============================================================

// Get a value from object with dot notation
func getDotValue(object any, field string) (any, error) {
	fieldParts := strings.Split(field, ".")
	return getDotValueSplit(object, fieldParts)
}

func getDotValueSplit(object any, fieldParts []string) (any, error) {
	if object == nil {
		return nil, nil
	}

	curr := object
	var err error
	for n, part := range fieldParts {
		switch v := curr.(type) {
		case doc:
			curr = v[part]
		case list:
			curr, err = getDotValueList(v, fieldParts[n:])
			if err != nil {
				return nil, err
			}
		}
	}
	return curr, nil
}

func getDotValueList(v list, fieldParts []string) (any, error) {

	i, err := strconv.Atoi(fieldParts[0])
	if err != nil {
		m := make(list, len(v))
		for n, el := range v {
			m[n], err = getDotValueSplit(el, fieldParts)
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
