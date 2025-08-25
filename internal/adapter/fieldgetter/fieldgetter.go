package fieldgetter

import (
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// FieldGetter implements [domain.FieldGetter].
type FieldGetter struct{}

// NewFieldGetter returns a new instance of [domain.FieldGetter].
func NewFieldGetter() domain.FieldGetter {
	return &FieldGetter{}
}

// GetField implements [domain.FieldGetter].
func (fg *FieldGetter) GetField(obj any, field string) ([]any, bool, error) {

	if obj == nil {
		return nil, false, nil
	}

	var (
		fieldParts = strings.Split(field, ".")
		// has to be a list to include expanded queries
		curr = []any{obj}
		// set to true when continuing a query for every item in a list
		expanded = false
	)

	for _, part := range fieldParts {
		for n := 0; ; n++ {
			if n > len(curr)-1 {
				break
			}

			item := curr[n]

			switch t := item.(type) {
			case domain.Document:
				curr[n] = t.Get(part)
				if !expanded && !t.Has(part) {
					return nil, false, nil
				}
			case []any:
				i, err := strconv.Atoi(part)
				if err != nil {
					expanded = true

					// expanding current list without losing
					// track of current index by inserting
					// in current position.

					// removing current item
					start := curr[:n]
					end := curr[n+1:]

					// expanding t
					curr = append(append(start, t...), end...)

					// rerunning current index because the
					// order was changed by removal
					n--

				} else if i >= 0 && i < len(t) {
					curr[n] = t[i]
				} else if expanded {
					curr[n] = nil
				} else {
					return nil, false, nil
				}
			default:
				curr[n] = nil
				if !expanded {
					return nil, false, nil
				}
			}

		}
	}
	return curr, true, nil
}

// SplitFields implements [domain.FieldGetter].
func (fg *FieldGetter) SplitFields(in string) ([]string, error) {
	return strings.Split(in, ","), nil
}
