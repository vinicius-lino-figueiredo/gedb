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

// GetAddress returns a new instance of [domain.FieldGetter].
func (fg *FieldGetter) GetAddress(field string) ([]string, error) {
	return strings.Split(field, "."), nil
}

// GetField implements [domain.FieldGetter].
func (fg *FieldGetter) GetField(obj any, field string) ([]any, bool, error) {
	fieldParts, _ := fg.GetAddress(field)
	return fg.GetFieldFromParts(obj, fieldParts...)
}

// GetFieldFromParts implements [domain.FieldGetter].
func (fg *FieldGetter) GetFieldFromParts(obj any, fieldParts ...string) ([]any, bool, error) {
	if obj == nil {
		return nil, false, nil
	}

	type V struct {
		v          any
		expandable bool
	}

	var (
		// has to be a list to include expanded queries
		curr = []V{{v: obj, expandable: true}}
		// set to true when continuing a query for every item in a list
		expanded = false
	)

	for _, part := range fieldParts {
		for n := 0; ; n++ {
			if n > len(curr)-1 {
				break
			}

			item := curr[n]

			switch t := item.v.(type) {
			case domain.Document:
				curr[n] = V{v: t.Get(part), expandable: true}
				if !expanded && !t.Has(part) {
					return nil, false, nil
				}
			case []any:
				i, err := strconv.Atoi(part)
				if err != nil {
					expanded = true

					if !item.expandable {
						// curr = slices.Delete(curr, n, n+1)
						curr[n] = V{v: nil, expandable: true}
						n--
						continue
					}

					tv := make([]V, len(t))
					for n, v := range t {
						tv[n] = V{v: v, expandable: false}
					}

					// expanding current list without losing
					// track of current index by inserting
					// in current position.

					// removing current item
					start := curr[:n]
					end := curr[n+1:]

					// expanding t
					curr = append(append(start, tv...), end...)

					// rerunning current index because the
					// order was changed by removal
					n--

				} else if i >= 0 && i < len(t) {
					curr[n] = V{v: t[i], expandable: true}
				} else if expanded {
					curr[n] = V{v: nil, expandable: true}
				} else {
					return nil, false, nil
				}
			default:
				curr[n].v = nil
				if !expanded {
					return nil, false, nil
				}
			}

		}
	}

	res := make([]any, len(curr))
	for n, v := range curr {
		res[n] = v.v
	}

	return res, true, nil
}

// SplitFields implements [domain.FieldGetter].
func (fg *FieldGetter) SplitFields(in string) ([]string, error) {
	return strings.Split(in, ","), nil
}
