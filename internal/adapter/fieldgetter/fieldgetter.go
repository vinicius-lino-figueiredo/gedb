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
func (fg *FieldGetter) GetField(obj any, fieldParts ...string) ([]domain.GetSetter, bool, error) {
	invalid := []domain.GetSetter{NewGetSetterEmpty()}
	if obj == nil {
		return invalid, false, nil
	}

	if len(fieldParts) == 0 {
		return invalid, false, nil
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

	var res []domain.GetSetter

	for partIdx, part := range fieldParts {
		if partIdx == len(fieldParts)-1 {
			res = make([]domain.GetSetter, len(curr))
		}
		for n := 0; ; n++ {
			if n > len(curr)-1 {
				break
			}

			item := curr[n]

			switch t := item.v.(type) {
			case domain.Document:
				curr[n] = V{v: t.Get(part), expandable: true}
				if partIdx == len(fieldParts)-1 {
					res[n] = NewGetSetterWithDoc(t, part)
				}
				if !expanded && !t.Has(part) {
					return invalid, false, nil
				}
			case []any:
				i, err := strconv.Atoi(part)
				if err != nil {
					expanded = true

					if !item.expandable {
						curr[n] = V{v: nil, expandable: true}
						res[n] = NewGetSetterEmpty()
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

					// resizing result slice to match curr
					newRes := make([]domain.GetSetter, len(curr))
					_ = copy(newRes, res)
					res = newRes

					// rerunning current index because the
					// order was changed by removal
					n--

				} else {
					if partIdx == len(fieldParts)-1 {
						res[n] = NewGetSetterWithArrayIndex(t, i)
					}

					// valid index (within range)
					if i >= 0 && i < len(t) {
						curr[n] = V{v: t[i], expandable: true}
						continue
					}

					// invalid but expanded
					if expanded {
						curr[n] = V{v: nil, expandable: true}
					}

					// invalid and not expanded
					res := []domain.GetSetter{NewGetSetterEmpty()}
					return res, false, nil
				}
			default:
				curr[n].v = nil
				res[n] = NewGetSetterEmpty()
				if !expanded {
					return invalid, false, nil
				}
			}

		}
	}

	return res, expanded, nil
}

// SplitFields implements [domain.FieldGetter].
func (fg *FieldGetter) SplitFields(in string) ([]string, error) {
	return strings.Split(in, ","), nil
}
