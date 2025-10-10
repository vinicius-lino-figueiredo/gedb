package fieldnavigator

import (
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// FieldNavigator implements [domain.FieldNavigator].
type FieldNavigator struct {
	docFac func(any) (domain.Document, error)
}

// NewFieldNavigator returns a new instance of [domain.FieldNavigator].
func NewFieldNavigator(docFac func(any) (domain.Document, error)) domain.FieldNavigator {
	return &FieldNavigator{
		docFac: docFac,
	}
}

// GetAddress implements [domain.FieldNavigator].
func (fn *FieldNavigator) GetAddress(field string) ([]string, error) {
	return strings.Split(field, "."), nil
}

// GetField implements [domain.FieldNavigator].
func (fn *FieldNavigator) GetField(obj any, fieldParts ...string) ([]domain.GetSetter, bool, error) {
	return fn.getField(obj, fieldParts, false)
}

// EnsureField implements [domain.FieldNavigator].
func (fn *FieldNavigator) EnsureField(obj any, fieldParts ...string) ([]domain.GetSetter, error) {
	res, _, err := fn.getField(obj, fieldParts, true)
	return res, err
}

func (fn *FieldNavigator) getField(obj any, fieldParts []string, ensure bool) ([]domain.GetSetter, bool, error) {
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
		gs         domain.GetSetter
	}

	var (
		// has to be a list to include expanded queries
		curr = []V{{v: obj, expandable: true}}
		// set to true when continuing a query for every item in a list
		expanded = false
	)

	for idx, part := range fieldParts {
		for n := 0; ; n++ {
			if n > len(curr)-1 {
				break
			}

			item := curr[n]

			switch t := item.v.(type) {
			case domain.Document:
				if !expanded && !t.Has(part) {
					// when not ensuring a field, a unset
					// key in a document is invalid.
					if !ensure {
						return invalid, false, nil
					}
					if idx < len(fieldParts)-1 {
						newDoc, err := fn.docFac(nil)
						if err != nil {
							return nil, false, err
						}

						t.Set(part, newDoc)
					} else {
						t.Set(part, nil)
					}

				}
				curr[n] = V{
					v:          t.Get(part),
					expandable: true,
					gs:         NewGetSetterWithDoc(t, part),
				}
			case []any:
				i, err := strconv.Atoi(part)
				if err != nil {
					expanded = true

					if !item.expandable {
						curr[n] = V{
							v:          nil,
							expandable: true,
							gs:         NewGetSetterEmpty(),
						}
						n--
						continue
					}

					tv := make([]V, len(t))
					for nn, v := range t {
						tv[nn] = V{v: v, expandable: false, gs: NewGetSetterEmpty()}
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

				} else {

					// valid index (within range)
					if i >= 0 && (i < len(t) || ensure) {
						if ensure && i >= 0 {
							newT := make([]any, i+1)
							_ = copy(newT, t)
							t = newT
							curr[n].gs.Set(newT)
						}
						curr[n] = V{
							v:          t[i],
							expandable: true,
							gs:         NewGetSetterWithArrayIndex(t, i),
						}
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
				curr[n].v = NewGetSetterEmpty()
				if !expanded {
					return invalid, false, nil
				}
			}

		}
	}

	res := make([]domain.GetSetter, len(curr))
	for n, v := range curr {
		res[n] = v.gs
	}

	return res, expanded, nil
}

// SplitFields implements [domain.FieldNavigator].
func (fn *FieldNavigator) SplitFields(in string) ([]string, error) {
	return strings.Split(in, ","), nil
}
