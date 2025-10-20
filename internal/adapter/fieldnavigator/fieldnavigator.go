// Package fieldnavigator contains the defalt implementation of
// [domain.FieldNavigator] for GEDB package.
package fieldnavigator

import (
	"strconv"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var invalid = []domain.GetSetter{NewGetSetterEmpty()}

type navCtx struct {
	// has to be a list to include expanded queries
	curr []field
	idx  int
	n    int
	item field
	part string
	addr []string
	// set to true when continuing a query for every item in a list
	expanded bool
	ensure   bool
}

type field struct {
	v          any
	expandable bool
	gs         domain.GetSetter
}

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
	if obj == nil {
		return invalid, false, nil
	}

	if len(fieldParts) == 0 {
		return []domain.GetSetter{NewReadOnlyGetSetter(obj)}, false, nil
	}

	ctx := &navCtx{
		curr:   []field{{v: obj, expandable: true}},
		addr:   fieldParts,
		ensure: ensure,
	}

	for ctx.idx, ctx.part = range fieldParts {
		for ctx.n = 0; ; ctx.n++ {
			if ctx.n > len(ctx.curr)-1 {
				break
			}

			ctx.item = ctx.curr[ctx.n]

			switch t := ctx.item.v.(type) {
			case domain.Document:
				v, ok, err := fn.enterDoc(ctx, t)
				if err != nil || ok {
					return v, ctx.expanded, err
				}
			case []any:
				v, ok, err := fn.enterArr(ctx, t)
				if err != nil || ok {
					return v, ctx.expanded, err
				}
			default:
				ctx.curr[ctx.n].v = nil
				ctx.curr[ctx.n].v = NewGetSetterEmpty()
				if !ctx.expanded {
					return invalid, false, nil
				}
			}

		}
	}

	res := make([]domain.GetSetter, len(ctx.curr))
	for n, v := range ctx.curr {
		res[n] = v.gs
	}

	return res, ctx.expanded, nil
}

func (fn *FieldNavigator) enterDoc(ctx *navCtx, t domain.Document) ([]domain.GetSetter, bool, error) {
	if !ctx.expanded && !t.Has(ctx.part) {
		// when not ensuring a field, a unset
		// key in a document is invalid.
		if !ctx.ensure {
			return invalid, true, nil
		}
		if ctx.idx < len(ctx.addr)-1 {
			newDoc, err := fn.docFac(nil)
			if err != nil {
				return nil, false, err
			}

			t.Set(ctx.part, newDoc)
		} else {
			t.Set(ctx.part, nil)
		}

	}
	ctx.curr[ctx.n] = field{
		v:          t.Get(ctx.part),
		expandable: true,
		gs:         NewGetSetterWithDoc(t, ctx.part),
	}
	return nil, false, nil
}

func (fn *FieldNavigator) enterArr(ctx *navCtx, t []any) ([]domain.GetSetter, bool, error) {
	i, err := strconv.Atoi(ctx.part)
	if err != nil {
		ctx.expanded = true

		if !ctx.item.expandable {
			ctx.curr[ctx.n] = field{
				v:          nil,
				expandable: true,
				gs:         NewGetSetterEmpty(),
			}
			ctx.n--
			return nil, false, nil
		}

		tv := make([]field, len(t))
		for nn, v := range t {
			tv[nn] = field{v: v, expandable: false, gs: NewGetSetterEmpty()}
		}

		// expanding current list without losing
		// track of current index by inserting
		// in current position.

		// removing current item
		start := ctx.curr[:ctx.n]
		end := ctx.curr[ctx.n+1:]

		// expanding t
		ctx.curr = append(append(start, tv...), end...)

		// rerunning current index because the
		// order was changed by removal
		ctx.n--

	} else {

		// valid index (within range)
		if i >= 0 && (i < len(t) || ctx.ensure) {
			if ctx.ensure && i >= 0 {
				newT := make([]any, i+1)
				_ = copy(newT, t)
				t = newT
				ctx.curr[ctx.n].gs.Set(newT)
			}
			ctx.curr[ctx.n] = field{
				v:          t[i],
				expandable: true,
				gs:         NewGetSetterWithArrayIndex(t, i),
			}
			return nil, false, nil
		}

		// invalid but expanded
		if ctx.expanded {
			ctx.curr[ctx.n] = field{v: nil, expandable: true}
		}

		// invalid and not expanded
		res := []domain.GetSetter{NewGetSetterEmpty()}
		return res, true, nil
	}
	return nil, false, nil
}

// SplitFields implements [domain.FieldNavigator].
func (fn *FieldNavigator) SplitFields(in string) ([]string, error) {
	return strings.Split(in, ","), nil
}
