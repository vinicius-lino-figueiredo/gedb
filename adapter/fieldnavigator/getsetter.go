package fieldnavigator

import "github.com/vinicius-lino-figueiredo/gedb/domain"

// ListGetSetter is a [domain.GetSetter] than can read an write a specific index
// in a slice of [any].
type ListGetSetter struct {
	List  []any
	Index int
}

// NewGetSetterWithArrayIndex returns a new implementation of [domain.GetSetter]
// that will represent a value from a slice of [any].
func NewGetSetterWithArrayIndex(list []any, index int) domain.GetSetter {
	return &ListGetSetter{List: list, Index: index}
}

// Get implements [domain.GetSetter].
func (l *ListGetSetter) Get() (value any, defined bool) {
	if l.Index >= 0 && l.Index < len(l.List) {
		return l.List[l.Index], true
	}
	return nil, false
}

// Set implements [domain.GetSetter].
func (l *ListGetSetter) Set(value any) {
	if l.Index >= 0 && l.Index < len(l.List) {
		l.List[l.Index] = value
	}
}

// Unset implements [domain.GetSetter].
func (l *ListGetSetter) Unset() {
	if l.Index >= 0 && l.Index < len(l.List) {
		l.List[l.Index] = nil
	}
}

// DocGetSetter is a [domain.GetSetter] than can read an write a specific key in
// a [domain.Document].
type DocGetSetter struct {
	Doc domain.Document
	Key string
}

// NewGetSetterWithDoc returns a new implementation of [domain.GetSetter] that
// will represent a value from a [domain.Document].
func NewGetSetterWithDoc(doc domain.Document, key string) domain.GetSetter {
	return &DocGetSetter{Doc: doc, Key: key}
}

// Get implements [domain.GetSetter].
func (d *DocGetSetter) Get() (value any, defined bool) {
	return d.Doc.Get(d.Key), d.Doc.Has(d.Key)
}

// Set implements [domain.GetSetter].
func (d *DocGetSetter) Set(value any) {
	d.Doc.Set(d.Key, value)
}

// Unset implements [domain.GetSetter].
func (d *DocGetSetter) Unset() {
	d.Doc.Unset(d.Key)
}

// ReadOnlyGetSetter is a [domain.GetSetter] that can only read.
// [domain.GetSetter.Set] and [domain.GetSetter.Unset] are no-op.
type ReadOnlyGetSetter struct {
	V any
}

// NewReadOnlyGetSetter returns a new implementation of [domain.GetSetter] that
// can be read but cannot modified.
func NewReadOnlyGetSetter(v any) domain.GetSetter {
	return &ReadOnlyGetSetter{V: v}
}

// Get implements [domain.GetSetter].
func (r *ReadOnlyGetSetter) Get() (value any, defined bool) {
	return r.V, true
}

// Set implements [domain.GetSetter].
func (r *ReadOnlyGetSetter) Set(any) {
}

// Unset implements [domain.GetSetter].
func (r *ReadOnlyGetSetter) Unset() {}

// EmptyGetSetter implements [domain.EmptyGetSetter].
type EmptyGetSetter struct{}

// NewGetSetterEmpty returns a new [domain.GetSetter] of an undefined value.
func NewGetSetterEmpty() domain.GetSetter {
	return &EmptyGetSetter{}
}

// Get implements [domain.GetSetter].
func (gs *EmptyGetSetter) Get() (any, bool) { return nil, false }

// Set implements [domain.GetSetter].
func (gs *EmptyGetSetter) Set(any) {}

// Unset implements [domain.GetSetter].
func (gs *EmptyGetSetter) Unset() {}
