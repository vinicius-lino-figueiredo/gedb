package fieldnavigator

import "github.com/vinicius-lino-figueiredo/gedb/domain"

// GetSetter implements [domain.GetSetter].
type GetSetter struct {
	get   func() (value any, defined bool)
	set   func(any)
	unset func()
}

// NewGetSetterWithArrayIndex returns a new implementation of [domain.GetSetter]
// that will represent a value from a slice of [any].
func NewGetSetterWithArrayIndex(array []any, index int) domain.GetSetter {
	return &GetSetter{
		get: func() (any, bool) {
			if index >= 0 && index < len(array) {
				return array[index], true
			}
			return nil, false
		},
		set: func(value any) {
			if index >= 0 && index < len(array) {
				array[index] = value
			}
		},
		unset: func() {
			if index >= 0 && index < len(array) {
				array[index] = nil
			}
		},
	}
}

// NewGetSetterWithDoc returns a new implementation of [domain.GetSetter] that
// will represent a value from a [domain.Document].
func NewGetSetterWithDoc(doc domain.Document, key string) domain.GetSetter {
	return &GetSetter{
		get:   func() (any, bool) { return doc.Get(key), doc.Has(key) },
		set:   func(value any) { doc.Set(key, value) },
		unset: func() { doc.Unset(key) },
	}
}

// NewReadOnlyGetSetter returns a new implementation of [domain.GetSetter] that
// can be read but cannot modified.
func NewReadOnlyGetSetter(v any) domain.GetSetter {
	return &GetSetter{
		get: func() (any, bool) { return v, true },
	}
}

// NewGetSetterEmpty returns a new [domain.GetSetter] of an undefined value.
func NewGetSetterEmpty() domain.GetSetter {
	return &GetSetter{}
}

// Get implements [domain.GetSetter].
func (gs *GetSetter) Get() (value any, defined bool) {
	if gs.get != nil {
		return gs.get()
	}
	return nil, false
}

// Set implements [domain.GetSetter].
func (gs *GetSetter) Set(value any) {
	if gs.set != nil {
		gs.set(value)
	}
}

// Unset implements [domain.GetSetter].
func (gs *GetSetter) Unset() {
	if gs.unset != nil {
		gs.unset()
	}
}
