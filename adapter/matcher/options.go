package matcher

import "github.com/vinicius-lino-figueiredo/gedb/domain"

// WithDocumentFactory sets the document factory for creating documents during
// matching.
func WithDocumentFactory(d domain.DocumentFactory) Option {
	return func(mo *Matcher) {
		mo.documentFactory = d
	}
}

// WithComparer sets the comparer implementation for value comparisons during
// matching.
func WithComparer(c domain.Comparer) Option {
	return func(mo *Matcher) {
		mo.comparer = c
	}
}

// WithFieldNavigator sets the field getter for accessing document fields during
// matching.
func WithFieldNavigator(f domain.FieldNavigator) Option {
	return func(mo *Matcher) {
		mo.fieldNavigator = f
	}
}

// Option configures matcher behavior through the functional options pattern.
type Option func(*Matcher)
