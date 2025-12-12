package querier

import "github.com/vinicius-lino-figueiredo/gedb/domain"

// WithDocumentFactory sets the factory function for creating documents.
func WithDocumentFactory(df domain.DocumentFactory) Option {
	return func(q *Querier) {
		q.docFac = df
	}
}

// WithMatcher sets the matcher implementation for querier evaluations.
func WithMatcher(m domain.Matcher) Option {
	return func(q *Querier) {
		q.mtchr = m
	}
}

// WithComparer sets the comparer implementation for sorting operations.
func WithComparer(c domain.Comparer) Option {
	return func(q *Querier) {
		q.cmpr = c
	}
}

// WithFieldNavigator sets the field getter for accessing document
// fields.
func WithFieldNavigator(f domain.FieldNavigator) Option {
	return func(q *Querier) {
		q.fn = f
	}
}

// WithProjector sets the implementation what will be sed to project
// the resultant documents.
func WithProjector(p domain.Projector) Option {
	return func(q *Querier) {
		q.proj = p
	}
}

// Option configures querier behavior through the functional options
// pattern.
type Option func(*Querier)
