package projector

import "github.com/vinicius-lino-figueiredo/gedb/domain"

// WithFieldNavigator sets the [domain.FieldNavigator] that will be used by
// [Projector].
func WithFieldNavigator(fn domain.FieldNavigator) Option {
	return func(p *Projector) {
		p.fn = fn
	}
}

// WithDocumentFactory sets the [domain.Document] factory function that will be
// used by [Projector].
func WithDocumentFactory(df domain.DocumentFactory) Option {
	return func(p *Projector) {
		p.docFac = df
	}
}

// Option configures projector behavior through the functional options pattern.
type Option func(*Projector)
