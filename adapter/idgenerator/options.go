package idgenerator

import "io"

// WithReader sets the reader that will provide random bytes.
func WithReader(r io.Reader) Option {
	return func(igo *IDGenerator) {
		igo.reader = r
	}
}

// Option configures behavior through the functional options pattern.
type Option func(*IDGenerator)
