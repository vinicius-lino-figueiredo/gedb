package lib

import "github.com/vinicius-lino-figueiredo/gedb"

// Matcher implements gedb.Matcher.
type Matcher struct{}

// NewMatcher returns a new implementation of gedb.Matcher.
func NewMatcher() gedb.Matcher {
	return &Matcher{}
}

// Match implements gedb.Matcher.
func (m *Matcher) Match(any, any) (bool, error) {
	panic("unimplemented")
}
