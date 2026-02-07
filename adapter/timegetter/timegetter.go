// Package timegetter contains the default [domain.TimeGetter] implementation.
package timegetter

import (
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// TimeGetter implements [domain.TimeGetter].
type TimeGetter struct{}

// NewTimeGetter returns a new implementation of domain.TimeGetter.
func NewTimeGetter() domain.TimeGetter {
	return &TimeGetter{}
}

// GetTime implements [domain.TimeGetter].
func (t *TimeGetter) GetTime() time.Time {
	return time.Now()
}
