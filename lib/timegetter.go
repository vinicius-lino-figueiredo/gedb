package lib

import (
	"github.com/vinicius-lino-figueiredo/gedb"
	"time"
)

// TimeGetter implements gedb.TimeGetter.
type TimeGetter struct{}

// NewTimeGetter returns a new implementation of gedb.TimeGetter.
func NewTimeGetter() gedb.TimeGetter {
	return &TimeGetter{}
}

// GetTime implements gedb.TimeGetter.
func (t *TimeGetter) GetTime() time.Time {
	return time.Now()
}
