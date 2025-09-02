package domain

import (
	"fmt"
	"math"
)

// ErrTargetNil is returned when the passed target, which should be a pointer,
// is passed as a nil value.
type ErrTargetNil struct{}

func (e *ErrTargetNil) Error() string { return "target interface is nil" }

type ErrBufferReset struct{}

func (e ErrBufferReset) Error() string { return "executor buffer was reset" }

type ErrCorruptFiles struct {
	CorruptionRate        float64
	CorruptItems          int
	DataLength            int
	CorruptAlertThreshold float64
}

func (e ErrCorruptFiles) Error() string {
	return fmt.Sprintf("%f%% of the data file is corrupt, more than given corruptAlertThreshold (%f%%). Cautiously refusing to start GeDB to prevent dataloss.", math.Floor(100*e.CorruptionRate), math.Floor(100*e.CorruptAlertThreshold))
}

type ErrFlushToStorage struct {
	ErrorOnFsync error
	ErrorOnClose error
}

func (e ErrFlushToStorage) Error() string {
	var err error
	if e.ErrorOnFsync != nil {
		err = e.ErrorOnFsync
	} else {
		err = e.ErrorOnClose
	}
	return fmt.Sprint("storage flush error:", err.Error())
}
