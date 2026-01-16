package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrConstraintViolated is returned by [Index] when an action cannot be
	// performed because it is being blocked by an index constraint.
	ErrConstraintViolated = errors.New("constraint violated")
	// ErrCursorClosed is returned when trying to perform operations on a
	// closed [Cursor].
	ErrCursorClosed = errors.New("cursor is closed")
	// ErrScanBeforeNext is returned when calling [Cursor.Scan] before
	// calling [Cursor.Next].
	ErrScanBeforeNext = errors.New("called Scan before calling Next")
	// ErrNoFieldName is returned if no field name is provided when creating
	// a new [Index].
	ErrNoFieldName = errors.New("field name should be passed")
	// ErrNotFound is returned when [GEDB.FindOne] cannot find any matching
	// result for the given query.
	ErrNotFound = errors.New("expected 1 record, got 0")
	// ErrTargetNil is returned when user provides a nil value as a target
	// to decode data, for example, calling [GEDB.FindOne].
	ErrTargetNil = errors.New("target interface is nil")
	// ErrCannotModifyID is returned by [Modifier.Modify] when the user
	// performs some action that would modify a document _id.
	ErrCannotModifyID = errors.New("cannot modify document _id")
)

// ErrFieldName represents an invalid field name, usually for when a document is
// created with a reserved prefix or forbidden character.
type ErrFieldName struct {
	Field  string
	Reason string
}

// Error implements [error].
func (e ErrFieldName) Error() string {
	return fmt.Sprintf("invalid field name %q: %s", e.Field, e.Reason)
}

// ErrDatafileName is returned when the user specifies an invalid name for data
// file. That usually happens if a file with the suffix reserved for the crash
// backup file is passed as a file name.
type ErrDatafileName struct {
	Name   string
	Reason string
}

// Error implements [error].
func (e ErrDatafileName) Error() string {
	return fmt.Sprintf("invalid datafile name %q: %s", e.Name, e.Reason)
}

// ErrDocumentType is returned when an user passes a value that is invalid or
// contains an invalid sub value for creating a document.
type ErrDocumentType struct {
	Reason string
}

// Error implements [error].
func (e ErrDocumentType) Error() string {
	return fmt.Sprintf("invalid doc instantiation: %s", e.Reason)
}

// ErrCannotCompare is returned when [Comparer.Compare] is called with two
// values that cannot be compared by the current [Comparer] interface.
type ErrCannotCompare struct {
	A, B any
}

// Error implements [error].
func (e ErrCannotCompare) Error() string {
	return fmt.Sprintf("cannot compare %+v and %+v", e.A, e.B)
}

// ErrCorruptFiles is returned by methods [GEDB.LoadDatabase] and
// [Persistence.LoadDatabase] when the db is unable to correctly load more data
// in the datafile than the minimum threshold set by the user.
type ErrCorruptFiles struct {
	CorruptionRate        float64
	CorruptItems          int
	DataLength            int
	CorruptAlertThreshold float64
}

// Error implements [error].
func (e ErrCorruptFiles) Error() string {
	return fmt.Sprintf(
		"corrupted %.2f%% (%d of %d) exceeded threshold %.2f%%",
		100*e.CorruptionRate,
		e.CorruptItems, e.DataLength,
		100*e.CorruptAlertThreshold,
	)
}

// ErrDecode is returned by [Decoder.Decode] to easily wrap third party decoding
// errors.
type ErrDecode struct {
	Source, Target any
}

// Error implements [error].
func (e ErrDecode) Error() string {
	return fmt.Sprintf("cannot decode %+s into %T", e.Source, e.Target)
}
