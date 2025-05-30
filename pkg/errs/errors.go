package errs

// ErrTargetNil is returned when the passed target, which should be a pointer,
// is passed as a nil value.
type ErrTargetNil struct{}

func (e *ErrTargetNil) Error() string { return "target interface is nil" }
