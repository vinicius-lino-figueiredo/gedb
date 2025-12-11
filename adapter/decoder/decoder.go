// Package decoder contains the default [domain.Decoder] implementation.
package decoder

import (
	"fmt"

	"github.com/goccy/go-reflect"
	"github.com/mitchellh/mapstructure"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// Decoder implements domain.Decoder.
type Decoder struct{}

// NewDecoder returns a new implementation of domain.Decoder.
func NewDecoder() domain.Decoder {
	return &Decoder{}
}

// Decode implements domain.Decoder.
func (d *Decoder) Decode(source any, target any) error {
	if target == nil {
		return domain.ErrTargetNil
	}
	if reflect.ValueNoEscapeOf(target).Kind() != reflect.Ptr {
		return domain.ErrNonPointer
	}
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "gedb",
		Result:  target,
	})
	if err != nil {
		return err
	}
	if err := dec.Decode(source); err != nil {
		errDec := domain.ErrDecode{Source: source, Target: target}
		return fmt.Errorf("%w: %w", errDec, err)
	}
	return nil
}
