// Package decoder contains the default [domain.Decoder] implementation.
package decoder

import (
	"fmt"

	"github.com/goccy/go-reflect"
	"github.com/mitchellh/mapstructure"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var docReflectType = reflect.TypeOf((*domain.Document)(nil)).Elem()

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

	value := reflect.ValueNoEscapeOf(target)
	if value.Kind() != reflect.Ptr {
		return domain.ErrNonPointer
	}

	if !value.Type().Elem().Implements(docReflectType) {
		source = d.adjustDoc(source)
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

func (d *Decoder) adjustDoc(value any) any {
	switch t := value.(type) {
	case domain.Document:
		doc := make(map[string]any, t.Len())
		for k, v := range t.Iter() {
			doc[k] = d.adjustDoc(v)
		}
		return doc
	case []any:
		lst := make([]any, len(t))
		for n, v := range t {
			lst[n] = d.adjustDoc(v)
		}
		return lst
	default:
		return value
	}
}
