// Package decoder contains the default [domain.Decoder] implementation.
package decoder

import (
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
func (d *Decoder) Decode(src any, tgt any) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "gedb",
		Result:  tgt,
	})
	if err != nil {
		return err
	}
	return dec.Decode(src)
}
