package lib

import (
	"github.com/mitchellh/mapstructure"
	"github.com/vinicius-lino-figueiredo/gedb"
)

// Decoder implements gedb.Decoder.
type Decoder struct{}

// NewDecoder returns a new implementation of gedb.Decoder.
func NewDecoder() gedb.Decoder {
	return &Decoder{}
}

// Decode implements gedb.Decoder.
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
