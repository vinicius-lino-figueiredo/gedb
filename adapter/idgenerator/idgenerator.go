// Package idgenerator contains the default [domain.IDGenerator] implementation
// using base64-encoded random bytes.
package idgenerator

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"strings"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// IDGenerator implements [domain.IDGenerator].
type IDGenerator struct {
	reader   io.Reader
	replacer *strings.Replacer
}

// NewIDGenerator implements [domain.IDGenerator].
func NewIDGenerator(opts ...Option) domain.IDGenerator {
	i := IDGenerator{
		reader:   rand.Reader,
		replacer: strings.NewReplacer("+", "", "/", ""),
	}
	for _, opt := range opts {
		opt(&i)
	}
	return &i
}

// GenerateID implements [domain.IDGenerator].
func (i *IDGenerator) GenerateID(l int) (string, error) {
	buf := make([]byte, max(8, l*2))
	_, err := i.reader.Read(buf)
	if err != nil {
		return "", err
	}

	dst := base64.StdEncoding.EncodeToString(buf)

	res := make([]byte, l)
	w := 0
	for _, b := range []byte(dst) {
		switch b {
		case '+', '/':
		default:
			res[w] = b
			w++
		}
		if w == l {
			break
		}
	}

	return string(res), nil
}
