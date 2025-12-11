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
	reader io.Reader
}

// NewIDGenerator implements [domain.IDGenerator].
func NewIDGenerator(opts ...Option) domain.IDGenerator {
	i := IDGenerator{reader: rand.Reader}
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
	enc := base64.StdEncoding.EncodeToString(buf)
	return strings.NewReplacer("+", "", "/", "").Replace(enc)[:l], nil
}
