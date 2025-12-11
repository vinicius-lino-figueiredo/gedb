package persistence

import (
	"os"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// WithFilename sets the database filename for persistence.
func WithFilename(f string) Option {
	return func(po *Persistence) {
		po.filename = f
	}
}

// WithInMemoryOnly enables in-memory only mode without file
// persistence.
func WithInMemoryOnly(i bool) Option {
	return func(po *Persistence) {
		po.inMemoryOnly = i
	}
}

// WithCorruptAlertThreshold sets the threshold for corruption
// warnings.
func WithCorruptAlertThreshold(c float64) Option {
	return func(po *Persistence) {
		po.corruptAlertThreshold = c
	}
}

// WithFileMode sets the file permissions for database files.
func WithFileMode(f os.FileMode) Option {
	return func(po *Persistence) {
		po.fileMode = f
	}
}

// WithDirMode sets the directory permissions for database
// directories.
func WithDirMode(d os.FileMode) Option {
	return func(po *Persistence) {
		po.dirMode = d
	}
}

// WithSerializer sets the serializer for converting documents to
// bytes.
func WithSerializer(s domain.Serializer) Option {
	return func(po *Persistence) {
		po.serializer = s
	}
}

// WithDeserializer sets the deserializer for converting bytes to
// documents.
func WithDeserializer(d domain.Deserializer) Option {
	return func(po *Persistence) {
		po.deserializer = d
	}
}

// WithStorage sets the storage implementation for file operations.
func WithStorage(s domain.Storage) Option {
	return func(po *Persistence) {
		po.storage = s
	}
}

// WithDecoder sets the decoder for data format conversions.
func WithDecoder(d domain.Decoder) Option {
	return func(po *Persistence) {
		po.decoder = d
	}
}

// WithComparer sets the comparer for value comparison operations.
func WithComparer(c domain.Comparer) Option {
	return func(po *Persistence) {
		po.comparer = c
	}
}

// WithDocFactory sets the factory function for creating
// documents.
func WithDocFactory(d func(any) (domain.Document, error)) Option {
	return func(po *Persistence) {
		po.documentFactory = d
	}
}

// WithHasher sets the hasher for generating hash values.
func WithHasher(h domain.Hasher) Option {
	return func(po *Persistence) {
		po.hasher = h
	}
}

// Option configures persistence behavior through the functional
// options pattern.
type Option func(*Persistence)
