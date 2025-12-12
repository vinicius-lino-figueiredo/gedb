package datastore

import (
	"io"
	"os"

	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

// WithFilename sets the database filename for the datastore.
func WithFilename(f string) Option {
	return func(dso *Datastore) {
		dso.filename = f
	}
}

// WithTimestamps enables automatic timestamping of documents with createdAt and
// updatedAt fields.
func WithTimestamps(t bool) Option {
	return func(dso *Datastore) {
		dso.timestampData = t
	}
}

// WithInMemoryOnly enables in-memory only mode without file persistence.
func WithInMemoryOnly(i bool) Option {
	return func(dso *Datastore) {
		dso.inMemoryOnly = i
	}
}

// WithSerializer sets the serializer for converting documents to bytes.
func WithSerializer(s domain.Serializer) Option {
	return func(dso *Datastore) {
		dso.serializer = s
	}
}

// WithDeserializer sets the deserializer for converting bytes to documents.
func WithDeserializer(d domain.Deserializer) Option {
	return func(dso *Datastore) {
		dso.deserializer = d
	}
}

// WithCorruptionThreshold sets the threshold for corruption warnings.
func WithCorruptionThreshold(c float64) Option {
	return func(dso *Datastore) {
		dso.corruptAlertThreshold = c
	}
}

// WithComparer sets the comparer for value comparison operations.
func WithComparer(c domain.Comparer) Option {
	return func(dso *Datastore) {
		dso.comparer = c
	}
}

// WithFileMode sets the file permissions for database files.
func WithFileMode(f os.FileMode) Option {
	return func(dso *Datastore) {
		dso.fileMode = f
	}
}

// WithDirMode sets the directory permissions for database directories.
func WithDirMode(d os.FileMode) Option {
	return func(dso *Datastore) {
		dso.dirMode = d
	}
}

// WithPersistence sets the persistence implementation for data storage.
func WithPersistence(p domain.Persistence) Option {
	return func(dso *Datastore) {
		dso.persistence = p
	}
}

// WithStorage sets the storage implementation for low-level file operations.
func WithStorage(s domain.Storage) Option {
	return func(dso *Datastore) {
		dso.storage = s
	}
}

// WithIndexFactory sets the factory function for creating index instances.
func WithIndexFactory(i domain.IndexFactory) Option {
	return func(dso *Datastore) {
		dso.indexFactory = i
	}
}

// WithDocumentFactory sets the factory function for creating document instances.
func WithDocumentFactory(d domain.DocumentFactory) Option {
	return func(dso *Datastore) {
		dso.documentFactory = d
	}
}

// WithDecoder sets the decoder for data format conversions.
func WithDecoder(d domain.Decoder) Option {
	return func(dso *Datastore) {
		dso.decoder = d
	}
}

// WithMatcher sets the matcher implementation for query evaluation.
func WithMatcher(m domain.Matcher) Option {
	return func(dso *Datastore) {
		dso.matcher = m
	}
}

// WithCursorFactory sets the factory function for creating cursor instances.
func WithCursorFactory(c domain.CursorFactory) Option {
	return func(dso *Datastore) {
		dso.cursorFactory = c
	}
}

// WithModifier sets the modifier implementation for document updates.
func WithModifier(m domain.Modifier) Option {
	return func(dso *Datastore) {
		dso.modifier = m
	}
}

// WithTimeGetter sets the time getter for timestamping operations.
func WithTimeGetter(t domain.TimeGetter) Option {
	return func(dso *Datastore) {
		dso.timeGetter = t
	}
}

// WithHasher sets the hasher for generating hash values.
func WithHasher(h domain.Hasher) Option {
	return func(dso *Datastore) {
		dso.hasher = h
	}
}

// WithFieldNavigator sets the field getter for accessing document fields.
func WithFieldNavigator(f domain.FieldNavigator) Option {
	return func(dso *Datastore) {
		dso.fieldNavigator = f
	}
}

// WithIDGenerator sets the idgenerator to create new document ids.
func WithIDGenerator(ig domain.IDGenerator) Option {
	return func(dso *Datastore) {
		dso.idGenerator = ig
	}
}

// WithRandomReader sets the reader to be used by the IDGenerator.
func WithRandomReader(r io.Reader) Option {
	return func(dso *Datastore) {
		dso.randomReader = r
	}
}

// Option configures datastore behavior through the functional options
// pattern.
type Option func(*Datastore)
