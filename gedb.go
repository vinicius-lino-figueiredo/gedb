package gedb

import (
	"context"
	"io"
	"iter"
	"os"
	"time"
)

// GEDB defines the main interface for interacting with the embedded database,
// modeled after the NeDB (Node.js) API. It provides basic persistence,
// indexing, and query functionality with context-aware operations.
//
// All data is stored locally on disk, and operations are safe to use
// concurrently from multiple goroutines.
type GEDB interface {
	// LoadDatabase initializes or loads the database file, preparing it for
	// further operations. Must be called before using other methods except
	// for in-memory-only databases.
	LoadDatabase(ctx context.Context) error

	// DropDatabase permanently deletes all data and removes the database
	// file, if any.
	DropDatabase(ctx context.Context) error

	// CompactDatafile rewrites the data file to remove duplicates caused by
	// append-only file format. Useful to reduce file size.
	CompactDatafile(ctx context.Context) error

	// GetAllData returns a cursor over all documents in the datastore.
	GetAllData(ctx context.Context) (Cursor, error)

	// EnsureIndex creates an index on one or more fields to improve query
	// performance. If the index already exists, this is a no-op.
	EnsureIndex(ctx context.Context, options EnsureIndexOptions) error

	// RemoveIndex deletes an existing index by field name(s).
	RemoveIndex(ctx context.Context, fieldNames []string) error

	// Insert adds one or more documents to the database and returns the
	// stored versions, including generated metadata like IDs. Documents
	// must be structs or maps.
	Insert(ctx context.Context, newDocs ...any) ([]Document, error)

	// Count returns the number of documents matching the given query.
	Count(ctx context.Context, query any) (int64, error)

	// Find returns a cursor over all documents matching the query. A
	// projection can be used to control which fields are returned.
	Find(ctx context.Context, query any, options FindOptions) (Cursor, error)

	// FindOne returns a cursor over the first document matching the query.
	FindOne(ctx context.Context, query any, target any, options FindOptions) error

	// Update modifies documents that match the query using the updateQuery.
	// Returns the number of documents updated.
	Update(ctx context.Context, query any, updateQuery any, options UpdateOptions) ([]Document, error)

	// Remove deletes documents matching the query. Returns the number of
	// documents removed.
	Remove(ctx context.Context, query any, options RemoveOptions) (int64, error)

	// WaitCompaction blocks until the ongoing compaction process (if any)
	// completes.
	WaitCompaction(ctx context.Context) error
}

type FindOptions struct {
	Projection any
	Skip       int64
	Limit      int64
	Sort       any
}

type Cursor interface {
	Exec(ctx context.Context, target any) error
	Next() bool
	Err() error
	Close() error
}

type DatastoreOptions struct {
	Filename              string
	TimestampData         bool
	InMemoryOnly          bool
	Autoload              bool
	Serializer            Serializer
	Deserializer          Deserializer
	CorruptAlertThreshold float64
	Comparer              Comparer
	FileMode              os.FileMode
	DirMode               os.FileMode
	Persistence           Persistence
	Storage               Storage
	IndexFactory          func(IndexOptions) Index
	DocumentFactory       func(any) (Document, error)
	Decoder               Decoder
	Matcher               Matcher
	CursorFactory         func(context.Context, []Document, CursorOptions) (Cursor, error)
	Modifier              Modifier
	TimeGetter            TimeGetter
	Hasher                Hasher
}

type Serializer interface {
	Serialize(context.Context, any) ([]byte, error)
}

// SerializeFunc implements Serializer.
type SerializeFunc func(context.Context, any) ([]byte, error)

// Serialize implements Serializer.
func (s SerializeFunc) Serialize(ctx context.Context, v any) ([]byte, error) { return s(ctx, v) }

type Deserializer interface {
	Deserialize(context.Context, []byte, any) error
}

// DeserializeFunc implements Deserializer.
type DeserializeFunc func(context.Context, []byte, any) error

// Deserialize implements Deserializer.
func (s DeserializeFunc) Deserialize(ctx context.Context, b []byte, v any) error {
	return s(ctx, b, v)
}

type UpdateOptions struct {
	Multi             bool
	Upsert            bool
	ReturnUpdatedDocs bool
}

type RemoveOptions struct {
	Multi bool
}

type EnsureIndexOptions struct {
	FieldNames  []string
	Unique      bool
	Sparse      bool
	ExpireAfter time.Duration
}

type Executor interface {
	Bufferize()
	Push(ctx context.Context, task func(context.Context), forceQueuing bool) error
	GoPush(ctx context.Context, task func(context.Context), forceQueuing bool) error
	ProcessBuffer()
	ResetBuffer()
}

type Index interface {
	GetAll() []Document
	GetBetweenBounds(ctx context.Context, query any) ([]Document, error)
	GetMatching(value ...any) ([]Document, error)
	Insert(ctx context.Context, docs ...Document) error
	Remove(ctx context.Context, docs ...Document) error
	Reset(ctx context.Context, newData ...Document) error
	RevertMultipleUpdates(ctx context.Context, pairs ...Update) error
	RevertUpdate(ctx context.Context, oldDoc Document, newDoc Document) error
	Update(ctx context.Context, oldDoc Document, newDoc Document) error
	UpdateMultipleDocs(ctx context.Context, pairs ...Update) error

	FieldName() string
	Unique() bool
	Sparse() bool
}

type IndexOptions struct {
	FieldName       string
	Unique          bool
	Sparse          bool
	ExpireAfter     time.Duration
	DocumentFactory func(any) (Document, error)
	Comparer        Comparer
	Hasher          Hasher
}

type Update struct {
	OldDoc Document
	NewDoc Document
}

// Document represents a record in the persistence layer, used internally to
// carry raw data from persistence to a user-defined type via a cursor. It's
// not returned directly by the Datastore. Document is read by one goroutine at
// a time and doesn't need to be concurrency safe.
type Document interface {
	// ID returns the document ID, if any, or an empty string.
	ID() any
	// D returns the subdocument for the given key, if any.
	D(string) Document
	// Get returns the value under the given key, or nil if unset.
	Get(string) any
	// Set sets the value under the given key.
	Set(string, any)
	// Iter returns an unordered sequence of key-value pairs in the
	// document.
	Iter() iter.Seq2[string, any]
	// Keys returns an unordered sequence of keys in the document.
	Keys() iter.Seq[string]
	// Values returns an unordered sequence of values in the document.
	Values() iter.Seq[any]
	// Has reports whether a value is set under the given key.
	Has(string) bool
	// Len returns the number of set fields in the document.
	Len() int
}

type PersistenceOptions struct {
	Filename              string
	InMemoryOnly          bool
	CorruptAlertThreshold float64
	FileMode              os.FileMode
	DirMode               os.FileMode
	Serializer            Serializer
	Deserializer          Deserializer
	Storage               Storage
	Decoder               Decoder
	Comparer              Comparer
	DocumentFactory       func(any) (Document, error)
	Hasher                Hasher
}

type IndexDTO struct {
	IndexCreated IndexCreated `json:"$$indexCreated" gedb:"$$indexCreated,omitzero"`
	IndexRemoved bool         `json:"$$indexRemoved" gedb:"$$indexRemoved,omitzero"`
}
type IndexCreated struct {
	FieldName   string  `json:"fieldName" gedb:"fieldName,omitzero"`
	Unique      bool    `json:"unique" gedb:"unique,omitzero"`
	Sparse      bool    `json:"sparse" gedb:"sparse,omitzero"`
	ExpireAfter float64 `json:"$$expireAfterSeconds" gedb:"$$expireAfterSeconds,omitzero"`
}

type Storage interface {
	AppendFile(string, os.FileMode, []byte) (int, error)
	Exists(string) (bool, error)
	EnsureParentDirectoryExists(string, os.FileMode) error
	EnsureDatafileIntegrity(string, os.FileMode) error
	CrashSafeWriteFileLines(string, [][]byte, os.FileMode, os.FileMode) error
	ReadFileStream(string, os.FileMode) (io.ReadCloser, error)
	Remove(string) error
}

type Persistence interface {
	DropDatabase(ctx context.Context) error
	LoadDatabase(ctx context.Context) ([]Document, map[string]IndexDTO, error)
	PersistNewState(ctx context.Context, newDocs ...Document) error
	SetCorruptAlertThreshold(v float64)
	TreadRawStream(ctx context.Context, rawStream io.Reader) ([]Document, map[string]IndexDTO, error)
	WaitCompaction(ctx context.Context) error
	PersistCachedDatabase(ctx context.Context, allData []Document, indexes map[string]IndexDTO) error
}

type Decoder interface {
	Decode(any, any) error
}

type CursorOptions struct {
	Query           Document
	Limit           int64
	Skip            int64
	Sort            map[string]int64
	Projection      map[string]uint64
	Matcher         Matcher
	Decoder         Decoder
	DocumentFactory func(any) (Document, error)
	Comparer        Comparer
}

type Matcher interface {
	Match(Document, Document) (bool, error)
}

type Comparer interface {
	Compare(any, any) (int, error)
}

type Modifier interface {
	Modify(Document, Document) (Document, error)
}

type TimeGetter interface {
	GetTime() time.Time
}

type Hasher interface {
	Hash(any) (uint64, error)
}
