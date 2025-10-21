// Package domain contains domain-specific interfaces and option types for GEDB.
//
// This package defines the core interfaces that must be implemented by
// adapters, as well as functional options for configuring various components
// like queries, updates, indexes, cursors, matchers, and persistence.
package domain

import (
	"context"
	"io"
	"iter"
	"os"
	"time"
)

// Serializer converts documents to bytes for storage.
type Serializer interface {
	// Serialize converts a document to bytes for persistence.
	Serialize(context.Context, any) ([]byte, error)
}

// Deserializer converts bytes back to documents.
type Deserializer interface {
	// Deserialize converts bytes back to a document.
	Deserialize(context.Context, []byte, any) error
}

// Storage provides low-level file operations with crash-safety guarantees.
type Storage interface {
	// AppendFile appends data to a file, creating it if necessary.
	AppendFile(string, os.FileMode, []byte) (int, error)
	// Exists checks if a file exists.
	Exists(string) (bool, error)
	// EnsureParentDirectoryExists creates parent directories if needed.
	EnsureParentDirectoryExists(string, os.FileMode) error
	// EnsureDatafileIntegrity verifies or repairs file integrity.
	EnsureDatafileIntegrity(string, os.FileMode) error
	// CrashSafeWriteFileLines atomically writes multiple lines to a file.
	CrashSafeWriteFileLines(string, [][]byte, os.FileMode, os.FileMode) error
	// ReadFileStream opens a file for streaming reads.
	ReadFileStream(string, os.FileMode) (io.ReadCloser, error)
	// Remove deletes a file.
	Remove(string) error
}

// Decoder converts between different data representations.
type Decoder interface {
	// Decode converts from one data format to another.
	Decode(any, any) error
}

// Comparer provides ordering and comparison operations for different data types.
type Comparer interface {
	// Compare returns -1, 0, or 1 based on the comparison of two values.
	Compare(any, any) (int, error)
	// Comparable returns true if two values can be compared.
	Comparable(any, any) bool
}

// TimeGetter provides current time for timestamping operations.
type TimeGetter interface {
	// GetTime returns the current time.
	GetTime() time.Time
}

// Getter represents a value that can be treated as undefined.
type Getter interface {
	// Get returns the value for the given address and a bool that indicates
	// whether the value counts as defined or not. Unset values are
	// inaccessible for some reason. If an address points to an unset key in
	// a document, or an out of bounds index in an array or any address
	// within a primitive value ([string], [bool], etc.), it counts as
	// undefined. If a value is explicitly [nil], it will not count as
	// undefined.
	Get() (value any, defined bool)
}

// GetSetter represents a value in a [Document]. It will be returned by
// [FieldNavigator] so things like identifying unset values and appending to
// nested arrays becomes easier. Default GetSetter IS NOT concurrency safe, but
// other implementations might be.
type GetSetter interface {
	// GetSetter implements [Getter]. Undefined values can neither be set
	// nor unset.
	Getter
	// Set will set a new value for the address.
	Set(any)
	// Unset removes the given value from the parent item (object or array).
	Unset()
}

// FieldNavigator provides field access operations with dot notation support.
type FieldNavigator interface {
	// GetField extracts values from nested documents, following path parts.
	GetField(any, ...string) ([]GetSetter, bool, error)
	EnsureField(any, ...string) ([]GetSetter, error)
	// GetAddress extracts nested path from the string address using the
	// expected notation.
	GetAddress(field string) ([]string, error)
	// SplitFields parses compound field names into individual field
	// components.
	SplitFields(string) ([]string, error)
}

// Hasher generates hash values for data deduplication and indexing.
type Hasher interface {
	// Hash generates a hash value for the given data.
	Hash(any) (uint64, error)
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
	// Unset unsets the value under the given key.
	Unset(string)
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

// Matcher evaluates whether values match query criteria.
type Matcher interface {
	// Match returns true if the value matches the query.
	Match(any, any) (bool, error)
}

// Modifier applies update operations to documents.
type Modifier interface {
	// Modify applies an update query to a document and returns the result.
	Modify(Document, Document) (Document, error)
}

// Persistence manages database serialization and file operations.
type Persistence interface {
	// DropDatabase permanently deletes all persisted data.
	DropDatabase(ctx context.Context) error
	// LoadDatabase reads the database from storage and returns documents and indexes.
	LoadDatabase(ctx context.Context) ([]Document, map[string]IndexDTO, error)
	// PersistNewState appends new documents to the persistence layer.
	PersistNewState(ctx context.Context, newDocs ...Document) error
	// SetCorruptAlertThreshold sets the threshold for corruption warnings.
	SetCorruptAlertThreshold(v float64)
	// TreatRawStream parses a raw data stream and extracts documents and indexes.
	TreatRawStream(ctx context.Context, rawStream io.Reader) ([]Document, map[string]IndexDTO, error)
	// WaitCompaction blocks until any running compaction process completes.
	WaitCompaction(ctx context.Context) error
	// PersistCachedDatabase writes all data and indexes to storage in one operation.
	PersistCachedDatabase(ctx context.Context, allData []Document, indexes map[string]IndexDTO) error
}

// Cursor provides iteration over query results with pagination support.
type Cursor interface {
	// Scan executes the cursor and decodes all results into the target slice.
	Scan(ctx context.Context, target any) error
	// Next advances the cursor to the next document, returning true if available.
	Next() bool
	// Err returns any error that occurred during iteration.
	Err() error
	// Close releases cursor resources and should be called when done.
	Close() error
}

// Index provides fast document lookups based on field values.
type Index interface {
	// GetAll returns all documents in the index.
	GetAll() []Document
	// GetBetweenBounds returns documents matching range queries.
	GetBetweenBounds(ctx context.Context, query Document) ([]Document, error)
	// GetMatching returns documents with the specified field values.
	GetMatching(value ...any) ([]Document, error)
	// Insert adds documents to the index.
	Insert(ctx context.Context, docs ...Document) error
	// Remove removes documents from the index.
	Remove(ctx context.Context, docs ...Document) error
	// Reset clears the index and re-inserts the provided documents.
	Reset(ctx context.Context, newData ...Document) error
	// RevertMultipleUpdates undoes multiple document updates in the index.
	RevertMultipleUpdates(ctx context.Context, pairs ...Update) error
	// RevertUpdate undoes a single document update in the index.
	RevertUpdate(ctx context.Context, oldDoc Document, newDoc Document) error
	// Update modifies a document's index entry.
	Update(ctx context.Context, oldDoc Document, newDoc Document) error
	// UpdateMultipleDocs modifies multiple documents' index entries.
	UpdateMultipleDocs(ctx context.Context, pairs ...Update) error
	// GetNumberOfKeys returns the number of unique keys in the index.
	GetNumberOfKeys() int
	// FieldName returns the field name(s) this index covers.
	FieldName() string
	// Unique returns true if this is a unique index.
	Unique() bool
	// Sparse returns true if this index excludes null/undefined values.
	Sparse() bool
}

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
	EnsureIndex(ctx context.Context, options ...EnsureIndexOption) error

	// RemoveIndex deletes an existing index by field name(s).
	RemoveIndex(ctx context.Context, fieldNames ...string) error

	// Insert adds one or more documents to the database and returns the
	// stored versions, including generated metadata like IDs. Documents
	// must be structs or maps.
	Insert(ctx context.Context, newDocs ...any) (Cursor, error)

	// Count returns the number of documents matching the given query.
	Count(ctx context.Context, query any) (int64, error)

	// Find returns a cursor over all documents matching the query. A
	// projection can be used to control which fields are returned.
	Find(ctx context.Context, query any, options ...FindOption) (Cursor, error)

	// FindOne returns a cursor over the first document matching the query.
	FindOne(ctx context.Context, query any, target any, options ...FindOption) error

	// Update modifies documents that match the query using the updateQuery.
	// Returns the number of documents updated.
	Update(ctx context.Context, query any, updateQuery any, options ...UpdateOption) (Cursor, error)

	// Remove deletes documents matching the query. Returns the number of
	// documents removed.
	Remove(ctx context.Context, query any, options ...RemoveOption) (int64, error)

	// WaitCompaction blocks until the ongoing compaction process (if any)
	// completes.
	WaitCompaction(ctx context.Context) error
}
