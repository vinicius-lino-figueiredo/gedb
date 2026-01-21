// Package gedb provides an embedded MongoDB-like database for golang.
//
// This package contains a new implementation of nedb. Most features there can
// be used here.
//
// The basic usage starts with creating a new [GEDB] instance, which can be done
// by calling [NewDB].
package gedb

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb/adapter/datastore"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var (
	// ErrConstraintViolated is returned by [Index] when an action cannot be
	// performed because it is being blocked by an index constraint.
	ErrConstraintViolated = domain.ErrConstraintViolated
	// ErrCursorClosed is returned when trying to perform operations on a
	// closed [Cursor].
	ErrCursorClosed = domain.ErrCursorClosed
	// ErrScanBeforeNext is returned when calling [Cursor.Scan] before
	// calling [Cursor.Next].
	ErrScanBeforeNext = domain.ErrScanBeforeNext
	// ErrNoFieldName is returned if no field name is provided when creating
	// a new [Index].
	ErrNoFieldName = domain.ErrNoFieldName
	// ErrNotFound is returned when [GEDB.FindOne] cannot find any matching
	// result for the given query.
	ErrNotFound = domain.ErrNotFound
	// ErrTargetNil is returned when user provides a nil value as a target
	// to decode data, for example, calling [GEDB.FindOne].
	ErrTargetNil = domain.ErrTargetNil
	// ErrCannotModifyID is returned by [Modifier.Modify] when the user
	// performs some action that would modify a document _id.
	ErrCannotModifyID = domain.ErrCannotModifyID
)

// ErrFieldName represents an invalid field name, usually for when a document is
// created with a reserved prefix or forbidden character.
type ErrFieldName = domain.ErrFieldName

// ErrDatafileName is returned when the user specifies an invalid name for data
// file. That usually happens if a file with the suffix reserved for the crash
// backup file is passed as a file name.
type ErrDatafileName = domain.ErrDatafileName

// ErrDocumentType is returned when an user passes a value that is invalid or
// contains an invalid sub value for creating a document.
type ErrDocumentType = domain.ErrDocumentType

// ErrCannotCompare is returned when [Comparer.Compare] is called with two
// values that cannot be compared by the current [Comparer] interface.
type ErrCannotCompare = domain.ErrCannotCompare

// ErrCorruptFiles is returned by methods [GEDB.LoadDatabase] and
// [Persistence.LoadDatabase] when the db is unable to correctly load more data
// in the datafile than the minimum threshold set by the user.
type ErrCorruptFiles = domain.ErrCorruptFiles

// ErrDecode is returned by [Decoder.Decode] to easily wrap third party decoding
// errors.
type ErrDecode = domain.ErrDecode

// NewDB creates a new GEDB instance with the provided configuration options:
//
// - [WithFilename]: sets the database filename for the datastore.
//
// - [WithTimestamps]: enables automatic timestamping of documents
//
// - [WithInMemoryOnly]: enables in-memory only mode without file persistence.
//
// - [WithSerializer]: sets the serializer for converting data to bytes.
//
// - [WithDeserializer]: sets the deserializer for converting bytes to data.
//
// - [WithCorruptionThreshold]: sets the threshold for corruption warnings.
//
// - [WithComparer]: sets the comparer for value comparison operations.
//
// - [WithFileMode]: sets the file permissions for database files.
//
// - [WithDirMode]: sets the directory permissions for database directories.
//
// - [WithPersistence]: sets the persistence implementation for data storage.
//
// - [WithStorage]: sets the storage implementation for file operations.
//
// - [WithIndexFactory]: sets the factory function for creating index instances.
//
// - [WithDocumentFactory]: sets the function for creating [Document] instances.
//
// - [WithDecoder]: sets the decoder for data format conversions.
//
// - [WithMatcher]: sets the matcher implementation for query evaluation.
//
// - [WithCursorFactory]: sets the function for creating cursor instances.
//
// - [WithModifier]: sets the modifier implementation for document updates.
//
// - [WithTimeGetter]: sets the time getter for timestamping operations.
//
// - [WithHasher]: sets the hasher for generating hash values.
//
// - [WithFieldNavigator]: sets the field getter for accessing document fields.
//
// - [WithIDGenerator]: sets the idgenerator to create new document ids.
//
// - [WithRandomReader]: sets the reader to be used by the IDGenerator.
func NewDB(options ...datastore.Option) (GEDB, error) {
	return datastore.NewDatastore(options...)
}

// GEDB defines the main interface for interacting with the embedded database,
// It provides data persistence, indexing, and query functionality with
// context-aware operations.
//
// All data is stored either in-memory only or locally on disk, and operations
// are safe to use concurrently from multiple goroutines.
//
// If set as in-memory-only, user can start using the db right away, but for
// persistent databases, [GEDB.LoadDatabase] should be called to load the
// datafile.
type GEDB interface {
	// LoadDatabase loads the database file, preparing it for further
	// operations. Must be called before using other methods except for
	// in-memory-only databases.
	LoadDatabase(ctx context.Context) error

	// DropDatabase permanently deletes all data and removes the database
	// file, if any.
	DropDatabase(ctx context.Context) error

	// CompactDatafile rewrites the data file to remove duplicates caused by
	// append-only file format.
	CompactDatafile(ctx context.Context) error

	// GetAllData returns a cursor over all documents in the datastore.
	GetAllData(ctx context.Context) (Cursor, error)

	// EnsureIndex creates an index on one or more fields to improve query
	// performance. If the index already exists, this is a no-op. Options
	// can be used to setup behavior:
	// - [WithFields]
	// - [WithUnique]
	// - [WithSparse]
	// - [WithTTL]
	EnsureIndex(ctx context.Context, options ...EnsureIndexOption) error

	// RemoveIndex deletes an existing index by field name(s).
	RemoveIndex(ctx context.Context, fieldNames ...string) error

	// Insert adds one or more documents to the database and returns the
	// stored versions, including generated metadata like IDs.
	//
	// If not reimplementing [DocumentFactory] defaults, Insert should
	// accept any structs or maps[string]T. Values can be nested structures
	// and/or arrays and slices.
	//
	// For structs, unexported fields will be ignored; If a field has a
	// "gedb" struct tag, it's value will replace the field name. If tag
	// value contains ",omitempty", [nil] values will not be set; If tag
	// value contains ",omitzero", uninitialized fields will not be set.
	Insert(ctx context.Context, newDocs ...any) (Cursor, error)

	// Count returns the number of documents matching the given query using
	// current [Matcher].
	Count(ctx context.Context, query any) (int64, error)

	// Find filters data using [Index] and [Matcher], and returns a cursor
	// over all documents matching the query. Options can also be added to
	// control result properties, those being:
	// - [WithProjection]
	// - [WithSkip]
	// - [WithLimit]
	// - [WithSort]
	Find(ctx context.Context, query any, options ...FindOption) (Cursor, error)

	// FindOne returns a cursor over the first document matching the query.
	// This method accepts the same options as [GEDB.Find], but skip option
	// will be replaced with 1.
	FindOne(ctx context.Context, query any, target any, options ...FindOption) error

	// Update modifies documents that match the query using the updateQuery.
	// Returns a [Cursor] containing the affected documents. The following
	// options can be provided to affect behavior:
	// - [WithUpsert]
	// - [WithUpdateMulti]
	Update(ctx context.Context, query any, updateQuery any, options ...UpdateOption) (Cursor, error)

	// Remove deletes documents matching the query using [Matcher]. Returns
	// the number of documents removed. By default, deletes only one
	// document. Option [WithRemoveMulti] can be provided to allow more than
	// one document.
	Remove(ctx context.Context, query any, options ...RemoveOption) (int64, error)

	// WaitCompaction waits until either context is canceled or a successful
	// compaction happens to data file. The method will never return an
	// error unless context is canceled.
	WaitCompaction(ctx context.Context) error
}

// Serializer converts documents to bytes for storage.
type Serializer = domain.Serializer

// Deserializer converts bytes back to documents.
type Deserializer = domain.Deserializer

// Storage provides low-level file operations with crash-safety guarantees.
type Storage = domain.Storage

// Decoder converts between different data representations.
type Decoder = domain.Decoder

// Comparer provides ordering and comparison for different data types.
type Comparer = domain.Comparer

// TimeGetter provides current time for timestamping operations.
type TimeGetter = domain.TimeGetter

// FieldNavigator provides field access operations with dot notation support.
type FieldNavigator = domain.FieldNavigator

// Hasher generates hash values for data deduplication and indexing.
type Hasher = domain.Hasher

// Document represents a record in the persistence layer.
type Document = domain.Document

// Matcher evaluates whether documents match query criteria.
type Matcher = domain.Matcher

// Modifier applies update operations to documents.
type Modifier = domain.Modifier

// Persistence manages database serialization and file operations.
type Persistence = domain.Persistence

// Cursor provides iteration over query results with pagination support.
type Cursor = domain.Cursor

// Index provides fast document lookups based on field values.
type Index = domain.Index

// IDGenerator is used to create unique IDs for new instances of [Document].
type IDGenerator = domain.IDGenerator

// Sort represents an ordered list of fields which should be used, respectively,
// to sort the results of a query.
type Sort = []SortName

// SortName represents a single field and the order which should be used to sort
// it, a positive value meaning ascending order and a negative value meaning
// descending order.
type SortName = domain.SortName

// DocumentFactory represents a [Document] constructor that can be
// reimplemented. It should accept structured data types and create an
// equivalent [Document], respecting the given structure. If nil is given as
// argument, a document of length 0 should be returned.
type DocumentFactory = domain.DocumentFactory

// CursorFactory represents a [Cursor] constructor that can be reimplemented. It
// should receive an ordered set of documents and allow user to scan them into
// a data type of their choice.
type CursorFactory = domain.CursorFactory

// IndexFactory represents a [Index] constructor that can be reimplemented. It
// should receive some options and create a index to a given set of fields.
type IndexFactory = domain.IndexFactory

// FindOption configures query behavior through the functional options pattern.
type FindOption = domain.FindOption

// WithProjection specifies which fields to include or exclude from query
// results.
func WithProjection(p any) FindOption {
	return domain.WithProjection(p)
}

// WithSkip sets the number of documents to skip in query results.
func WithSkip(s int64) FindOption {
	return domain.WithSkip(s)
}

// WithLimit sets the maximum number of documents to return.
func WithLimit(l int64) FindOption {
	return domain.WithLimit(l)
}

// WithSort specifies the sort order for query results.
func WithSort(s Sort) FindOption {
	return domain.WithSort(s)
}

// UpdateOption configures update behavior through the functional options
// pattern.
type UpdateOption = domain.UpdateOption

// WithUpdateMulti enables updating multiple documents that match the query.
func WithUpdateMulti(m bool) UpdateOption {
	return domain.WithUpdateMulti(m)
}

// WithUpsert enables inserting a document if no matches are found.
func WithUpsert(u bool) UpdateOption {
	return domain.WithUpsert(u)
}

// RemoveOption configures remove behavior through the functional options
// pattern.
type RemoveOption = domain.RemoveOption

// WithRemoveMulti enables removing multiple documents that match the query.
func WithRemoveMulti(m bool) RemoveOption {
	return domain.WithRemoveMulti(m)
}

// EnsureIndexOption configures index creation through the functional options
// pattern.
type EnsureIndexOption = domain.EnsureIndexOption

// WithFields specifies the field names for the index.
func WithFields(fn ...string) EnsureIndexOption {
	return domain.WithFields(fn...)
}

// WithUnique creates a unique index that prevents duplicate values.
func WithUnique(u bool) EnsureIndexOption {
	return domain.WithUnique(u)
}

// WithSparse creates a sparse index that excludes null/undefined
// values.
func WithSparse(s bool) EnsureIndexOption {
	return domain.WithSparse(s)
}

// WithTTL creates a TTL index that automatically removes
// documents after the specified duration.
func WithTTL(e time.Duration) EnsureIndexOption {
	return domain.WithTTL(e)
}

// QueryOption configures query behavior through the functional options pattern.
type QueryOption = domain.QueryOption

// WithQuery sets the query criteria for a [Querier.Query] call.
func WithQuery(q Document) QueryOption {
	return domain.WithQuery(q)
}

// WithQueryLimit sets the maximum number of documents the query should return.
func WithQueryLimit(l int64) QueryOption {
	return domain.WithQueryLimit(l)
}

// WithQuerySkip sets the number of documents the query should skip.
func WithQuerySkip(s int64) QueryOption {
	return domain.WithQuerySkip(s)
}

// WithQuerySort sets the sort order for query results.
func WithQuerySort(s Sort) QueryOption {
	return domain.WithQuerySort(s)
}

// WithQueryProjection specifies which fields to include or exclude in query
// results.
func WithQueryProjection(p map[string]uint8) QueryOption {
	return domain.WithQueryProjection(p)
}

// CursorOption configures cursor behavior through the functional options
// pattern.
type CursorOption = domain.CursorOption

// WithCursorDecoder sets the decoder for converting cursor results.
func WithCursorDecoder(d Decoder) CursorOption {
	return domain.WithCursorDecoder(d)
}

// IndexOption configures index behavior through the functional options pattern.
type IndexOption = domain.IndexOption

// WithIndexFieldName sets the field name for the index.
func WithIndexFieldName(f string) IndexOption {
	return domain.WithIndexFieldName(f)
}

// WithIndexUnique creates a unique index that prevents duplicate values.
func WithIndexUnique(u bool) IndexOption {
	return domain.WithIndexUnique(u)
}

// WithIndexSparse creates a sparse index that excludes null/undefined values.
func WithIndexSparse(s bool) IndexOption {
	return domain.WithIndexSparse(s)
}

// WithIndexExpireAfter creates a TTL index that automatically removes documents
// after the specified duration.
func WithIndexExpireAfter(e time.Duration) IndexOption {
	return domain.WithIndexExpireAfter(e)
}

// WithIndexDocumentFactory sets the document factory for creating documents
// during index operations.
func WithIndexDocumentFactory(d DocumentFactory) IndexOption {
	return domain.WithIndexDocumentFactory(d)
}

// WithIndexComparer sets the comparer implementation for sorting operations in
// the index.
func WithIndexComparer(c Comparer) IndexOption {
	return domain.WithIndexComparer(c)
}

// WithIndexHasher sets the hasher for generating hash values in index
// operations.
func WithIndexHasher(h Hasher) IndexOption {
	return domain.WithIndexHasher(h)
}

// WithIndexFieldNavigator sets the field getter for accessing document fields
// during indexing.
func WithIndexFieldNavigator(f FieldNavigator) IndexOption {
	return domain.WithIndexFieldNavigator(f)
}

// Option configures datastore behavior through the functional options
// pattern.
type Option = datastore.Option

// WithFilename sets the database filename for the datastore.
func WithFilename(f string) Option {
	return datastore.WithFilename(f)
}

// WithTimestamps enables automatic timestamping of documents with createdAt and
// updatedAt fields.
func WithTimestamps(t bool) Option {
	return datastore.WithTimestamps(t)
}

// WithInMemoryOnly enables in-memory only mode without file persistence.
func WithInMemoryOnly(i bool) Option {
	return datastore.WithInMemoryOnly(i)
}

// WithSerializer sets the serializer for converting documents to bytes.
func WithSerializer(s Serializer) Option {
	return datastore.WithSerializer(s)
}

// WithDeserializer sets the deserializer for converting bytes to documents.
func WithDeserializer(d Deserializer) Option {
	return datastore.WithDeserializer(d)
}

// WithCorruptionThreshold sets the threshold for corruption warnings.
func WithCorruptionThreshold(c float64) Option {
	return datastore.WithCorruptionThreshold(c)
}

// WithComparer sets the comparer for value comparison operations.
func WithComparer(c Comparer) Option {
	return datastore.WithComparer(c)
}

// WithFileMode sets the file permissions for database files.
func WithFileMode(f os.FileMode) Option {
	return datastore.WithFileMode(f)
}

// WithDirMode sets the directory permissions for database directories.
func WithDirMode(d os.FileMode) Option {
	return datastore.WithDirMode(d)
}

// WithPersistence sets the persistence implementation for data storage.
func WithPersistence(p Persistence) Option {
	return datastore.WithPersistence(p)
}

// WithStorage sets the storage implementation for low-level file operations.
func WithStorage(s Storage) Option {
	return datastore.WithStorage(s)
}

// WithIndexFactory sets the factory function for creating index instances.
func WithIndexFactory(i IndexFactory) Option {
	return datastore.WithIndexFactory(i)
}

// WithDocumentFactory sets the factory function for creating [Document]
// instances.
func WithDocumentFactory(d DocumentFactory) Option {
	return datastore.WithDocumentFactory(d)
}

// WithDecoder sets the decoder for data format conversions.
func WithDecoder(d Decoder) Option {
	return datastore.WithDecoder(d)
}

// WithMatcher sets the matcher implementation for query evaluation.
func WithMatcher(m Matcher) Option {
	return datastore.WithMatcher(m)
}

// WithCursorFactory sets the factory function for creating cursor instances.
func WithCursorFactory(c CursorFactory) Option {
	return datastore.WithCursorFactory(c)
}

// WithModifier sets the modifier implementation for document updates.
func WithModifier(m Modifier) Option {
	return datastore.WithModifier(m)
}

// WithTimeGetter sets the time getter for timestamping operations.
func WithTimeGetter(t TimeGetter) Option {
	return datastore.WithTimeGetter(t)
}

// WithHasher sets the hasher for generating hash values.
func WithHasher(h Hasher) Option {
	return datastore.WithHasher(h)
}

// WithFieldNavigator sets the field getter for accessing document fields.
func WithFieldNavigator(f FieldNavigator) Option {
	return datastore.WithFieldNavigator(f)
}

// WithIDGenerator sets the idgenerator to create new document ids.
func WithIDGenerator(ig IDGenerator) Option {
	return datastore.WithIDGenerator(ig)
}

// WithRandomReader sets the reader to be used by the IDGenerator.
func WithRandomReader(r io.Reader) Option {
	return datastore.WithRandomReader(r)
}
