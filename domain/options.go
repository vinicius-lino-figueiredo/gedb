package domain

import (
	"io"
	"os"
	"time"
)

// WithProjection specifies which fields to include or exclude from query
// results.
func WithProjection(p any) FindOption {
	return func(fo *FindOptions) {
		fo.Projection = p
	}
}

// WithSkip sets the number of documents to skip in query results.
func WithSkip(s int64) FindOption {
	return func(fo *FindOptions) {
		fo.Skip = s
	}
}

// WithLimit sets the maximum number of documents to return.
func WithLimit(l int64) FindOption {
	return func(fo *FindOptions) {
		fo.Limit = l
	}
}

// WithSort specifies the sort order for query results.
func WithSort(s Sort) FindOption {
	return func(fo *FindOptions) {
		fo.Sort = s
	}
}

// FindOption configures query behavior through the functional options pattern.
type FindOption func(*FindOptions)

// FindOptions contains parameters for customizing query execution.
type FindOptions struct {
	// Projection specifies which fields to include or exclude from results.
	Projection any
	// Skip specifies the number of documents to skip.
	Skip int64
	// Limit specifies the maximum number of documents to return.
	Limit int64
	// Sort specifies the sort order for results.
	Sort Sort
}

// WithUpdateMulti enables updating multiple documents that match the query.
func WithUpdateMulti(m bool) UpdateOption {
	return func(uo *UpdateOptions) {
		uo.Multi = m
	}
}

// WithUpsert enables inserting a document if no matches are found.
func WithUpsert(u bool) UpdateOption {
	return func(uo *UpdateOptions) {
		uo.Upsert = u
	}
}

// UpdateOption configures update behavior through the functional options
// pattern.
type UpdateOption func(*UpdateOptions)

// UpdateOptions contains parameters for customizing update operations.
type UpdateOptions struct {
	// Multi enables updating multiple documents that match the query.
	Multi bool
	// Upsert enables inserting a document if no matches are found.
	Upsert bool
}

// WithRemoveMulti enables removing multiple documents that match the query.
func WithRemoveMulti(m bool) RemoveOption {
	return func(ro *RemoveOptions) {
		ro.Multi = m
	}
}

// RemoveOption configures remove behavior through the functional options
// pattern.
type RemoveOption func(*RemoveOptions)

// RemoveOptions contains parameters for customizing remove operations.
type RemoveOptions struct {
	// Multi enables removing multiple documents that match the query.
	Multi bool
}

// WithFields specifies the field names for the index.
func WithFields(fn ...string) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.FieldNames = fn
	}
}

// WithUnique creates a unique index that prevents duplicate values.
func WithUnique(u bool) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.Unique = u
	}
}

// WithSparse creates a sparse index that excludes null/undefined
// values.
func WithSparse(s bool) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.Sparse = s
	}
}

// WithTTL creates a TTL index that automatically removes
// documents after the specified duration.
func WithTTL(e time.Duration) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.ExpireAfter = e
	}
}

// EnsureIndexOption configures index creation through the functional options
// pattern.
type EnsureIndexOption func(*EnsureIndexOptions)

// EnsureIndexOptions contains parameters for customizing index creation.
type EnsureIndexOptions struct {
	// FieldNames specifies the field names to index.
	FieldNames []string
	// Unique prevents duplicate values in the indexed field(s).
	Unique bool
	// Sparse excludes documents with null/undefined values from the index.
	Sparse bool
	// ExpireAfter automatically removes documents after the specified
	// duration (TTL).
	ExpireAfter time.Duration
}

// WithQuery sets the query criteria for a [Querier.Query] call.
func WithQuery(q Document) QueryOption {
	return func(qo *QueryOptions) {
		qo.Query = q
	}
}

// WithQueryLimit sets the maximum number of documents the query should return.
func WithQueryLimit(l int64) QueryOption {
	return func(qo *QueryOptions) {
		qo.Limit = l
	}
}

// WithQuerySkip sets the number of documents the query should skip.
func WithQuerySkip(s int64) QueryOption {
	return func(qo *QueryOptions) {
		qo.Skip = s
	}
}

// WithQuerySort sets the sort order for query results.
func WithQuerySort(s Sort) QueryOption {
	return func(qo *QueryOptions) {
		qo.Sort = s
	}
}

// WithQueryProjection specifies which fields to include or exclude in query
// results.
func WithQueryProjection(p map[string]uint8) QueryOption {
	return func(qo *QueryOptions) {
		qo.Projection = p
	}
}

// QueryOption configures query behavior through the functional options pattern.
type QueryOption func(*QueryOptions)

// QueryOptions contains parameters for customizing query behavior.
type QueryOptions struct {
	// Query specifies the criteria for filtering documents.
	Query Document
	// Limit specifies the maximum number of documents to return.
	Limit int64
	// Skip specifies the number of documents to skip.
	Skip int64
	// Sort specifies the sort order for results.
	Sort Sort
	// Projection specifies which fields to include or exclude.
	Projection map[string]uint8
	// Matcher provides query evaluation logic.
	Matcher Matcher
	// Comparer provides sorting operations.
	Comparer Comparer
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
}

// WithCursorDecoder sets the decoder for converting cursor results.
func WithCursorDecoder(d Decoder) CursorOption {
	return func(co *CursorOptions) {
		co.Decoder = d
	}
}

// CursorOption configures cursor behavior through the functional options
// pattern.
type CursorOption func(*CursorOptions)

// CursorOptions contains parameters for customizing cursor behavior.
type CursorOptions struct {
	// Decoder converts between data representations.
	Decoder Decoder
}

// WithIndexFieldName sets the field name for the index.
func WithIndexFieldName(f string) IndexOption {
	return func(io *IndexOptions) {
		io.FieldName = f
	}
}

// WithIndexUnique creates a unique index that prevents duplicate values.
func WithIndexUnique(u bool) IndexOption {
	return func(io *IndexOptions) {
		io.Unique = u
	}
}

// WithIndexSparse creates a sparse index that excludes null/undefined values.
func WithIndexSparse(s bool) IndexOption {
	return func(io *IndexOptions) {
		io.Sparse = s
	}
}

// WithIndexExpireAfter creates a TTL index that automatically removes documents after the specified duration.
func WithIndexExpireAfter(e time.Duration) IndexOption {
	return func(io *IndexOptions) {
		io.ExpireAfter = e
	}
}

// WithIndexDocumentFactory sets the document factory for creating documents during index operations.
func WithIndexDocumentFactory(d DocumentFactory) IndexOption {
	return func(io *IndexOptions) {
		io.DocumentFactory = d
	}
}

// WithIndexComparer sets the comparer implementation for sorting operations in the index.
func WithIndexComparer(c Comparer) IndexOption {
	return func(io *IndexOptions) {
		io.Comparer = c
	}
}

// WithIndexHasher sets the hasher for generating hash values in index operations.
func WithIndexHasher(h Hasher) IndexOption {
	return func(io *IndexOptions) {
		io.Hasher = h
	}
}

// WithIndexFieldNavigator sets the field getter for accessing document fields
// during indexing.
func WithIndexFieldNavigator(f FieldNavigator) IndexOption {
	return func(io *IndexOptions) {
		io.FieldNavigator = f
	}
}

// IndexOption configures index behavior through the functional options pattern.
type IndexOption func(*IndexOptions)

// IndexOptions contains parameters for customizing index creation and behavior.
type IndexOptions struct {
	// FieldName specifies the field to index.
	FieldName string
	// Unique prevents duplicate values in the indexed field.
	Unique bool
	// Sparse excludes documents with null/undefined values from the index.
	Sparse bool
	// ExpireAfter automatically removes documents after the specified
	// duration (TTL).
	ExpireAfter time.Duration
	// DocumentFactory creates document instances during index operations.
	DocumentFactory DocumentFactory
	// Comparer provides sorting operations for the index.
	Comparer Comparer
	// Hasher generates hash values for index operations.
	Hasher Hasher
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
}

// WithFilename sets the database filename for the datastore.
func WithFilename(f string) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Filename = f
	}
}

// WithTimestamps enables automatic timestamping of documents with createdAt and
// updatedAt fields.
func WithTimestamps(t bool) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.TimestampData = t
	}
}

// WithInMemoryOnly enables in-memory only mode without file persistence.
func WithInMemoryOnly(i bool) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.InMemoryOnly = i
	}
}

// WithSerializer sets the serializer for converting documents to bytes.
func WithSerializer(s Serializer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Serializer = s
	}
}

// WithDeserializer sets the deserializer for converting bytes to documents.
func WithDeserializer(d Deserializer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Deserializer = d
	}
}

// WithCorruptionThreshold sets the threshold for corruption warnings.
func WithCorruptionThreshold(c float64) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.CorruptAlertThreshold = c
	}
}

// WithComparer sets the comparer for value comparison operations.
func WithComparer(c Comparer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Comparer = c
	}
}

// WithFileMode sets the file permissions for database files.
func WithFileMode(f os.FileMode) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.FileMode = f
	}
}

// WithDirMode sets the directory permissions for database directories.
func WithDirMode(d os.FileMode) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.DirMode = d
	}
}

// WithPersistence sets the persistence implementation for data storage.
func WithPersistence(p Persistence) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Persistence = p
	}
}

// WithStorage sets the storage implementation for low-level file operations.
func WithStorage(s Storage) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Storage = s
	}
}

// WithIndexFactory sets the factory function for creating index instances.
func WithIndexFactory(i IndexFactory) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.IndexFactory = i
	}
}

// WithDocumentFactory sets the factory function for creating document instances.
func WithDocumentFactory(d DocumentFactory) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.DocumentFactory = d
	}
}

// WithDecoder sets the decoder for data format conversions.
func WithDecoder(d Decoder) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Decoder = d
	}
}

// WithMatcher sets the matcher implementation for query evaluation.
func WithMatcher(m Matcher) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Matcher = m
	}
}

// WithCursorFactory sets the factory function for creating cursor instances.
func WithCursorFactory(c CursorFactory) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.CursorFactory = c
	}
}

// WithModifier sets the modifier implementation for document updates.
func WithModifier(m Modifier) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Modifier = m
	}
}

// WithTimeGetter sets the time getter for timestamping operations.
func WithTimeGetter(t TimeGetter) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.TimeGetter = t
	}
}

// WithHasher sets the hasher for generating hash values.
func WithHasher(h Hasher) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Hasher = h
	}
}

// WithFieldNavigator sets the field getter for accessing document fields.
func WithFieldNavigator(f FieldNavigator) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.FieldNavigator = f
	}
}

// WithIDGenerator sets the idgenerator to create new document ids.
func WithIDGenerator(ig IDGenerator) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.IDGenerator = ig
	}
}

// WithRandomReader sets the reader to be used by the IDGenerator.
func WithRandomReader(r io.Reader) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.RandomReader = r
	}
}

// DatastoreOption configures datastore behavior through the functional options
// pattern.
type DatastoreOption func(*DatastoreOptions)

// DatastoreOptions contains parameters for customizing datastore behavior.
type DatastoreOptions struct {
	// Filename specifies the database file path.
	Filename string
	// TimestampData enables automatic timestamping of documents.
	TimestampData bool
	// InMemoryOnly enables in-memory only mode without file persistence.
	InMemoryOnly bool
	// Serializer converts documents to bytes for storage.
	Serializer Serializer
	// Deserializer converts bytes back to documents.
	Deserializer Deserializer
	// CorruptAlertThreshold sets the threshold for corruption warnings.
	CorruptAlertThreshold float64
	// Comparer provides value comparison operations.
	Comparer Comparer
	// FileMode specifies file permissions for database files.
	FileMode os.FileMode
	// DirMode specifies directory permissions for database directories.
	DirMode os.FileMode
	// Persistence manages database serialization and file operations.
	Persistence Persistence
	// Storage provides low-level file operations.
	Storage Storage
	// IndexFactory creates index instances.
	IndexFactory IndexFactory
	// DocumentFactory creates document instances.
	DocumentFactory DocumentFactory
	// Decoder converts between data representations.
	Decoder Decoder
	// Matcher evaluates whether documents match query criteria.
	Matcher Matcher
	// CursorFactory creates cursor instances.
	CursorFactory CursorFactory
	// Modifier applies update operations to documents.
	Modifier Modifier
	// TimeGetter provides current time for timestamping operations.
	TimeGetter TimeGetter
	// Hasher generates hash values for data.
	Hasher Hasher
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
	// Querier allows filtering, ordering, limiting and projecting docs.
	Querier Querier
	// IDGenerator generates IDs for new documents.
	IDGenerator IDGenerator
	// RandomReader is used by idGenerator to generate a random ID.
	RandomReader io.Reader
}
