package domain

import (
	"context"
	"os"
	"time"
)

// WithFindProjection specifies which fields to include or exclude from query
// results.
func WithFindProjection(p any) FindOption {
	return func(fo *FindOptions) {
		fo.Projection = p
	}
}

// WithFindSkip sets the number of documents to skip in query results.
func WithFindSkip(s int64) FindOption {
	return func(fo *FindOptions) {
		fo.Skip = s
	}
}

// WithFindLimit sets the maximum number of documents to return.
func WithFindLimit(l int64) FindOption {
	return func(fo *FindOptions) {
		fo.Limit = l
	}
}

// WithFindSort specifies the sort order for query results.
func WithFindSort(s Sort) FindOption {
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

// WithEnsureIndexFieldNames specifies the field names for the index.
func WithEnsureIndexFieldNames(fn ...string) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.FieldNames = fn
	}
}

// WithEnsureIndexUnique creates a unique index that prevents duplicate values.
func WithEnsureIndexUnique(u bool) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.Unique = u
	}
}

// WithEnsureIndexSparse creates a sparse index that excludes null/undefined
// values.
func WithEnsureIndexSparse(s bool) EnsureIndexOption {
	return func(eio *EnsureIndexOptions) {
		eio.Sparse = s
	}
}

// WithEnsureIndexExpiry creates a TTL index that automatically removes
// documents after the specified duration.
func WithEnsureIndexExpiry(e time.Duration) EnsureIndexOption {
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

// WithProjectorFieldNavigator sets the [FieldNavigator] that will be used by
// [Projector]
func WithProjectorFieldNavigator(fn FieldNavigator) ProjectorOption {
	return func(po *ProjectorOptions) {
		po.FieldNavigator = fn
	}
}

// WithProjectorDocumentFactory sets the [Document] factory function that will
// be used by [Projector]
func WithProjectorDocumentFactory(df func(any) (Document, error)) ProjectorOption {
	return func(po *ProjectorOptions) {
		po.DocFac = df
	}
}

// ProjectorOption configures projector behavior through the functional options
// pattern.
type ProjectorOption func(*ProjectorOptions)

// ProjectorOptions contains parameters for customizing projector behavior.
type ProjectorOptions struct {
	FieldNavigator FieldNavigator
	DocFac         func(any) (Document, error)
}

// WithQuerierDocumentFactory sets the factory function for creating documents.
func WithQuerierDocumentFactory(df func(any) (Document, error)) QuerierOption {
	return func(qo *QuerierOptions) {
		qo.DocFac = df
	}
}

// WithQuerierMatcher sets the matcher implementation for querier evaluations.
func WithQuerierMatcher(m Matcher) QuerierOption {
	return func(qo *QuerierOptions) {
		qo.Matcher = m
	}
}

// WithQuerierComparer sets the comparer implementation for sorting operations.
func WithQuerierComparer(c Comparer) QuerierOption {
	return func(qo *QuerierOptions) {
		qo.Comparer = c
	}
}

// WithQuerierFieldNavigator sets the field getter for accessing document
// fields.
func WithQuerierFieldNavigator(f FieldNavigator) QuerierOption {
	return func(qo *QuerierOptions) {
		qo.FieldNavigator = f
	}
}

// WithQuerierProjector sets the implementation what will be sed to project
// the resultant documents.
func WithQuerierProjector(p Projector) QuerierOption {
	return func(qo *QuerierOptions) {
		qo.Projector = p
	}
}

// QuerierOption configures querier behavior through the functional options
// pattern.
type QuerierOption func(*QuerierOptions)

// QuerierOptions contains parameters for customizing querier behavior.
type QuerierOptions struct {
	DocFac func(any) (Document, error)
	// Matcher provides query evaluation logic.
	Matcher Matcher
	// Comparer provides sorting operations.
	Comparer Comparer
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
	// Projector provides field projection.
	Projector Projector
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

// WithMatcherDocumentFactory sets the document factory for creating documents
// during matching.
func WithMatcherDocumentFactory(d func(any) (Document, error)) MatcherOption {
	return func(mo *MatcherOptions) {
		mo.DocumentFactory = d
	}
}

// WithMatcherComparer sets the comparer implementation for value comparisons
// during matching.
func WithMatcherComparer(c Comparer) MatcherOption {
	return func(mo *MatcherOptions) {
		mo.Comparer = c
	}
}

// WithMatcherFieldNavigator sets the field getter for accessing document fields
// during matching.
func WithMatcherFieldNavigator(f FieldNavigator) MatcherOption {
	return func(mo *MatcherOptions) {
		mo.FieldNavigator = f
	}
}

// MatcherOption configures matcher behavior through the functional options
// pattern.
type MatcherOption func(*MatcherOptions)

// MatcherOptions contains parameters for customizing matcher behavior.
type MatcherOptions struct {
	// DocumentFactory creates document instances during matching
	// operations.
	DocumentFactory func(any) (Document, error)
	// Comparer provides value comparison operations.
	Comparer Comparer
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
}

// WithPersistenceFilename sets the database filename for persistence.
func WithPersistenceFilename(f string) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Filename = f
	}
}

// WithPersistenceInMemoryOnly enables in-memory only mode without file
// persistence.
func WithPersistenceInMemoryOnly(i bool) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.InMemoryOnly = i
	}
}

// WithPersistenceCorruptAlertThreshold sets the threshold for corruption
// warnings.
func WithPersistenceCorruptAlertThreshold(c float64) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.CorruptAlertThreshold = c
	}
}

// WithPersistenceFileMode sets the file permissions for database files.
func WithPersistenceFileMode(f os.FileMode) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.FileMode = f
	}
}

// WithPersistenceDirMode sets the directory permissions for database
// directories.
func WithPersistenceDirMode(d os.FileMode) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.DirMode = d
	}
}

// WithPersistenceSerializer sets the serializer for converting documents to
// bytes.
func WithPersistenceSerializer(s Serializer) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Serializer = s
	}
}

// WithPersistenceDeserializer sets the deserializer for converting bytes to
// documents.
func WithPersistenceDeserializer(d Deserializer) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Deserializer = d
	}
}

// WithPersistenceStorage sets the storage implementation for file operations.
func WithPersistenceStorage(s Storage) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Storage = s
	}
}

// WithPersistenceDecoder sets the decoder for data format conversions.
func WithPersistenceDecoder(d Decoder) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Decoder = d
	}
}

// WithPersistenceComparer sets the comparer for value comparison operations.
func WithPersistenceComparer(c Comparer) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Comparer = c
	}
}

// WithPersistenceDocFactory sets the factory function for creating
// documents.
func WithPersistenceDocFactory(d func(any) (Document, error)) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.DocumentFactory = d
	}
}

// WithPersistenceHasher sets the hasher for generating hash values.
func WithPersistenceHasher(h Hasher) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.Hasher = h
	}
}

// WithPersistenceFieldNavigator sets the field getter for accessing document
// fields.
func WithPersistenceFieldNavigator(f FieldNavigator) PersistenceOption {
	return func(po *PersistenceOptions) {
		po.FieldNavigator = f
	}
}

// PersistenceOption configures persistence behavior through the functional
// options pattern.
type PersistenceOption func(*PersistenceOptions)

// PersistenceOptions contains parameters for customizing persistence behavior.
type PersistenceOptions struct {
	// Filename specifies the database file path.
	Filename string
	// InMemoryOnly enables in-memory only mode without file persistence.
	InMemoryOnly bool
	// CorruptAlertThreshold sets the threshold for corruption warnings.
	CorruptAlertThreshold float64
	// FileMode specifies file permissions for database files.
	FileMode os.FileMode
	// DirMode specifies directory permissions for database directories.
	DirMode os.FileMode
	// Serializer converts documents to bytes for storage.
	Serializer Serializer
	// Deserializer converts bytes back to documents.
	Deserializer Deserializer
	// Storage provides low-level file operations.
	Storage Storage
	// Decoder converts between data representations.
	Decoder Decoder
	// Comparer provides value comparison operations.
	Comparer Comparer
	// DocumentFactory creates document instances.
	DocumentFactory func(any) (Document, error)
	// Hasher generates hash values for data.
	Hasher Hasher
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
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
func WithIndexDocumentFactory(d func(any) (Document, error)) IndexOption {
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
	// ExpireAfter automatically removes documents after the specified duration (TTL).
	ExpireAfter time.Duration
	// DocumentFactory creates document instances during index operations.
	DocumentFactory func(any) (Document, error)
	// Comparer provides sorting operations for the index.
	Comparer Comparer
	// Hasher generates hash values for index operations.
	Hasher Hasher
	// FieldNavigator provides field access operations.
	FieldNavigator FieldNavigator
}

// WithDatastoreFilename sets the database filename for the datastore.
func WithDatastoreFilename(f string) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Filename = f
	}
}

// WithDatastoreTimestampData enables automatic timestamping of documents with createdAt and updatedAt fields.
func WithDatastoreTimestampData(t bool) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.TimestampData = t
	}
}

// WithDatastoreInMemoryOnly enables in-memory only mode without file persistence.
func WithDatastoreInMemoryOnly(i bool) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.InMemoryOnly = i
	}
}

// WithDatastoreSerializer sets the serializer for converting documents to bytes.
func WithDatastoreSerializer(s Serializer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Serializer = s
	}
}

// WithDatastoreDeserializer sets the deserializer for converting bytes to documents.
func WithDatastoreDeserializer(d Deserializer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Deserializer = d
	}
}

// WithDatastoreCorruptAlertThreshold sets the threshold for corruption warnings.
func WithDatastoreCorruptAlertThreshold(c float64) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.CorruptAlertThreshold = c
	}
}

// WithDatastoreComparer sets the comparer for value comparison operations.
func WithDatastoreComparer(c Comparer) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Comparer = c
	}
}

// WithDatastoreFileMode sets the file permissions for database files.
func WithDatastoreFileMode(f os.FileMode) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.FileMode = f
	}
}

// WithDatastoreDirMode sets the directory permissions for database directories.
func WithDatastoreDirMode(d os.FileMode) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.DirMode = d
	}
}

// WithDatastorePersistence sets the persistence implementation for data storage.
func WithDatastorePersistence(p Persistence) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Persistence = p
	}
}

// WithDatastoreStorage sets the storage implementation for low-level file operations.
func WithDatastoreStorage(s Storage) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Storage = s
	}
}

// WithDatastoreIndexFactory sets the factory function for creating index instances.
func WithDatastoreIndexFactory(i func(...IndexOption) (Index, error)) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.IndexFactory = i
	}
}

// WithDatastoreDocumentFactory sets the factory function for creating document instances.
func WithDatastoreDocumentFactory(d func(any) (Document, error)) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.DocumentFactory = d
	}
}

// WithDatastoreDecoder sets the decoder for data format conversions.
func WithDatastoreDecoder(d Decoder) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Decoder = d
	}
}

// WithDatastoreMatcher sets the matcher implementation for query evaluation.
func WithDatastoreMatcher(m Matcher) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Matcher = m
	}
}

// WithDatastoreCursorFactory sets the factory function for creating cursor instances.
func WithDatastoreCursorFactory(c func(context.Context, []Document, ...CursorOption) (Cursor, error)) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.CursorFactory = c
	}
}

// WithDatastoreModifier sets the modifier implementation for document updates.
func WithDatastoreModifier(m Modifier) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Modifier = m
	}
}

// WithDatastoreTimeGetter sets the time getter for timestamping operations.
func WithDatastoreTimeGetter(t TimeGetter) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.TimeGetter = t
	}
}

// WithDatastoreHasher sets the hasher for generating hash values.
func WithDatastoreHasher(h Hasher) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.Hasher = h
	}
}

// WithDatastoreFieldNavigator sets the field getter for accessing document fields.
func WithDatastoreFieldNavigator(f FieldNavigator) DatastoreOption {
	return func(dso *DatastoreOptions) {
		dso.FieldNavigator = f
	}
}

// DatastoreOption configures datastore behavior through the functional options pattern.
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
	IndexFactory func(...IndexOption) (Index, error)
	// DocumentFactory creates document instances.
	DocumentFactory func(any) (Document, error)
	// Decoder converts between data representations.
	Decoder Decoder
	// Matcher evaluates whether documents match query criteria.
	Matcher Matcher
	// CursorFactory creates cursor instances.
	CursorFactory func(context.Context, []Document, ...CursorOption) (Cursor, error)
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
}
