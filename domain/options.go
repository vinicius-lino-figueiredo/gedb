package domain

import (
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

// WithQueryCap TODO
func WithQueryCap(c int) QueryOption {
	return func(qo *QueryOptions) {
		qo.Cap = c
	}
}

// QueryOption configures query behavior through the functional options pattern.
type QueryOption func(*QueryOptions)

// QueryOptions contains parameters for customizing query behavior.
type QueryOptions struct {
	// Query specifies the criteria for filtering documents.
	Query any
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
	Cap            int
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
