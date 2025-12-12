package domain

import "context"

// IndexDTO represents a data transfer object for index operations, used to
// persist index creation and removal events in the datastore.
type IndexDTO struct {
	IndexCreated IndexCreated `json:"$$indexCreated" gedb:"$$indexCreated,omitzero"`
	IndexRemoved string       `json:"$$indexRemoved" gedb:"$$indexRemoved,omitzero"`
}

// IndexCreated represents the configuration of an index that was created,
// including field name, uniqueness constraint, sparseness, and TTL settings.
type IndexCreated struct {
	FieldName   string  `json:"fieldName" gedb:"fieldName,omitzero"`
	Unique      bool    `json:"unique" gedb:"unique,omitzero"`
	Sparse      bool    `json:"sparse" gedb:"sparse,omitzero"`
	ExpireAfter float64 `json:"$$expireAfterSeconds" gedb:"$$expireAfterSeconds,omitzero"`
}

// Update represents a pair of documents used in index update operations,
// containing both the old and new versions of a document.
type Update struct {
	OldDoc Document
	NewDoc Document
}

// Sort represents an ordered list of fields which should be used to sort query
// results, applied in sequence.
type Sort = []SortName

// SortName represents a single field and the order which should be used to sort
// it. A positive Order value means ascending order and a negative value means
// descending order.
type SortName struct {
	Key   string
	Order int64
}

// DocumentFactory represents a function that constructs [Document] instances
// from structured data types. If nil is provided, returns an empty document.
type DocumentFactory = func(any) (Document, error)

// CursorFactory represents a function that constructs [Cursor] instances from a
// set of documents with configurable options.
type CursorFactory = func(context.Context, []Document, ...CursorOption) (Cursor, error)

// IndexFactory represents a function that constructs [Index] instances with
// configurable options.
type IndexFactory = func(...IndexOption) (Index, error)
