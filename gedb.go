// Package gedb provides an embedded MongoDB-like database for golang.
//
// This package contains a new implementation of nedb. Most features there can
// be used here.
// TODO: finish package comment
package gedb

import (
	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/datastore"
)

// GEDB is the main interface for interacting with the embedded database.
type GEDB = domain.GEDB

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

// NewDB creates a new GEDB instance with the provided configuration options.
func NewDB(options ...domain.DatastoreOption) (domain.GEDB, error) {
	return datastore.NewDatastore(options...)
}
