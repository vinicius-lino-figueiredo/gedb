package nedb

import (
	"context"
	"io"
	"os"
	"time"
)

type Nedb interface {
	LoadDatabase(ctx context.Context) error

	DropDatabase(ctx context.Context) error

	CompactDatafile(ctx context.Context) error

	SetAutocompactionInterval(interval time.Duration)

	StopAutocompaction()

	GetAllData(ctx context.Context) (Cursor, error)

	EnsureIndex(ctx context.Context, options EnsureIndexOptions) error

	RemoveIndex(ctx context.Context, fieldNames []string) error

	Insert(ctx context.Context, newDocs []any) error

	Count(ctx context.Context, query any) (int64, error)

	Find(ctx context.Context, query any, projection any) (Cursor, error)

	FindOne(ctx context.Context, query any, projectio any) (Cursor, error)

	Update(ctx context.Context, query any, updateQuery any, options UpdateOptions) (int64, error)

	Remove(ctx context.Context, query any, options RemoveOptions) (int64, error)

	Compacted(ctx context.Context) error
}

type Cursor interface {
	ID() string
	Projection(query any) Cursor
	Exec(ctx context.Context, target any) error
	Sort(query any) Cursor
	Skip(n int64) Cursor
	Limit(n int64) Cursor
	Next(ctx context.Context) bool
	Close()
}

type DatastoreOptions struct {
	Filename              string
	TimestampData         bool
	InMemoryOnly          bool
	Autoload              bool
	Serializer            Serializer
	Deserializer          Deserializer
	CorruptAlertThreshold float64
	Compare               func(any, any) int
	FileMode              os.FileMode
	DirMode               os.FileMode
}

type Serializer interface {
	Serialize(context.Context, any) ([]byte, error)
}

type SerializeFunc func(context.Context, any) ([]byte, error)

func (s SerializeFunc) Serialize(ctx context.Context, v any) ([]byte, error) { return s(ctx, v) }

type Deserializer interface {
	Deserialize(context.Context, []byte, any) error
}

type DeserializeFunc func(context.Context, []byte, any) error

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
	GetMatching(value ...any) []Document
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
	FieldName string
	Unique    bool
	Sparse    bool
}

type Update struct {
	OldDoc Document
	NewDoc Document
}

type Document interface {
	ID() string
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
}

type IndexDTO struct {
	IndexCreated IndexCreated `json:"$$indexCreated" mapstructure:"$$indexCreated"`
}
type IndexCreated struct {
	FieldName string `json:"fieldName" mapstructure:"fieldName"`
	Unique    bool   `json:"unique" mapstructure:"unique"`
	Sparse    bool   `json:"sparse" mapstructure:"sparse"`
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
