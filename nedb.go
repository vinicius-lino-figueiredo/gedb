package nedb

import (
	"context"
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

	EnsureIndexAsync(ctx context.Context, options EnsureIndexOptions) error

	RemoveIndex(ctx context.Context, fieldNames []string) error

	Insert(ctx context.Context, newDocs []any)

	Count(ctx context.Context, query any) (int64, error)

	Find(ctx context.Context, query any, projection any) (Cursor, error)

	FindOne(ctx context.Context, query any, projectio any) (Cursor, error)

	Update(ctx context.Context, query any, updateQuery any, options UpdateOptions) (int64, error)

	Remove(ctx context.Context, query any, options RemoveOptions) (int64, error)

	Compacted(ctx context.Context) error
}

type Cursor interface {
	ID() [16]byte
	Projection(query any) Cursor
	Exec(ctx context.Context, target any) error
	Sort(query any) Cursor
	Skip(n int64) Cursor
	Limit(n int64) Cursor
	Next(ctx context.Context) bool
	Close()
}

type DataStoreOptions struct {
	Filename              string
	TimestampData         bool
	InMemoryOnly          bool
	Autoload              bool
	serializer            Serializer
	deserializer          Deserializer
	CorruptAlertThreshold float64
	Compare               func(any, any) int
	FileMode              os.FileMode
	DirMode               os.FileMode
}

type Serializer interface {
	Serialize(context.Context, any) ([]byte, error)
}

type Deserializer interface {
	Deserialize(context.Context, any, []byte) error
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
	ProcessBuffer()
	ResetBuffer()
}
