package lib

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/vinicius-lino-figueiredo/gedb"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

// Datastore implements gedb.GEDB.
type Datastore struct {
	filename              string
	timestampData         bool
	inMemoryOnly          bool
	autoload              bool
	serializer            gedb.Serializer
	deserializer          gedb.Deserializer
	corruptAlertThreshold float64
	compare               func(any, any) int
	fileMode              os.FileMode
	dirMode               os.FileMode
	executor              *ctxsync.Mutex
	persistence           gedb.Persistence
	indexes               map[string]gedb.Index
	ttlIndexes            map[string]time.Time
	indexFactory          func(gedb.IndexOptions) gedb.Index
}

// NewDatastore returns a new implementation of Datastore
func NewDatastore(options gedb.DatastoreOptions) (gedb.GEDB, error) {
	if options.Persistence == nil {
		var err error
		persistenceOptions := gedb.PersistenceOptions{
			Filename:              options.Filename,
			InMemoryOnly:          options.InMemoryOnly,
			CorruptAlertThreshold: options.CorruptAlertThreshold,
			FileMode:              options.FileMode,
			DirMode:               options.DirMode,
			Serializer:            options.Serializer,
			Deserializer:          options.Deserializer,
			Storage:               options.Storage,
		}
		options.Persistence, err = NewPersistence(persistenceOptions)
		if err != nil {
			return nil, err
		}
	}
	if options.IndexFactory == nil {
		options.IndexFactory = NewIndex
	}
	IDIdx := options.IndexFactory(gedb.IndexOptions{FieldName: "_id", Unique: true})
	return &Datastore{
		filename:              options.Filename,
		timestampData:         options.TimestampData,
		inMemoryOnly:          options.InMemoryOnly || options.Filename == "",
		indexes:               map[string]gedb.Index{"_id": IDIdx},
		corruptAlertThreshold: options.CorruptAlertThreshold,
		fileMode:              options.FileMode,
		dirMode:               options.DirMode,
		executor:              ctxsync.NewMutex(),
		persistence:           options.Persistence,
		indexFactory:          options.IndexFactory,
	}, nil
}

// CompactDatafile implements gedb.GEDB.
func (d *Datastore) CompactDatafile(ctx context.Context) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()

	allData := d.indexes["_id"].GetAll()
	indexDTOs := d.getIndexDTOs()

	return d.persistence.PersistCachedDatabase(ctx, allData, indexDTOs)
}

// Count implements gedb.GEDB.
func (d *Datastore) Count(ctx context.Context, query any) (int64, error) {
	panic("unimplemented") // TODO: implement
}

// DropDatabase implements gedb.GEDB.
func (d *Datastore) DropDatabase(ctx context.Context) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	ctx = context.WithoutCancel(ctx) // should complete this task
	// d.StopAutocompaction not added for now
	IDIdx := d.indexFactory(gedb.IndexOptions{FieldName: "_id"})
	d.indexes = map[string]gedb.Index{"_id": IDIdx}
	d.ttlIndexes = make(map[string]time.Time)
	return d.persistence.DropDatabase(ctx)
}

// EnsureIndex implements gedb.GEDB.
func (d *Datastore) EnsureIndex(ctx context.Context, options gedb.EnsureIndexOptions) error {
	panic("unimplemented") // TODO: implement
}

// Find implements gedb.GEDB.
func (d *Datastore) Find(ctx context.Context, query any, projection any) (gedb.Cursor, error) {
	panic("unimplemented") // TODO: implement
}

// FindOne implements gedb.GEDB.
func (d *Datastore) FindOne(ctx context.Context, query any, projection any) (gedb.Cursor, error) {
	panic("unimplemented") // TODO: implement
}

// GetAllData implements gedb.GEDB.
func (d *Datastore) GetAllData(ctx context.Context) (gedb.Cursor, error) {
	panic("unimplemented") // TODO: implement
}

func (d *Datastore) getIndexDTOs() map[string]gedb.IndexDTO {
	indexDTOs := make(map[string]gedb.IndexDTO, len(d.indexes))
	for indexName, idx := range d.indexes {
		indexDTOs[indexName] = gedb.IndexDTO{
			IndexCreated: gedb.IndexCreated{
				FieldName: idx.FieldName(),
				Unique:    idx.Unique(),
				Sparse:    idx.Sparse(),
			},
		}
	}
	return indexDTOs
}

// Insert implements gedb.GEDB.
func (d *Datastore) Insert(ctx context.Context, newDocs ...any) ([]gedb.Document, error) {
	panic("unimplemented") // TODO: implement
}

// LoadDatabase implements gedb.GEDB.
func (d *Datastore) LoadDatabase(ctx context.Context) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	if err := d.resetIndexes(ctx); err != nil {
		return err
	}
	if d.inMemoryOnly {
		return nil
	}
	docs, indexes, err := d.persistence.LoadDatabase(ctx)
	if err != nil {
		return err
	}
	for key, idx := range indexes {
		d.indexes[key] = d.indexFactory(gedb.IndexOptions{DTO: idx})
	}
	if err := d.resetIndexes(ctx, docs...); err != nil {
		if resetErr := d.resetIndexes(ctx); resetErr != nil {
			return errors.Join(err, resetErr)
		}
		return err
	}

	indexDTOs := d.getIndexDTOs()

	return d.persistence.PersistCachedDatabase(ctx, docs, indexDTOs)
}

// Remove implements gedb.GEDB.
func (d *Datastore) Remove(ctx context.Context, query any, options gedb.RemoveOptions) (int64, error) {
	panic("unimplemented") // TODO: implement
}

// RemoveIndex implements gedb.GEDB.
func (d *Datastore) RemoveIndex(ctx context.Context, fieldNames []string) error {
	panic("unimplemented") // TODO: implement
}

func (d *Datastore) resetIndexes(ctx context.Context, docs ...gedb.Document) error {
	for _, index := range d.indexes {
		if err := index.Reset(ctx, docs...); err != nil {
			return err
		}
	}
	return nil
}

// SetAutocompactionInterval implements gedb.GEDB.
func (d *Datastore) SetAutocompactionInterval(interval time.Duration) {
	panic("unimplemented") // TODO: implement
}

// StopAutocompaction implements gedb.GEDB.
func (d *Datastore) StopAutocompaction() {
	panic("unimplemented") // TODO: implement
}

// Update implements gedb.GEDB.
func (d *Datastore) Update(ctx context.Context, query any, updateQuery any, options gedb.UpdateOptions) (int64, error) {
	panic("unimplemented") // TODO: implement
}

// WaitCompaction implements gedb.GEDB.
func (d *Datastore) WaitCompaction(ctx context.Context) error {
	panic("unimplemented") // TODO: implement
}
