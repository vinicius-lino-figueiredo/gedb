package lib

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
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
	ttlIndexes            map[string]time.Duration
	indexFactory          func(gedb.IndexOptions) gedb.Index
	documentFactory       func(any) (gedb.Document, error)
}

// NewDatastore returns a new implementation of Datastore
func NewDatastore(options gedb.DatastoreOptions) (gedb.GEDB, error) {
	if options.Persistence == nil {
		var err error
		persistenceOptions := gedb.PersistenceOptions{
			Filename:              options.Filename,
			InMemoryOnly:          options.InMemoryOnly || options.Filename == "",
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
	if options.DocumentFactory == nil {
		options.DocumentFactory = NewDocument
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
		documentFactory:       options.DocumentFactory,
	}, nil
}

func (d *Datastore) addToIndexes(ctx context.Context, doc gedb.Document) error {
	var failingIndex int
	var err error
	keys := slices.Collect(maps.Keys(d.indexes))

	for i, key := range keys {
		if err = d.indexes[key].Insert(ctx, doc); err != nil {
			failingIndex = i
			break
		}
	}

	if err != nil {
		for i := range failingIndex {
			if removeErr := d.indexes[keys[i]].Remove(ctx, doc); removeErr != nil {
				return errors.Join(err, removeErr)
			}
		}
		return err
	}
	return nil
}

func (d *Datastore) checkDocuments(docs ...gedb.Document) error {
	for _, doc := range docs {
		for k, v := range doc.Iter() {
			if subDoc, ok := v.(gedb.Document); ok {
				if err := d.checkDocuments(subDoc); err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(k, "$") {
				switch k {
				case "$$date":
					if v != nil {
						if _, ok := v.(time.Time); ok {
							continue
						}
					}
					fallthrough
				case "$$deleted":
					if v != nil {
						if deleted, _ := v.(bool); deleted {
							continue
						}
					}
					fallthrough
				case "$$indexCreated", "$$indexRemoved":
					continue
				default:
					return fmt.Errorf("field names cannot begin with the $ character")
				}
			}
			if strings.ContainsRune(k, '.') {
				return fmt.Errorf("field names cannot contain a '.'")
			}
		}
	}
	return nil
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

func (d *Datastore) createNewID() (string, error) {
	for {
		buf := make([]byte, 8)
		_, err := rand.Read(buf)
		if err != nil {
			return "", err
		}

		enc := base64.StdEncoding.EncodeToString(buf)
		enc = strings.NewReplacer("+", "", "/", "").Replace(enc)
		if m := d.indexes["_id"].GetMatching(enc); len(m) == 0 {
			return enc, nil
		}
	}
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
	d.ttlIndexes = make(map[string]time.Duration)
	return d.persistence.DropDatabase(ctx)
}

// EnsureIndex implements gedb.GEDB.
func (d *Datastore) EnsureIndex(ctx context.Context, options gedb.EnsureIndexOptions) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	if len(options.FieldNames) == 0 || slices.Contains(options.FieldNames, "") {
		return fmt.Errorf("cannot create an index without a fieldName")
	}

	_fields := slices.Clone(options.FieldNames)
	slices.Sort(_fields)

	containsComma := func(s string) bool {
		return strings.ContainsRune(s, ',')
	}
	if slices.ContainsFunc(_fields, containsComma) {
		return errors.New("Cannot use comma in index fieldName")
	}

	_options := gedb.IndexOptions{
		FieldName:   strings.Join(_fields, ","),
		Unique:      options.Unique,
		Sparse:      options.Sparse,
		ExpireAfter: options.ExpireAfter,
	}

	if _, exists := d.indexes[_options.FieldName]; exists {
		return nil
	}

	d.indexes[_options.FieldName] = d.indexFactory(_options)
	if options.ExpireAfter > 0 {
		d.ttlIndexes[_options.FieldName] = options.ExpireAfter
	}

	data := d.getAllData()
	if err := d.indexes[_options.FieldName].Insert(ctx, data...); err != nil {
		delete(d.indexes, _options.FieldName)
		return err
	}

	dto := gedb.IndexDTO{
		IndexCreated: gedb.IndexCreated{
			FieldName: _options.FieldName,
			Unique:    _options.Unique,
			Sparse:    _options.Sparse,
		},
	}

	idxDoc, err := d.documentFactory(dto)
	if err != nil {
		return err
	}

	return d.persistence.PersistNewState(ctx, idxDoc)
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

func (d *Datastore) getAllData() []gedb.Document {
	return d.indexes["_id"].GetAll()
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
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	if len(newDocs) == 0 {
		return nil, nil
	}
	preparedDocs, err := d.prepareDocumentsForInsertion(newDocs)
	if err != nil {
		return nil, err
	}
	// avoid a mess by ensuring it wont cancel during cache insertion
	ctx = context.WithoutCancel(ctx)
	if err = d.insertInCache(ctx, preparedDocs); err != nil {
		return nil, err
	}
	if err := d.persistence.PersistNewState(ctx, preparedDocs...); err != nil {
		return nil, err
	}
	return preparedDocs, nil
}

func (d *Datastore) insertInCache(ctx context.Context, preparedDocs []gedb.Document) error {
	var failingIndex int
	var err error

	for i, preparedDoc := range preparedDocs {
		if err = d.addToIndexes(ctx, preparedDoc); err != nil {
			failingIndex = i
			break
		}
	}

	if err != nil {
		for i := range failingIndex {
			if removeErr := d.removeFromIndexes(ctx, preparedDocs[i]); removeErr != nil {
				return errors.Join(err, removeErr)
			}
		}
		return err
	}
	return nil
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

func (d *Datastore) prepareDocumentsForInsertion(newDocs []any) ([]gedb.Document, error) {
	preparedDocs := make([]gedb.Document, len(newDocs))
	for n, newDoc := range newDocs {
		preparedDoc, err := d.documentFactory(newDoc)
		if err != nil {
			return nil, err
		}
		if !preparedDoc.Has("_id") {
			id, err := d.createNewID()
			if err != nil {
				return nil, err
			}
			preparedDoc.Set("_id", id)
		}
		if d.timestampData {
			now := time.Now()
			if !preparedDoc.Has("createdAt") {
				preparedDoc.Set("createdAt", now)
			}
			if !preparedDoc.Has("updatedAt") {
				preparedDoc.Set("updatedAt", now)
			}
		}
		if err := d.checkDocuments(preparedDoc); err != nil {
			return nil, err
		}
		preparedDocs[n] = preparedDoc
	}
	return preparedDocs, nil
}

// Remove implements gedb.GEDB.
func (d *Datastore) Remove(ctx context.Context, query any, options gedb.RemoveOptions) (int64, error) {
	panic("unimplemented") // TODO: implement
}

func (d *Datastore) removeFromIndexes(ctx context.Context, doc gedb.Document) error {
	for index := range maps.Values(d.indexes) {
		if err := index.Remove(ctx, doc); err != nil {
			return err
		}
	}
	return nil
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

// Update implements gedb.GEDB.
func (d *Datastore) Update(ctx context.Context, query any, updateQuery any, options gedb.UpdateOptions) (int64, error) {
	panic("unimplemented") // TODO: implement
}

// WaitCompaction implements gedb.GEDB.
func (d *Datastore) WaitCompaction(ctx context.Context) error {
	return d.persistence.WaitCompaction(ctx)
}
