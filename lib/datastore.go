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
	comparer              gedb.Comparer
	fileMode              os.FileMode
	dirMode               os.FileMode
	executor              *ctxsync.Mutex
	persistence           gedb.Persistence
	indexes               map[string]gedb.Index
	ttlIndexes            map[string]time.Duration
	indexFactory          func(gedb.IndexOptions) gedb.Index
	documentFactory       func(any) (gedb.Document, error)
	cursorFactory         func(context.Context, []gedb.Document, gedb.CursorOptions) (gedb.Cursor, error)
	matcher               gedb.Matcher
	decoder               gedb.Decoder
	modifier              gedb.Modifier
	timeGetter            gedb.TimeGetter
	hasher                gedb.Hasher
}

// LoadDatastore creates a new gedb.GEDB and loads the database.
func LoadDatastore(ctx context.Context, options gedb.DatastoreOptions) (gedb.GEDB, error) {
	db, err := NewDatastore(options)
	if err != nil {
		return nil, err
	}
	if err := db.LoadDatabase(ctx); err != nil {
		if dropErr := db.DropDatabase(context.WithoutCancel(ctx)); dropErr != nil {
			err = errors.Join(err, dropErr)
		}
		return nil, err
	}
	return db, nil
}

// NewDatastore returns a new implementation of Datastore
func NewDatastore(options gedb.DatastoreOptions) (gedb.GEDB, error) {
	if options.Decoder == nil {
		options.Decoder = NewDecoder()
	}
	if options.Hasher == nil {
		options.Hasher = NewHasher()
	}
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
			Decoder:               options.Decoder,
			Hasher:                options.Hasher,
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
	if options.Matcher == nil {
		options.Matcher = NewMatcher(options.DocumentFactory, options.Comparer)
	}
	if options.CursorFactory == nil {
		options.CursorFactory = NewCursor
	}
	if options.Comparer == nil {
		options.Comparer = NewComparer()
	}
	if options.Modifier == nil {
		options.Modifier = NewModifier(options.DocumentFactory)
	}
	if options.TimeGetter == nil {
		options.TimeGetter = NewTimeGetter()
	}
	IDIdx := options.IndexFactory(gedb.IndexOptions{FieldName: "_id", Unique: true})
	return &Datastore{
		filename:              options.Filename,
		timestampData:         options.TimestampData,
		inMemoryOnly:          options.InMemoryOnly || options.Filename == "",
		indexes:               map[string]gedb.Index{"_id": IDIdx},
		ttlIndexes:            make(map[string]time.Duration),
		corruptAlertThreshold: options.CorruptAlertThreshold,
		fileMode:              options.FileMode,
		dirMode:               options.DirMode,
		executor:              ctxsync.NewMutex(),
		persistence:           options.Persistence,
		indexFactory:          options.IndexFactory,
		documentFactory:       options.DocumentFactory,
		cursorFactory:         options.CursorFactory,
		decoder:               options.Decoder,
		comparer:              options.Comparer,
		modifier:              options.Modifier,
		timeGetter:            options.TimeGetter,
		hasher:                options.Hasher,
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
			if subDoc, ok := v.(gedb.Document); ok {
				if err := d.checkDocuments(subDoc); err != nil {
					return err
				}
				continue
			}
		}
	}
	return nil
}

func (d *Datastore) cloneDocs(docs ...gedb.Document) ([]gedb.Document, error) {
	res := make([]gedb.Document, len(docs))
	for n, doc := range docs {
		newDoc, err := d.clone(doc)
		if err != nil {
			return nil, err
		}
		res[n] = newDoc.(gedb.Document)
	}
	return res, nil
}

func (d *Datastore) clone(v any) (any, error) {
	switch t := v.(type) {
	case gedb.Document:
		res, err := d.documentFactory(nil)
		if err != nil {
			return nil, err
		}
		for k, v := range t.Iter() {
			val, err := d.clone(v)
			if err != nil {
				return nil, err
			}
			res.Set(k, val)
		}
		return res, nil
	case []any:
		res := make([]any, len(t))
		for n, v := range t {
			val, err := d.clone(v)
			if err != nil {
				return nil, err
			}
			res[n] = val
		}
		return res, nil
	default:
		return t, nil
	}
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
	if err := d.executor.LockWithContext(ctx); err != nil {
		return 0, err
	}
	defer d.executor.Unlock()

	cur, err := d.find(ctx, query, gedb.FindOptions{}, false)
	if err != nil {
		return 0, err
	}
	var count int64
	for cur.Next() {
		count++
	}
	if err := cur.Err(); err != nil {
		return 0, err
	}
	return count, nil
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
		m, err := d.indexes["_id"].GetMatching(enc)
		if err != nil {
			return "", err
		}
		if len(m) == 0 {
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

func (d *Datastore) filterIndexNames(indexNames []string, k string, v any) bool {
	if !slices.Contains(indexNames, k) {
		return false
	}
	if _, ok := v.(gedb.Document); ok {
		return false
	}
	if _, ok := v.([]any); ok {
		return false
	}
	return true
}

// Find implements gedb.GEDB.
func (d *Datastore) Find(ctx context.Context, query any, options gedb.FindOptions) (gedb.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	return d.find(ctx, query, options, false)
}

func (d *Datastore) find(ctx context.Context, query any, options gedb.FindOptions, dontExpireStaleDocs bool) (gedb.Cursor, error) {
	queryDoc, err := d.documentFactory(query)
	if err != nil {
		return nil, err
	}

	proj := make(map[string]uint64)
	if err := d.decoder.Decode(options.Projection, &proj); err != nil {
		return nil, err
	}

	sort := make(map[string]int64)
	if err := d.decoder.Decode(options.Sort, &sort); err != nil {
		return nil, err
	}

	allData, err := d.getCandidates(ctx, queryDoc, dontExpireStaleDocs)
	if err != nil {
		return nil, err
	}

	cursorOptions := gedb.CursorOptions{
		Query:           queryDoc,
		Limit:           options.Limit,
		Skip:            options.Skip,
		Sort:            sort,
		Projection:      proj,
		Matcher:         d.matcher,
		Decoder:         d.decoder,
		DocumentFactory: d.documentFactory,
		Comparer:        d.comparer,
	}

	allData, err = d.cloneDocs(allData...)
	if err != nil {
		return nil, err
	}

	return d.cursorFactory(ctx, allData, cursorOptions)
}

// FindOne implements gedb.GEDB.
func (d *Datastore) FindOne(ctx context.Context, query any, target any, options gedb.FindOptions) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	options.Limit = 1
	cur, err := d.find(ctx, query, options, false)
	if err != nil {
		return err
	}
	defer cur.Close()
	if !cur.Next() {
		return fmt.Errorf("expected exactly one record, got 0")
	}
	return cur.Exec(ctx, target)
}

// GetAllData implements gedb.GEDB.
func (d *Datastore) GetAllData(ctx context.Context) (gedb.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	return d.cursorFactory(ctx, d.getAllData(), gedb.CursorOptions{})
}

func (d *Datastore) getAllData() []gedb.Document {
	return d.indexes["_id"].GetAll()
}

func (d *Datastore) getCandidates(ctx context.Context, query gedb.Document, dontExpireStaleDocs bool) ([]gedb.Document, error) {
	docs, err := d.getRawCandidates(ctx, query)
	if err != nil {
		return nil, err
	}

	if dontExpireStaleDocs {
		return docs, nil
	}
	now := d.timeGetter.GetTime()
	expiredDocsIDs := make([]any, 0, len(docs))
	validDocs := make([]gedb.Document, 0, len(docs))
DocLoop:
	for _, doc := range docs {
		for i, ttl := range d.ttlIndexes {
			v := doc.Get(i)
			if v == nil {
				continue
			}
			t, ok := v.(time.Time)
			if !ok {
				continue
			}
			if now.After(t.Add(ttl)) {
				expiredDocsIDs = append(expiredDocsIDs, doc.ID())
				continue DocLoop
			}
		}
		validDocs = append(validDocs, doc)
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	for _, _id := range expiredDocsIDs {
		rmMap := map[string]any{"_id": _id}
		rm, err := d.documentFactory(rmMap)
		if err != nil {
			return nil, err
		}

		options := gedb.RemoveOptions{Multi: false}
		if _, err = d.remove(ctx, rm, options); err != nil {
			return nil, err
		}
	}
	return validDocs, nil
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

func (d *Datastore) getRawCandidates(ctx context.Context, query gedb.Document) ([]gedb.Document, error) {
	if query.Len() == 0 {
		return d.getAllData(), nil
	}
	indexNames := slices.Collect(maps.Keys(d.indexes))
	for k, v := range query.Iter() {
		if !d.filterIndexNames(indexNames, k, v) {
			continue
		}
		return d.indexes[k].GetMatching(v)
	}

IndexesLoop:
	for idxName, idx := range d.indexes {
		parts := strings.Split(idxName, ",")
		if len(parts) == 0 {
			continue
		}

		queryKeys := slices.Collect(query.Keys())
		for i, k := range queryKeys {
			i++
			if !slices.Contains(parts, k) {
				continue IndexesLoop
			}
			vDoc := query.D(k)
			if vDoc != nil {
				continue IndexesLoop
			}
			if i == query.Len() {
				break
			}
		}
		return idx.GetMatching(query)
	}

	for k := range query.Iter() {
		if vDoc := query.D(k); vDoc != nil {
			if in := vDoc.Get("$in"); vDoc.Has("$in") {
				if idx, ok := d.indexes[k]; ok {
					if l, ok := in.([]any); ok {
						return idx.GetMatching(l...)
					}
					return idx.GetMatching(in)
				}
			}
		}
	}

	comp := [...]string{"$lt", "$lte", "$gt", "$gte"}
	for k, v := range query.Iter() {
		if v == nil {
			continue
		}

		vDoc := query.D(k)
		if vDoc == nil {
			continue
		}

		for _, c := range comp {
			if idx, ok := d.indexes[k]; ok && vDoc.Has(c) {
				return idx.GetBetweenBounds(ctx, vDoc)
			}
		}
	}

	return d.getAllData(), nil
}

// Insert implements gedb.GEDB.
func (d *Datastore) Insert(ctx context.Context, newDocs ...any) ([]gedb.Document, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	return d.insert(ctx, newDocs...)
}

func (d *Datastore) insert(ctx context.Context, newDocs ...any) ([]gedb.Document, error) {
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
	return d.cloneDocs(preparedDocs...)
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
		d.indexes[key] = d.indexFactory(gedb.IndexOptions{DTO: &idx})
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
			now := d.timeGetter.GetTime()
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
	if err := d.executor.LockWithContext(ctx); err != nil {
		return 0, err
	}
	defer d.executor.Unlock()
	queryDoc, err := d.documentFactory(query)
	if err != nil {
		return 0, err
	}
	return d.remove(ctx, queryDoc, options)
}

func (d *Datastore) remove(ctx context.Context, query gedb.Document, options gedb.RemoveOptions) (int64, error) {
	var limit int64
	if options.Multi {
		limit = 0
	}

	cur, err := d.find(ctx, query, gedb.FindOptions{Limit: limit}, true)
	if err != nil {
		return 0, err
	}

	// Probably should not directly reference this default type, but it
	// makes code much cleaner because there are no error checks
	var vals []Document
	for cur.Next() {
		var v Document
		if err := cur.Exec(ctx, &v); err != nil {
			return 0, err
		}
		vals = append(vals, v)
	}
	if err := cur.Err(); err != nil {
		return 0, err
	}

	docs := make([]gedb.Document, len(vals))
	var numRemoved int64
	for n, val := range vals {
		newVal := Document{"_id": val.ID(), "$$deleted": true}
		numRemoved++
		if err := d.removeFromIndexes(ctx, val); err != nil {
			return 0, err
		}
		docs[n] = newVal
	}

	if err := d.persistence.PersistNewState(ctx, docs...); err != nil {
		return 0, err
	}

	return numRemoved, nil
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
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	_fields := slices.Clone(fieldNames)
	slices.Sort(_fields)

	containsComma := func(s string) bool {
		return strings.ContainsRune(s, ',')
	}
	if slices.ContainsFunc(_fields, containsComma) {
		return errors.New("Cannot use comma in index fieldName")
	}
	fieldName := strings.Join(_fields, ",")
	delete(d.indexes, fieldName)

	dto := gedb.IndexDTO{
		IndexRemoved: true,
	}

	idxDoc, err := d.documentFactory(dto)
	if err != nil {
		return err
	}

	return d.persistence.PersistNewState(ctx, idxDoc)
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
func (d *Datastore) Update(ctx context.Context, query any, updateQuery any, options gedb.UpdateOptions) ([]gedb.Document, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()

	QryDoc, err := d.documentFactory(query)
	if err != nil {
		return nil, err
	}

	updateQryDoc, err := d.documentFactory(updateQuery)
	if err != nil {
		return nil, err
	}

	var limit int64
	if !options.Multi {
		limit = 1
	}

	if options.Upsert {
		cur, err := d.find(ctx, query, gedb.FindOptions{Limit: limit}, false)
		if err != nil {
			return nil, err
		}
		var count int64
		for cur.Next() {
			count++
		}
		if err := cur.Err(); err != nil {
			return nil, err
		}
		if count != 1 {
			if err := d.checkDocuments(updateQryDoc); err != nil {
				if updateQryDoc, err = d.modifier.Modify(QryDoc, updateQryDoc); err != nil {
					return nil, err
				}
			}
			insertedDoc, err := d.insert(ctx, updateQryDoc)
			if err != nil {
				return nil, err
			}
			return insertedDoc, err
		}
	}
	cur, err := d.find(ctx, query, gedb.FindOptions{Limit: limit}, false)
	if err != nil {
		return nil, err
	}
	var modifications []gedb.Update
	var updatedDocs []gedb.Document
	for cur.Next() {
		oldDoc, err := NewDocument(nil)
		if err != nil {
			return nil, err
		}
		if err := cur.Exec(ctx, &oldDoc); err != nil {
			return nil, err
		}
		newDoc, err := d.modifier.Modify(oldDoc, updateQryDoc)
		if err != nil {
			return nil, err
		}

		if d.timestampData {
			newDoc.Set("createdAt", oldDoc.Get("createdAt"))
			newDoc.Set("updatedAt", d.timeGetter.GetTime())
		}

		update := gedb.Update{OldDoc: oldDoc, NewDoc: newDoc}
		modifications = append(modifications, update)
		updatedDocs = append(updatedDocs, newDoc)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	if err := d.updateIndexes(ctx, modifications); err != nil {
		return nil, err
	}

	if err := d.persistence.PersistNewState(ctx, updatedDocs...); err != nil {
		return nil, err
	}

	return d.cloneDocs(updatedDocs...)
}

func (d *Datastore) updateIndexes(ctx context.Context, mods []gedb.Update) error {
	var failingIndex int
	var err error

	keys := slices.Collect(maps.Keys(d.indexes))
	for i, key := range keys {
		if err = d.indexes[key].UpdateMultipleDocs(ctx, mods...); err != nil {
			failingIndex = i
		}
	}
	if err != nil {
		for i := range failingIndex {
			if revertErr := d.indexes[keys[i]].RevertMultipleUpdates(ctx, mods...); revertErr != nil {
				err = errors.Join(err, revertErr)
				break
			}
		}
	}
	return err
}

// WaitCompaction implements gedb.GEDB.
func (d *Datastore) WaitCompaction(ctx context.Context) error {
	return d.persistence.WaitCompaction(ctx)
}
