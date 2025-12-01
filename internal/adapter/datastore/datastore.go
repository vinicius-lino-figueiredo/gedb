// Package datastore contains the default [domain.GEDB] implementation.
package datastore

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

	"github.com/vinicius-lino-figueiredo/gedb/domain"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/cursor"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/deserializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/index"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/matcher"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/modifier"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/persistence"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/querier"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/serializer"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/storage"
	"github.com/vinicius-lino-figueiredo/gedb/internal/adapter/timegetter"
	"github.com/vinicius-lino-figueiredo/gedb/pkg/ctxsync"
)

const (
	DefaultDirMode  os.FileMode = 0o755
	DefaultFileMode os.FileMode = 0o644
)

// Datastore implements domain.GEDB.
type Datastore struct {
	filename              string
	timestampData         bool
	inMemoryOnly          bool
	corruptAlertThreshold float64
	comparer              domain.Comparer
	fileMode              os.FileMode
	dirMode               os.FileMode
	executor              *ctxsync.Mutex
	persistence           domain.Persistence
	indexes               map[string]domain.Index
	ttlIndexes            map[string]time.Duration
	indexFactory          func(...domain.IndexOption) (domain.Index, error)
	documentFactory       func(any) (domain.Document, error)
	cursorFactory         func(context.Context, []domain.Document, ...domain.CursorOption) (domain.Cursor, error)
	matcher               domain.Matcher
	decoder               domain.Decoder
	modifier              domain.Modifier
	timeGetter            domain.TimeGetter
	hasher                domain.Hasher
	fieldNavigator        domain.FieldNavigator
	querier               domain.Querier
}

// NewDatastore returns a new implementation of Datastore.
func NewDatastore(options ...domain.DatastoreOption) (domain.GEDB, error) {

	comp := comparer.NewComparer()
	docFac := data.NewDocument
	dec := decoder.NewDecoder()
	fn := fieldnavigator.NewFieldNavigator(docFac)
	matchr := matcher.NewMatcher(
		domain.WithMatcherDocumentFactory(docFac),
		domain.WithMatcherComparer(comp),
		domain.WithMatcherFieldNavigator(fn),
	)
	opts := domain.DatastoreOptions{
		Filename:              "",
		TimestampData:         false,
		InMemoryOnly:          false,
		Serializer:            serializer.NewSerializer(comp, docFac),
		Deserializer:          deserializer.NewDeserializer(dec),
		CorruptAlertThreshold: 0.1,
		Comparer:              comp,
		FileMode:              DefaultFileMode,
		DirMode:               DefaultDirMode,
		Storage:               storage.NewStorage(),
		IndexFactory:          index.NewIndex,
		DocumentFactory:       docFac,
		Decoder:               dec,
		Matcher:               matchr,
		CursorFactory:         cursor.NewCursor,
		Modifier:              modifier.NewModifier(docFac, comp, fn, matchr),
		TimeGetter:            timegetter.NewTimeGetter(),
		Hasher:                hasher.NewHasher(),
		FieldNavigator:        fn,
		Querier:               querier.NewQuerier(),
	}
	for _, option := range options {
		option(&opts)
	}

	if opts.Persistence == nil {
		var err error
		persistenceOptions := []domain.PersistenceOption{
			domain.WithPersistenceFilename(opts.Filename),
			domain.WithPersistenceInMemoryOnly(opts.InMemoryOnly || opts.Filename == ""),
			domain.WithPersistenceCorruptAlertThreshold(opts.CorruptAlertThreshold),
			domain.WithPersistenceFileMode(opts.FileMode),
			domain.WithPersistenceDirMode(opts.DirMode),
			domain.WithPersistenceSerializer(opts.Serializer),
			domain.WithPersistenceDeserializer(opts.Deserializer),
			domain.WithPersistenceStorage(opts.Storage),
			domain.WithPersistenceDecoder(opts.Decoder),
			domain.WithPersistenceHasher(opts.Hasher),
			domain.WithPersistenceFieldNavigator(opts.FieldNavigator),
		}
		opts.Persistence, err = persistence.NewPersistence(persistenceOptions...)
		if err != nil {
			return nil, err
		}
	}
	IDIdx, err := opts.IndexFactory(
		domain.WithIndexFieldName("_id"),
		domain.WithIndexUnique(true),
	)
	if err != nil {
		return nil, err
	}
	return &Datastore{
		filename:              opts.Filename,
		timestampData:         opts.TimestampData,
		inMemoryOnly:          opts.InMemoryOnly || opts.Filename == "",
		indexes:               map[string]domain.Index{"_id": IDIdx},
		ttlIndexes:            make(map[string]time.Duration),
		corruptAlertThreshold: opts.CorruptAlertThreshold,
		fileMode:              opts.FileMode,
		dirMode:               opts.DirMode,
		executor:              ctxsync.NewMutex(),
		persistence:           opts.Persistence,
		indexFactory:          opts.IndexFactory,
		documentFactory:       opts.DocumentFactory,
		cursorFactory:         opts.CursorFactory,
		decoder:               opts.Decoder,
		comparer:              opts.Comparer,
		modifier:              opts.Modifier,
		timeGetter:            opts.TimeGetter,
		hasher:                opts.Hasher,
		fieldNavigator:        opts.FieldNavigator,
		matcher:               opts.Matcher,
		querier:               opts.Querier,
	}, nil
}

func (d *Datastore) addToIndexes(ctx context.Context, doc domain.Document) error {
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

func (d *Datastore) checkDocuments(docs ...domain.Document) error {
	for _, doc := range docs {
		for k, v := range doc.Iter() {
			if strings.HasPrefix(k, "$") {
				switch k {
				case "$$date":
					if _, ok := v.(time.Time); ok {
						continue
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
			if subDoc, ok := v.(domain.Document); ok {
				if err := d.checkDocuments(subDoc); err != nil {
					return err
				}
				continue
			}
		}
	}
	return nil
}

func (d *Datastore) cloneDocs(docs ...domain.Document) ([]domain.Document, error) {
	res := make([]domain.Document, len(docs))
	for n, doc := range docs {
		newDoc, err := d.clone(doc)
		if err != nil {
			return nil, err
		}
		res[n] = newDoc.(domain.Document)
	}
	return res, nil
}

func (d *Datastore) clone(v any) (any, error) {
	switch t := v.(type) {
	case domain.Document:
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

// CompactDatafile implements domain.GEDB.
func (d *Datastore) CompactDatafile(ctx context.Context) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()

	allData := d.indexes["_id"].GetAll()
	indexDTOs := d.getIndexDTOs()

	return d.persistence.PersistCachedDatabase(ctx, allData, indexDTOs)
}

// Count implements domain.GEDB.
func (d *Datastore) Count(ctx context.Context, query any) (int64, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return 0, err
	}
	defer d.executor.Unlock()

	cur, err := d.find(ctx, query, false)
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

// DropDatabase implements domain.GEDB.
func (d *Datastore) DropDatabase(ctx context.Context) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()
	ctx = context.WithoutCancel(ctx) // should complete this task
	// d.StopAutocompaction not added for now.
	IDIdx, err := d.indexFactory(domain.WithIndexFieldName("_id"))
	if err != nil {
		return err
	}
	d.indexes = map[string]domain.Index{"_id": IDIdx}
	d.ttlIndexes = make(map[string]time.Duration)
	return d.persistence.DropDatabase(ctx)
}

// EnsureIndex implements domain.GEDB.
func (d *Datastore) EnsureIndex(ctx context.Context, options ...domain.EnsureIndexOption) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()

	var opts domain.EnsureIndexOptions
	for _, option := range options {
		option(&opts)
	}

	if len(opts.FieldNames) == 0 || slices.Contains(opts.FieldNames, "") {
		return fmt.Errorf("cannot create an index without a fieldName")
	}

	_fields := slices.Clone(opts.FieldNames)
	slices.Sort(_fields)

	containsComma := func(s string) bool {
		return strings.ContainsRune(s, ',')
	}
	if slices.ContainsFunc(_fields, containsComma) {
		return errors.New("cannot use comma in index fieldName")
	}

	fields := strings.Join(_fields, ",")

	_options := []domain.IndexOption{
		domain.WithIndexFieldName(fields),
		domain.WithIndexUnique(opts.Unique),
		domain.WithIndexSparse(opts.Sparse),
		domain.WithIndexExpireAfter(opts.ExpireAfter),
	}

	if _, exists := d.indexes[fields]; exists {
		return nil
	}

	var err error
	d.indexes[fields], err = d.indexFactory(_options...)
	if err != nil {
		return err
	}

	if opts.ExpireAfter > 0 {
		d.ttlIndexes[fields] = opts.ExpireAfter
	}

	data := d.getAllData()
	if err := d.indexes[fields].Insert(ctx, data...); err != nil {
		delete(d.indexes, fields)
		return err
	}

	dto := domain.IndexDTO{
		IndexCreated: domain.IndexCreated{
			FieldName: fields,
			Unique:    opts.Unique,
			Sparse:    opts.Sparse,
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
	if _, ok := v.(domain.Document); ok {
		return false
	}
	if _, ok := v.([]any); ok {
		return false
	}
	return true
}

// Find implements domain.GEDB.
func (d *Datastore) Find(ctx context.Context, query any, options ...domain.FindOption) (domain.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	return d.find(ctx, query, false, options...)
}

func (d *Datastore) find(ctx context.Context, query any, dontExpireStaleDocs bool, options ...domain.FindOption) (domain.Cursor, error) {
	queryDoc, err := d.documentFactory(query)
	if err != nil {
		return nil, err
	}

	var opt domain.FindOptions
	for _, option := range options {
		option(&opt)
	}

	proj := make(map[string]uint8)
	if err := d.decoder.Decode(opt.Projection, &proj); err != nil {
		return nil, err
	}

	allData, err := d.getCandidates(ctx, queryDoc, dontExpireStaleDocs)
	if err != nil {
		return nil, err
	}

	cursorOptions := []domain.QueryOption{
		domain.WithQuery(queryDoc),
		domain.WithQueryLimit(opt.Limit),
		domain.WithQuerySkip(opt.Skip),
		domain.WithQuerySort(opt.Sort),
		domain.WithQueryProjection(proj),
	}

	res, err := d.querier.Query(allData, cursorOptions...)
	if err != nil {
		return nil, err
	}

	res, err = d.cloneDocs(res...)
	if err != nil {
		return nil, err
	}

	return d.cursorFactory(ctx, res)
}

// FindOne implements domain.GEDB.
func (d *Datastore) FindOne(ctx context.Context, query any, target any, options ...domain.FindOption) error {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return err
	}
	defer d.executor.Unlock()

	options = append(options, domain.WithFindLimit(1))

	cur, err := d.find(ctx, query, false, options...)
	if err != nil {
		return err
	}
	defer cur.Close()
	if !cur.Next() {
		return fmt.Errorf("expected exactly one record, got 0")
	}
	return cur.Scan(ctx, target)
}

// GetAllData implements domain.GEDB.
func (d *Datastore) GetAllData(ctx context.Context) (domain.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	return d.cursorFactory(ctx, d.getAllData())
}

func (d *Datastore) getAllData() []domain.Document {
	return d.indexes["_id"].GetAll()
}

func (d *Datastore) getCandidates(ctx context.Context, query domain.Document, dontExpireStaleDocs bool) ([]domain.Document, error) {
	docs, err := d.getRawCandidates(ctx, query)
	if err != nil {
		return nil, err
	}

	if dontExpireStaleDocs {
		return docs, nil
	}
	now := d.timeGetter.GetTime()
	expiredDocsIDs := make([]any, 0, len(docs))
	validDocs := make([]domain.Document, 0, len(docs))
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

		if _, err = d.remove(ctx, rm, domain.WithRemoveMulti(false)); err != nil {
			return nil, err
		}
	}
	return validDocs, nil
}

func (d *Datastore) getIndexDTOs() map[string]domain.IndexDTO {
	indexDTOs := make(map[string]domain.IndexDTO, len(d.indexes))
	for indexName, idx := range d.indexes {
		indexDTOs[indexName] = domain.IndexDTO{
			IndexCreated: domain.IndexCreated{
				FieldName: idx.FieldName(),
				Unique:    idx.Unique(),
				Sparse:    idx.Sparse(),
			},
		}
	}
	return indexDTOs
}

func (d *Datastore) getRawCandidates(ctx context.Context, query domain.Document) ([]domain.Document, error) {
	// if query is empty, return all
	if query.Len() == 0 {
		return d.getAllData(), nil
	}

	// checking if query has a indexed field.
	if res, ok, err := d.getSimpleCandidates(query); err != nil || ok {
		return res, err
	}

	// checking if query has all fields of an existent composed index.
	if res, ok, err := d.getComposedCandidates(query); err != nil || ok {
		return res, err
	}

	// checking if query has the query comparer $in, which is indexable.
	if res, ok, err := d.getEnumCandidates(query); err != nil || ok {
		return res, err
	}

	// checking if query has an indexable query field ($lt, $gte, etc.).
	if res, ok, err := d.getCompCandidates(ctx, query); err != nil || ok {
		return res, err
	}

	// if cannot use any indexes, return all data.
	return d.getAllData(), nil
}

func (d *Datastore) getSimpleCandidates(query domain.Document) ([]domain.Document, bool, error) {
	indexNames := slices.Collect(maps.Keys(d.indexes))
	for k, v := range query.Iter() {
		if !d.filterIndexNames(indexNames, k, v) {
			continue
		}
		return d.matchingResult(d.indexes[k].GetMatching(v))
	}
	return nil, false, nil
}

func (d *Datastore) getComposedCandidates(query domain.Document) ([]domain.Document, bool, error) {
IndexesLoop:
	for idxName, idx := range d.indexes {
		parts, err := d.fieldNavigator.SplitFields(idxName)
		if err != nil {
			return nil, false, err
		}
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
		return d.matchingResult(idx.GetMatching(query))
	}
	return nil, false, nil
}

func (d *Datastore) getEnumCandidates(query domain.Document) ([]domain.Document, bool, error) {
	for k := range query.Iter() {
		vDoc := query.D(k)
		if vDoc == nil || !vDoc.Has("$in") {
			continue
		}

		idx, ok := d.indexes[k]
		if !ok {
			continue
		}

		in := vDoc.Get("$in")
		if l, ok := in.([]any); ok {
			return d.matchingResult(idx.GetMatching(l...))
		}

		return d.matchingResult(idx.GetMatching(in))
	}
	return nil, false, nil
}

func (d *Datastore) getCompCandidates(ctx context.Context, query domain.Document) ([]domain.Document, bool, error) {
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
				return d.matchingResult(idx.GetBetweenBounds(ctx, vDoc))
			}
		}
	}
	return nil, false, nil
}

func (d *Datastore) matchingResult(dt []domain.Document, err error) ([]domain.Document, bool, error) {
	if err != nil {
		return nil, false, err
	}
	return dt, true, nil
}

// Insert implements domain.GEDB.
func (d *Datastore) Insert(ctx context.Context, newDocs ...any) (domain.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	res, err := d.insert(ctx, newDocs...)
	if err != nil {
		return nil, err
	}
	return d.cursorFactory(ctx, res)
}

func (d *Datastore) insert(ctx context.Context, newDocs ...any) ([]domain.Document, error) {
	if len(newDocs) == 0 {
		return nil, nil
	}
	preparedDocs, err := d.prepareDocumentsForInsertion(newDocs)
	if err != nil {
		return nil, err
	}
	// avoid a mess by ensuring it won't cancel during cache insertion
	ctx = context.WithoutCancel(ctx)
	if err = d.insertInCache(ctx, preparedDocs); err != nil {
		return nil, err
	}
	if err := d.persistence.PersistNewState(ctx, preparedDocs...); err != nil {
		return nil, err
	}
	return d.cloneDocs(preparedDocs...)
}

func (d *Datastore) insertInCache(ctx context.Context, preparedDocs []domain.Document) error {
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

// LoadDatabase implements domain.GEDB.
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
		options := []domain.IndexOption{
			domain.WithIndexFieldName(idx.IndexCreated.FieldName),
			domain.WithIndexUnique(idx.IndexCreated.Unique),
			domain.WithIndexSparse(idx.IndexCreated.Sparse),
			domain.WithIndexExpireAfter(time.Duration(idx.IndexCreated.ExpireAfter * float64(time.Second))),
			domain.WithIndexDocumentFactory(d.documentFactory),
			domain.WithIndexComparer(d.comparer),
			domain.WithIndexHasher(d.hasher),
		}
		d.indexes[key], err = d.indexFactory(options...)
		if err != nil {
			return err
		}
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

func (d *Datastore) prepareDocumentsForInsertion(newDocs []any) ([]domain.Document, error) {
	preparedDocs := make([]domain.Document, len(newDocs))
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

// Remove implements domain.GEDB.
func (d *Datastore) Remove(ctx context.Context, query any, options ...domain.RemoveOption) (int64, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return 0, err
	}
	defer d.executor.Unlock()
	queryDoc, err := d.documentFactory(query)
	if err != nil {
		return 0, err
	}
	return d.remove(ctx, queryDoc, options...)
}

func (d *Datastore) remove(ctx context.Context, query domain.Document, options ...domain.RemoveOption) (int64, error) {
	var limit int64

	var opts domain.RemoveOptions
	for _, option := range options {
		option(&opts)
	}

	if opts.Multi {
		limit = 0
	}

	cur, err := d.find(ctx, query, true, domain.WithFindLimit(limit))
	if err != nil {
		return 0, err
	}

	// Probably should not directly reference this default type, but it
	// makes code much cleaner because there are no error checks
	var vals []data.M
	for cur.Next() {
		var v data.M
		if err := cur.Scan(ctx, &v); err != nil {
			return 0, err
		}
		vals = append(vals, v)
	}
	if err := cur.Err(); err != nil {
		return 0, err
	}

	docs := make([]domain.Document, len(vals))
	var numRemoved int64
	for n, val := range vals {
		newVal := data.M{"_id": val.ID(), "$$deleted": true}
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

func (d *Datastore) removeFromIndexes(ctx context.Context, doc domain.Document) error {
	for _, index := range d.indexes {
		if err := index.Remove(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

// RemoveIndex implements domain.GEDB.
func (d *Datastore) RemoveIndex(ctx context.Context, fieldNames ...string) error {
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
		return errors.New("cannot use comma in index fieldName")
	}
	fieldName := strings.Join(_fields, ",")
	delete(d.indexes, fieldName)

	dto := domain.IndexDTO{
		IndexRemoved: fieldName,
	}

	idxDoc, err := d.documentFactory(dto)
	if err != nil {
		return err
	}

	return d.persistence.PersistNewState(ctx, idxDoc)
}

func (d *Datastore) resetIndexes(ctx context.Context, docs ...domain.Document) error {
	for _, index := range d.indexes {
		if err := index.Reset(ctx, docs...); err != nil {
			return err
		}
	}
	return nil
}

// Update implements domain.GEDB.
func (d *Datastore) Update(ctx context.Context, query any, updateQuery any, options ...domain.UpdateOption) (domain.Cursor, error) {
	if err := d.executor.LockWithContext(ctx); err != nil {
		return nil, err
	}
	defer d.executor.Unlock()
	res, err := d.update(ctx, query, updateQuery, options...)
	if err != nil {
		return nil, err
	}
	return d.cursorFactory(ctx, res)
}

func (d *Datastore) update(ctx context.Context, query any, updateQuery any, options ...domain.UpdateOption) ([]domain.Document, error) {
	updateQryDoc, err := d.documentFactory(updateQuery)
	if err != nil {
		return nil, err
	}

	var opts domain.UpdateOptions
	for _, option := range options {
		option(&opts)
	}

	var limit int64
	if !opts.Multi {
		limit = 1
	}

	if opts.Upsert {
		inserted, rtrn, err := d.upsert(ctx, query, updateQryDoc, limit)
		if err != nil || rtrn {
			return inserted, err
		}
	}

	updated, mods, err := d.findAndModify(ctx, query, updateQryDoc, limit)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	if err := d.updateIndexes(ctx, mods); err != nil {
		return nil, err
	}

	if err := d.persistence.PersistNewState(ctx, updated...); err != nil {
		return nil, err
	}

	return d.cloneDocs(updated...)
}

func (d *Datastore) upsert(ctx context.Context, query any, mod domain.Document, limit int64) ([]domain.Document, bool, error) {
	cur, err := d.find(ctx, query, false, domain.WithFindLimit(limit))
	if err != nil {
		return nil, false, err
	}
	var count int64
	for cur.Next() {
		count++
	}
	if err := cur.Err(); err != nil {
		return nil, false, err
	}
	if count != 1 {
		qry, err := d.documentFactory(query)
		if err != nil {
			return nil, false, err
		}
		if err := d.checkDocuments(mod); err != nil {
			if mod, err = d.modifier.Modify(qry, mod); err != nil {
				return nil, false, err
			}
		}
		insertedDoc, err := d.insert(ctx, mod)
		if err != nil {
			return nil, false, err
		}
		return insertedDoc, true, err
	}
	return nil, false, nil
}

func (d *Datastore) findAndModify(ctx context.Context, qry any, modQry domain.Document, limit int64) ([]domain.Document, []domain.Update, error) {
	cur, err := d.find(ctx, qry, false, domain.WithFindLimit(limit))
	if err != nil {
		return nil, nil, err
	}
	var mods []domain.Update
	var updatedDocs []domain.Document
	for cur.Next() {
		oldDoc, err := data.NewDocument(nil)
		if err != nil {
			return nil, nil, err
		}
		if err := cur.Scan(ctx, &oldDoc); err != nil {
			return nil, nil, err
		}
		newDoc, err := d.modifier.Modify(oldDoc, modQry)
		if err != nil {
			return nil, nil, err
		}

		if d.timestampData {
			newDoc.Set("createdAt", oldDoc.Get("createdAt"))
			newDoc.Set("updatedAt", d.timeGetter.GetTime())
		}

		update := domain.Update{OldDoc: oldDoc, NewDoc: newDoc}
		mods = append(mods, update)
		updatedDocs = append(updatedDocs, newDoc)
	}
	if err := cur.Err(); err != nil {
		return nil, nil, err
	}
	return updatedDocs, mods, nil
}

func (d *Datastore) updateIndexes(ctx context.Context, mods []domain.Update) error {
	var failingIndex int
	var err error

	keys := slices.Collect(maps.Keys(d.indexes))
	for i, key := range keys {
		if err = d.indexes[key].UpdateMultipleDocs(ctx, mods...); err != nil {
			failingIndex = i
			break
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

// WaitCompaction implements domain.GEDB.
func (d *Datastore) WaitCompaction(ctx context.Context) error {
	return d.persistence.WaitCompaction(ctx)
}
