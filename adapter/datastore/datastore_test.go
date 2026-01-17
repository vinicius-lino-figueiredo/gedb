package datastore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/comparer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/cursor"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/data"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/decoder"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/deserializer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/fieldnavigator"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/hasher"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/idgenerator"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/index"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/matcher"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/modifier"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/persistence"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/projector"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/serializer"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/storage"
	"github.com/vinicius-lino-figueiredo/gedb/adapter/timegetter"
	"github.com/vinicius-lino-figueiredo/gedb/domain"
)

var ctx = context.Background()

type M = map[string]any

type A = []any

type timeGetterMock struct{ mock.Mock }

type S = domain.Sort

func (t *timeGetterMock) GetTime() time.Time { return t.Called().Get(0).(time.Time) }

type contextMock struct{ mock.Mock }

// Deadline implements [context.Context].
func (c *contextMock) Deadline() (deadline time.Time, ok bool) {
	call := c.Called()
	return call.Get(0).(time.Time), call.Bool(1)
}

// Done implements [context.Context].
func (c *contextMock) Done() <-chan struct{} {
	return c.Called().Get(0).(<-chan struct{})
}

// Err implements [context.Context].
func (c *contextMock) Err() error {
	return c.Called().Error(0)
}

// Value implements [context.Context].
func (c *contextMock) Value(key any) any {
	return c.Called(key).Get(0)
}

type fieldNavigatorMock struct{ mock.Mock }

// EnsureField implements [domain.FieldNavigator].
func (fn *fieldNavigatorMock) EnsureField(value any, addr ...string) ([]domain.GetSetter, error) {
	call := fn.Called(value, addr)
	return call.Get(0).([]domain.GetSetter), call.Error(1)
}

// GetAddress implements [domain.FieldNavigator].
func (fn *fieldNavigatorMock) GetAddress(field string) ([]string, error) {
	call := fn.Called(field)
	return call.Get(0).([]string), call.Error(1)
}

// GetField implements [domain.FieldNavigator].
func (fn *fieldNavigatorMock) GetField(value any, addr ...string) (fields []domain.GetSetter, expanded bool, err error) {
	call := fn.Called(value, addr)
	return call.Get(0).([]domain.GetSetter), call.Bool(1), call.Error(2)
}

// SplitFields implements [domain.FieldNavigator].
func (fn *fieldNavigatorMock) SplitFields(joinedFields string) ([]string, error) {
	call := fn.Called(joinedFields)
	return call.Get(0).([]string), call.Error(1)
}

type indexMock struct{ mock.Mock }

// FieldName implements [domain.Index].
func (i *indexMock) FieldName() string {
	return i.Called().String(0)
}

// GetAll implements [domain.Index].
func (i *indexMock) GetAll() iter.Seq[domain.Document] {
	return slices.Values(i.Called().Get(0).([]domain.Document))
}

// GetBetweenBounds implements [domain.Index].
func (i *indexMock) GetBetweenBounds(ctx context.Context, query domain.Document) (iter.Seq2[domain.Document, error], error) {
	call := i.Called(ctx, query)
	r, err := call.Get(0), call.Error(1)
	if r != nil {
		return r.(iter.Seq2[domain.Document, error]), err
	}
	return nil, err
}

// GetMatching implements [domain.Index].
func (i *indexMock) GetMatching(value ...any) (iter.Seq2[domain.Document, error], error) {
	call := i.Called(value)
	return call.Get(0).(iter.Seq2[domain.Document, error]), call.Error(1)
}

// GetNumberOfKeys implements [domain.Index].
func (i *indexMock) GetNumberOfKeys() int {
	return i.Called().Int(0)
}

// Insert implements [domain.Index].
func (i *indexMock) Insert(ctx context.Context, docs ...domain.Document) error {
	return i.Called(ctx, docs).Error(0)
}

// Remove implements [domain.Index].
func (i *indexMock) Remove(ctx context.Context, docs ...domain.Document) error {
	return i.Called(ctx, docs).Error(0)
}

// Reset implements [domain.Index].
func (i *indexMock) Reset(ctx context.Context, newData ...domain.Document) error {
	return i.Called(ctx, newData).Error(0)
}

// RevertMultipleUpdates implements [domain.Index].
func (i *indexMock) RevertMultipleUpdates(ctx context.Context, pairs ...domain.Update) error {
	return i.Called(ctx, pairs).Error(0)
}

// RevertUpdate implements [domain.Index].
func (i *indexMock) RevertUpdate(ctx context.Context, oldDoc domain.Document, newDoc domain.Document) error {
	return i.Called(ctx, oldDoc, newDoc).Error(0)
}

// Sparse implements [domain.Index].
func (i *indexMock) Sparse() bool {
	return i.Called().Bool(0)
}

// Unique implements [domain.Index].
func (i *indexMock) Unique() bool {
	return i.Called().Bool(0)
}

// Update implements [domain.Index].
func (i *indexMock) Update(ctx context.Context, oldDoc domain.Document, newDoc domain.Document) error {
	return i.Called(ctx, oldDoc, newDoc).Error(0)
}

// UpdateMultipleDocs implements [domain.Index].
func (i *indexMock) UpdateMultipleDocs(ctx context.Context, pairs ...domain.Update) error {
	return i.Called(ctx, pairs).Error(0)
}

type comparerMock struct{ mock.Mock }

// Comparable implements [domain.Comparer].
func (c *comparerMock) Comparable(a any, b any) bool {
	return c.Called(a, b).Bool(0)
}

// Compare implements [domain.Comparer].
func (c *comparerMock) Compare(a any, b any) (int, error) {
	call := c.Called(a, b)
	return call.Int(0), call.Error(1)
}

type idGeneratorMock struct{ mock.Mock }

// GenerateID implements [domain.IDGenerator].
func (i *idGeneratorMock) GenerateID(l int) (string, error) {
	call := i.Called(l)
	return call.String(0), call.Error(1)
}

type serializerMock struct{ mock.Mock }

// Serialize implements [domain.Serializer].
func (s *serializerMock) Serialize(ctx context.Context, value any) ([]byte, error) {
	call := s.Called(ctx, value)
	return []byte(call.String(0)), call.Error(1)
}

type matcherMock struct{ mock.Mock }

// Match implements [domain.Matcher].
func (m *matcherMock) Match(value any) (bool, error) {
	call := m.Called(value)
	return call.Bool(0), call.Error(1)
}

// SetQuery implements [domain.Matcher].
func (m *matcherMock) SetQuery(query any) error {
	return m.Called(query).Error(0)
}

type DatastoreTestSuite struct {
	suite.Suite
	d         *Datastore
	testDb    string
	testDbDir string
}

func (s *DatastoreTestSuite) SetupTest() {
	s.testDbDir = s.T().TempDir()
	s.testDb = filepath.Join(s.testDbDir, "test.db")

	c := comparer.NewComparer()
	dec := decoder.NewDecoder()
	ser := serializer.NewSerializer(c, data.NewDocument)
	des := deserializer.NewDeserializer(dec)
	h := hasher.NewHasher()
	st := storage.NewStorage()

	per, err := persistence.NewPersistence(
		persistence.WithFilename(s.testDb),
		persistence.WithInMemoryOnly(false),
		persistence.WithCorruptAlertThreshold(0.1),
		persistence.WithFileMode(DefaultDirMode),
		persistence.WithDirMode(DefaultDirMode),
		persistence.WithSerializer(ser),
		persistence.WithDeserializer(des),
		persistence.WithStorage(st),
		persistence.WithDecoder(dec),
		persistence.WithComparer(c),
		persistence.WithDocFactory(data.NewDocument),
		persistence.WithHasher(h),
	)
	if !s.NoError(err) {
		return
	}

	fn := fieldnavigator.NewFieldNavigator(data.NewDocument)
	mtchr := matcher.NewMatcher()

	d, err := NewDatastore(
		WithFilename(s.testDb),
		WithTimestamps(false),
		WithInMemoryOnly(false),
		WithSerializer(ser),
		WithDeserializer(des),
		WithCorruptionThreshold(0.1),
		WithComparer(c),
		WithFileMode(DefaultFileMode),
		WithDirMode(DefaultDirMode),
		WithPersistence(per),
		WithStorage(st),
		WithDocumentFactory(data.NewDocument),
		WithDecoder(dec),
		WithMatcher(mtchr),
		WithCursorFactory(cursor.NewCursor),
		WithModifier(modifier.NewModifier(data.NewDocument, c, fn, mtchr)),
		WithTimeGetter(timegetter.NewTimeGetter()),
		WithHasher(h),
		WithFieldNavigator(fn),
		WithIDGenerator(idgenerator.NewIDGenerator()),
		WithRandomReader(rand.Reader),
	)
	s.NoError(err)
	s.d = d.(*Datastore)
	s.NoError(s.d.persistence.(*persistence.Persistence).EnsureParentDirectoryExists(ctx, s.testDb, DefaultDirMode))
	if _, err = os.Stat(s.testDb); err != nil {
		if !os.IsNotExist(err) {
			s.FailNow(err.Error())
		}
	} else {
		s.NoError(os.Remove(s.testDb))
	}

	s.NoError(s.d.LoadDatabase(ctx))
	s.Len(slices.Collect(s.d.getAllData()), 0)
}

func (s *DatastoreTestSuite) SetupSubTest() {
	s.SetupTest()
}

func TestDatastoreTestSuite(t *testing.T) {
	suite.Run(t, new(DatastoreTestSuite))
}

func (s *DatastoreTestSuite) readCursor(cur domain.Cursor) ([]M, error) {
	var res []M
	ctx := context.Background()
	for cur.Next() {
		n := make(M)
		if err := cur.Scan(ctx, &n); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *DatastoreTestSuite) TestLoadInMemoryOnly() {
	db, err := NewDatastore(WithInMemoryOnly(true))
	s.NoError(err)
	s.NotNil(db)

	s.NoError(db.LoadDatabase(context.Background()))
}

func (s *DatastoreTestSuite) TestInvalidFilename() {
	db, err := NewDatastore(WithFilename("t~"))
	s.ErrorIs(err, domain.ErrDatafileName{
		Name:   "t~",
		Reason: "cannot end with '~', reserved for backup files",
	})
	s.Nil(db)
}

func (s *DatastoreTestSuite) TestFailToCreateIDIndex() {
	errIdxFac := fmt.Errorf("index fac error")
	fn := func(...domain.IndexOption) (domain.Index, error) {
		return nil, errIdxFac
	}
	db, err := NewDatastore(WithIndexFactory(fn))
	s.ErrorIs(err, errIdxFac)
	s.Nil(db)
}

func (s *DatastoreTestSuite) TestInsert() {

	s.Run("InsertNoDocs", func() {
		cur, err := s.d.Insert(ctx)
		s.NoError(err)
		s.NotNil(cur)
		s.False(cur.Next())
	})

	// Able to insert a document in the database, setting an _id if none provided, and retrieve it even after a reload
	s.Run("InsertDocAndSetIDIfNotProvidedAndRetrieveAfterReload", func() {
		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal("ok", docs[0]["somedata"])
		s.Contains(docs[0], "_id")

		s.NoError(s.d.LoadDatabase(ctx))
		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal("ok", docs[0]["somedata"])
		s.Contains(docs[0], "_id")
	})

	// Can insert multiple documents in the database
	s.Run("InsertMultipleDocs", func() {
		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "another"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))
		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 3)
		mapped := make([]any, len(docs))
		for n, doc := range docs {
			s.Contains(doc, "somedata")
			mapped[n] = doc["somedata"]
		}
		s.Contains(mapped, "ok")
		s.Contains(mapped, "another")
		s.Contains(mapped, "again")
	})

	// Can insert and get back from DB complex objects with all primitive and secondary types
	s.Run("InsertAndGetComplexObjects", func() {
		da := time.Now()
		obj := map[string]any{"a": []any{"ee", "ff", 42}, "date": da, "subobj": map[string]any{"a": "b", "b": "c"}}

		_ = s.insert(s.d.Insert(ctx, obj))

		var res M
		s.NoError(s.d.FindOne(ctx, nil, &res))

		s.Len(res["a"], 3)
		s.Equal("ee", res["a"].([]any)[0])
		s.Equal("ff", res["a"].([]any)[1])
		s.Equal(42, res["a"].([]any)[2])
		s.Equal(da, res["date"])
		s.Equal("b", res["subobj"].(M)["a"])
		s.Equal("c", res["subobj"].(M)["b"])

	})

	// If an object returned from the DB is modified and refetched, the original value should be found
	s.Run("CannotModifyFetched", func() {
		_, err := s.d.insert(ctx, M{"a": "something"})
		s.NoError(err)

		doc := make(M)
		s.NoError(s.d.FindOne(ctx, nil, &doc))
		s.Equal("something", doc["a"])
		doc["a"] = "another thing"
		s.Equal("another thing", doc["a"])

		doc2 := make(M)
		s.NoError(s.d.FindOne(ctx, nil, &doc2))
		s.Equal("something", doc2["a"])
		doc2["a"] = "another thing"
		s.Equal("another thing", doc2["a"])

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal("something", docs[0]["a"])
	})

	// Cannot insert a doc that has a field beginning with a $ sign
	s.Run("CannotInsertFieldWithDollarPrefix", func() {
		_, err := s.d.Insert(ctx, M{"$something": "atest"})
		s.ErrorIs(err, domain.ErrFieldName{
			Field:  "$something",
			Reason: "cannot start with '$'",
		})
	})

	// If an _id is already given when we insert a document, use that instead of generating a random one
	s.Run("UseCustomIDIfProvided", func() {
		newDoc := s.insert(s.d.Insert(ctx, M{"_id": "test", "stuff": true}))
		s.Equal(true, newDoc[0]["stuff"])

		s.Equal("test", newDoc[0]["_id"])

		_, err := s.d.Insert(ctx, M{"_id": "test", "otherstuff": 42})
		e := bst.ErrUniqueViolated{}
		s.ErrorAs(err, &e)
	})

	// Modifying the insertedDoc after an insert doesn1t change the copy saved in the database
	s.Run("InsertReturnsUnmodifiableDocs", func() {
		newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))
		newDoc[0]["hello"] = "changed"

		doc := make(M)
		s.NoError(s.d.FindOne(ctx, M{"a": 2}, &doc))
		s.Equal("world", doc["hello"])
	})

	// Can insert an array of documents at once
	s.Run("InsertMultipleDocsAtOnce", func() {
		_docs := []any{M{"a": 5, "b": "hello"}, M{"a": 42, "b": "world"}}

		_ = s.insert(s.d.Insert(ctx, _docs...))

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 2)
		docMaps := make(map[int]M)
		for _, value := range docs {
			docMaps[value["a"].(int)] = value
		}
		s.Equal("hello", docMaps[5]["b"])
		s.Equal("world", docMaps[42]["b"])

		b, err := os.ReadFile(s.testDb)
		s.NoError(err)
		lines := bytes.Split(b, []byte("\n"))
		dt := make([]map[string]any, 0, len(lines))
		for _, line := range lines {
			if len(line) > 0 {
				var d map[string]any
				s.NoError(json.Unmarshal(line, &d))
				dt = append(dt, d)
			}
		}

		s.Len(dt, 2)
		s.Equal(5.0, dt[0]["a"])
		s.Equal("hello", dt[0]["b"])
		s.Equal(42.0, dt[1]["a"])
		s.Equal("world", dt[1]["b"])
	})

	// If a bulk insert violates a constraint, all changes are rolled back
	s.Run("RollbackAllIfAnyViolatesConstraint", func() {
		_docs := []any{
			M{"a": 5, "b": "hello"},
			M{"a": 42, "b": "world"},
			M{"a": 5, "b": "bloup"},
			M{"a": 7},
		}

		s.NoError(s.d.EnsureIndex(ctx,
			domain.WithFields("a"),
			domain.WithUnique(true),
		))

		_, err := s.d.Insert(ctx, _docs...)
		e := bst.ErrUniqueViolated{}
		s.ErrorAs(err, &e)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		b, err := os.ReadFile(s.testDb)
		s.NoError(err)
		lines := bytes.Split(b, []byte("\n"))
		dt := make([]any, 0, len(lines))
		for _, line := range lines {
			if len(line) > 0 {
				var d map[string]any
				s.NoError(json.Unmarshal(line, &d))
				dt = append(dt, d)
			}
		}

		s.Equal([]any{map[string]any{"$$indexCreated": map[string]any{"fieldName": "a", "unique": true}}}, dt)
		length := 0
		for cur.Next() {
			length++
		}
		s.NoError(cur.Err())
	})

	// If timestampData option is set, a createdAt field is added and persisted
	s.Run("TimestampDataAddsCreatedAtField", func() {
		newDoc := M{"hello": "world"}

		// precision below milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		insertedDocs := s.insert(d.Insert(ctx, newDoc))
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]

		s.Equal(M{"hello": "world"}, newDoc)
		s.Equal("world", insertedDoc["hello"])
		s.Contains(insertedDoc, "createdAt")
		s.Contains(insertedDoc, "updatedAt")
		s.Equal(insertedDoc["createdAt"], insertedDoc["updatedAt"])
		s.Contains(insertedDoc, "_id")
		s.Len(insertedDoc, 4)
		s.Equal(beginning, insertedDoc["createdAt"])

		insertedDoc["bloup"] = "another"
		s.Len(insertedDoc, 5)

		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(M{
			"hello":     "world",
			"_id":       insertedDoc["_id"],
			"createdAt": insertedDoc["createdAt"],
			"updatedAt": insertedDoc["updatedAt"],
		}, docs[0])

		s.NoError(d.LoadDatabase(ctx))

		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(M{
			"hello":     "world",
			"_id":       insertedDoc["_id"],
			"createdAt": insertedDoc["createdAt"],
			"updatedAt": insertedDoc["updatedAt"],
		}, docs[0])
	})

	// If timestampData option not set, don't create a createdAt and a updatedAt field
	s.Run("IfNotTimestampDataCreatedAtNotAdded", func() {
		insertedDocs := s.insert(s.d.Insert(ctx, M{"hello": "world"}))
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]

		s.Len(insertedDoc, 2)
		s.NotContains(insertedDoc, "createdAt")
		s.NotContains(insertedDoc, "updatedAt")

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)

		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Equal(docs[0], insertedDoc)

	})

	// If timestampData is set but createdAt is specified by user, don't change it
	s.Run("ShouldNotChangeProvidedCreatedAtField", func() {
		newDoc := M{"hello": "world", "createdAt": time.UnixMilli(234)}

		// precision below milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		insertedDocs := s.insert(d.Insert(ctx, newDoc))
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]
		s.Len(insertedDoc, 4)
		s.Equal(time.UnixMilli(234), insertedDoc["createdAt"])
		s.Equal(beginning, insertedDoc["updatedAt"])

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])

		s.NoError(d.LoadDatabase(ctx))
		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])
	})

	// If timestampData is set but updatedAt is specified by user, don't change it
	s.Run("ShouldNotChangeProvidedCreatedAtField", func() {
		newDoc := M{"hello": "world", "updatedAt": time.UnixMilli(234)}

		// precision below milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		insertedDocs := s.insert(d.Insert(ctx, newDoc))
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]
		s.Len(insertedDoc, 4)
		s.Equal(time.UnixMilli(234), insertedDoc["updatedAt"])
		s.Equal(beginning, insertedDoc["createdAt"])

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])

		s.NoError(d.LoadDatabase(ctx))
		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])
	})

	// Can insert a doc with id 0
	s.Run("InsertNumberZeroAsID", func() {
		doc := s.insert(s.d.Insert(ctx, M{"_id": 0, "hello": "world"}))
		s.Equal(0, doc[0]["_id"])
		s.Equal("world", doc[0]["hello"])
	})

	s.Run("FailedRemoveFromIndexes", func() {
		errIdxInsert := fmt.Errorf("index insert error")
		errIdxRemove := fmt.Errorf("index remove error")
		errIdxRemove2 := fmt.Errorf("second index remove error")

		idxMock := new(indexMock)
		s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
			return idxMock, nil
		}

		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("b")))

		// [Datastore.addToIndexes] is called once per document, and
		// removes from previous indexes if any addition fails. To reach
		// [Datastore.removeFromIndexes], at least one addition has to
		// be successfully made. Insert should return nil error twice
		// because we made the same mock be interpreted by [Datastore]
		// as two different instances of [domain.Index].
		idxMock.On("Insert", mock.Anything, mock.Anything).
			Return(nil).
			Twice()

		// Similar to addToIndexes, [domain.Index.Insert] is called once
		// per index, only reaching [domain.Index.Remove] if at least
		// one index successfully inserted the given document, and
		// that's why this first call to Insert should not fail.
		idxMock.On("Insert", mock.Anything, mock.Anything).
			Return(nil).
			Once()

		idxMock.On("Insert", mock.Anything, mock.Anything).
			Return(errIdxInsert).
			Once()
		idxMock.On("Remove", mock.Anything, mock.Anything).
			Return(errIdxRemove).
			Once()
		idxMock.On("Remove", mock.Anything, mock.Anything).
			Return(errIdxRemove2).
			Once()

		cur, err := s.d.Insert(ctx, M{"a": 1}, M{"a": 2})
		s.ErrorIs(err, errIdxInsert)
		s.ErrorIs(err, errIdxRemove)
		s.ErrorIs(err, errIdxRemove2)
		s.Nil(cur)
	})

	s.Run("NonUniqueID", func() {
		idGenMock := new(idGeneratorMock)
		idGenMock.On("GenerateID", 16).Return("123", nil).Once()
		s.d.idGenerator = idGenMock

		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.NoError(err)
		s.NotNil(cur)

		// at this point, GenerateID has been called once
		idGenMock.AssertExpectations(s.T())

		// now that id "123" has been added to indexes,
		// [Datastore.createNewID] will loop until a unique id is
		// returned. It’s a non-halting loop, so I’m capping it at 100
		// iterations.
		idGenMock.On("GenerateID", 16).Return("123", nil).Times(100)

		idGenMock.On("GenerateID", 16).Return("1234", nil).Once()

		cur, err = s.d.Insert(ctx, M{"b": 2})
		s.NoError(err)
		s.NotNil(cur)

		// asserting that GenerateID has been called 100 times
		idGenMock.AssertExpectations(s.T())
	})

	// if idGenerator cannot generate an id, insert fails
	s.Run("FailedGenerateID", func() {
		s.d.idGenerator = idgenerator.NewIDGenerator(
			idgenerator.WithReader(bytes.NewReader(nil)),
		)
		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.ErrorIs(err, io.EOF)
		s.Nil(cur)
	})

	s.Run("FailCheckIfIDIsUnique", func() {
		idxMock := new(indexMock)
		s.d.indexes["_id"] = idxMock

		idGenMock := new(idGeneratorMock)
		idGenMock.On("GenerateID", 16).Return("123", nil).Once()
		s.d.idGenerator = idGenMock

		errGetMtch := fmt.Errorf("get matching error")
		idxMock.On("GetMatching", []any{"123"}).
			Return(maps.All(map[domain.Document]error{}), errGetMtch).
			Once()

		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.ErrorIs(err, errGetMtch)
		s.Nil(cur)
	})

	s.Run("FailedPersistence", func() {
		srMock := new(serializerMock)
		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithSerializer(srMock),
		)
		s.NoError(err)
		s.NotNil(d)

		errSerialize := fmt.Errorf("serialization error")
		srMock.On("Serialize", mock.Anything, mock.Anything).
			Return("", errSerialize).
			Once()

		cur, err := d.Insert(ctx, M{"a": 1})
		s.ErrorIs(err, errSerialize)
		s.Nil(cur)
	})

	s.Run("FailedPrepareDoc", func() {
		errDocFac := fmt.Errorf("doc fac error")
		s.d.documentFactory = func(any) (domain.Document, error) {
			return nil, errDocFac
		}
		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.ErrorIs(err, errDocFac)
		s.Nil(cur)
	})

} // ==== End of 'Insert' ==== //

func (s *DatastoreTestSuite) TestCheckDocument() {
	dollarDate := M{
		"integer":  M{"$$date": int(123)},
		"unsigned": M{"$$date": uint(123)},
		"float32":  M{"$$date": float32(123)},
		"float64":  M{"$$date": float64(123)},
	}

	invalidDollarDate := M{"string": M{"$$date": "invalid"}}

	deleted := M{"$$deleted": true}

	notDeleted := M{"$$deleted": false}

	indexCreated := M{"$$indexCreated": 123}

	indexRemoved := M{"$$indexRemoved": "abc"}

	containsDot := M{"contains.dot": "yes"}

	cur, err := s.d.Insert(ctx, dollarDate)
	s.NoError(err)
	s.NotNil(cur)

	cur, err = s.d.Insert(ctx, invalidDollarDate)
	s.ErrorIs(err, domain.ErrFieldName{
		Field:  "$$date",
		Reason: "cannot start with '$'",
	})
	s.Nil(cur)

	cur, err = s.d.Insert(ctx, deleted)
	s.NoError(err)
	s.NotNil(cur)

	cur, err = s.d.Insert(ctx, notDeleted)
	s.ErrorIs(err, domain.ErrFieldName{
		Field:  "$$deleted",
		Reason: "cannot start with '$'",
	})
	s.Nil(cur)

	cur, err = s.d.Insert(ctx, indexCreated)
	s.NoError(err)
	s.NotNil(cur)

	cur, err = s.d.Insert(ctx, indexRemoved)
	s.NoError(err)
	s.NotNil(cur)

	cur, err = s.d.Insert(ctx, containsDot)
	s.ErrorIs(err, domain.ErrFieldName{
		Field:  "contains.dot",
		Reason: "cannot contain '.'",
	})
	s.Nil(cur)
}

func (s *DatastoreTestSuite) TestGetCandidates() {
	// Can use an index to get docs with a basic match
	s.Run("BasicMatch", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf")))
		_doc1 := s.insert(s.d.Insert(ctx, M{"tf": 4}))
		s.Len(_doc1, 1)
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6}))
		_doc2 := s.insert(s.d.Insert(ctx, M{"tf": 4, "an": "other"}))
		s.Len(_doc2, 1)
		_ = s.insert(s.d.Insert(ctx, M{"tf": 9}))
		dt, err := listCandidates(s.d.getCandidates(ctx, data.M{"r": 6, "tf": 4}, false))
		s.NoError(err)
		s.Len(dt, 2)
		doc1ID := slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })
		s.GreaterOrEqual(doc1ID, 0)
		doc2ID := slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })
		s.GreaterOrEqual(doc2ID, 0)

		doc1 := dt[doc1ID]
		doc2 := dt[doc2ID]

		s.Equal(data.M{"_id": doc1.ID(), "tf": 4}, doc1)
		s.Equal(data.M{"_id": doc2.ID(), "tf": 4, "an": "other"}, doc2)
	})

	// cannot use simple match of list values
	s.Run("MatchSlice", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf")))

		_ = s.insert(s.d.Insert(ctx, M{"tf": []any{4}}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": []any{6}}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": []any{"another"}}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": []any{9}}))

		dt, err := listCandidates(s.d.getCandidates(
			ctx, data.M{"tf": []any{}}, false,
		))
		s.NoError(err)

		s.Equal(slices.Collect(s.d.getAllData()), dt)
	})

	// Can use a compound index to get docs with a basic match
	s.Run("BasicMatchCompoundIndex", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf", "tg")))

		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "tg": 0, "foo": 1}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "tg": 0, "foo": 2}))
		_doc1 := s.insert(s.d.Insert(ctx, M{"tf": 4, "tg": 1, "foo": 3}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "tg": 1, "foo": 4}))
		dt, err := listCandidates(s.d.getCandidates(ctx, data.M{"tf": 4, "tg": 1}, false))
		s.NoError(err)
		s.Len(dt, 1)
		doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
		s.Equal(data.M{"_id": doc1.ID(), "tf": 4, "tg": 1, "foo": 3}, doc1)
	})

	// Can use an index to get docs with a $in match
	s.Run("Match$inOperator", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf")))

		_ = s.insert(s.d.Insert(ctx, M{"tf": 4}))
		_doc1 := s.insert(s.d.Insert(ctx, M{"tf": 6}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "an": "other"}))
		_doc2 := s.insert(s.d.Insert(ctx, M{"tf": 9}))

		dt, err := listCandidates(s.d.getCandidates(ctx, data.M{"tf": data.M{"$in": 1}}, false))
		s.NoError(err)
		s.Len(dt, 0)

		dt, err = listCandidates(s.d.getCandidates(ctx, data.M{"tg": nil}, false))
		s.NoError(err)
		s.Len(dt, 4)

		dt, err = listCandidates(s.d.getCandidates(ctx, data.M{"r": 6, "tf": data.M{"$in": []any{6, 9, 5}}}, false))
		s.NoError(err)
		s.Len(dt, 2)

		doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
		doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]

		s.Len(dt, 2)

		s.Equal(data.M{"_id": doc1.ID(), "tf": 6}, doc1)
		s.Equal(data.M{"_id": doc2.ID(), "tf": 9}, doc2)
	})

	// If no index can be used, return the whole database
	s.Run("ReturnDatabaseIfNoUsabeIndex", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf")))

		_doc1 := s.insert(s.d.Insert(ctx, M{"tf": 4}))
		_doc2 := s.insert(s.d.Insert(ctx, M{"tf": 6}))
		_doc3 := s.insert(s.d.Insert(ctx, M{"tf": 4, "an": "other"}))
		_doc4 := s.insert(s.d.Insert(ctx, M{"tf": 9}))

		dt, err := listCandidates(s.d.getCandidates(ctx, data.M{"r": 6, "notf": data.M{"$in": []any{6, 9, 5}}}, false))
		s.NoError(err)

		doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
		doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]
		doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc3[0]["_id"] })]
		doc4 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc4[0]["_id"] })]

		s.Equal(data.M{"_id": doc1.ID(), "tf": 4}, doc1)
		s.Equal(data.M{"_id": doc2.ID(), "tf": 6}, doc2)
		s.Equal(data.M{"_id": doc3.ID(), "tf": 4, "an": "other"}, doc3)
		s.Equal(data.M{"_id": doc4.ID(), "tf": 9}, doc4)
	})

	// Can use indexes for comparison matches
	s.Run("ComparisonMatch", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf")))

		_ = s.insert(s.d.Insert(ctx, M{"tf": 4}))
		_doc2 := s.insert(s.d.Insert(ctx, M{"tf": 6}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "an": "other"}))
		_doc4 := s.insert(s.d.Insert(ctx, M{"tf": 9}))

		dt, err := listCandidates(s.d.getCandidates(ctx, data.M{"r": 6, "tf": data.M{"$lte": 9, "$gte": 6}}, false))
		s.NoError(err)

		doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]
		doc4 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc4[0]["_id"] })]

		s.Len(dt, 2)

		s.Equal(data.M{"_id": doc2.ID(), "tf": 6}, doc2)
		s.Equal(data.M{"_id": doc4.ID(), "tf": 9}, doc4)
	})

	// Can set a TTL index that expires documents
	s.Run("TLLIndex", func() {
		now := time.Now()
		timeGetter := new(timeGetterMock)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		s.NoError(d.EnsureIndex(ctx,
			domain.WithFields("exp"),
			domain.WithTTL(200*time.Millisecond),
		))

		// will be called on insert and on find
		timeGetter.On("GetTime").Return(now.Add(300 * time.Millisecond))

		_, err = d.Insert(ctx, M{"hello": "world", "exp": now})
		s.NoError(err)

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 0)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.NoError(d.WaitCompaction(ctx))
			b, err := os.ReadFile(s.testDb)
			s.NoError(err)
			s.NotContains(string(b), "world")
		}()
		s.NoError(d.CompactDatafile(ctx))
		wg.Wait()
	})

	// TTL indexes can expire multiple documents and only what needs to be expired
	s.Run("RemoveMultipleExpiredAndKeepOthers", func() {
		now := time.Now()
		timeGetter := new(timeGetterMock)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		s.NoError(d.EnsureIndex(ctx,
			domain.WithFields("exp"),
			domain.WithTTL(200*time.Millisecond),
		))

		// will be called on insert and on find
		firstTimestamp := timeGetter.On("GetTime").Return(now).Times(4)

		_, err = d.Insert(ctx, M{"hello": "world1", "exp": now})
		s.NoError(err)
		_, err = d.Insert(ctx, M{"hello": "world2", "exp": now.Add(50 * time.Millisecond)})
		s.NoError(err)
		_, err = d.Insert(ctx, M{"hello": "world3", "exp": now.Add(100 * time.Millisecond)})
		s.NoError(err)

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 3)

		firstTimestamp.Unset()

		// after first doc (200ms) and second doc (250ms) + 1ms
		secondTimestamp := timeGetter.On("GetTime").Return(now.Add(251 * time.Millisecond))

		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		secondTimestamp.Unset()

		// after third doc (300ms) + 1ms
		timeGetter.On("GetTime").Return(now.Add(301 * time.Millisecond))

		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Document where indexed field is absent or not a date are ignored
	s.Run("IgnoreIfFieldIsNotAValidDate", func() {
		now := time.Now()
		timeGetter := new(timeGetterMock)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		s.NoError(d.LoadDatabase(ctx))

		s.NoError(d.EnsureIndex(ctx, domain.WithFields("exp"), domain.WithTTL(200*time.Millisecond)))

		// will be called on insert and on find
		firstTimestamp := timeGetter.On("GetTime").Return(now).Times(4)

		_, err = d.Insert(ctx, M{"hello": "world1", "exp": now})
		s.NoError(err)
		_, err = d.Insert(ctx, M{"hello": "world2", "exp": "not a date"})
		s.NoError(err)
		_, err = d.Insert(ctx, M{"hello": "world3"})
		s.NoError(err)

		cur, err := d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 3)

		firstTimestamp.Unset()

		timeGetter.On("GetTime").Return(now.Add(301 * time.Millisecond))

		cur, err = d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

	})

	s.Run("FailedDocumentFactory", func() {
		tmGetMock := new(timeGetterMock)
		s.d.timeGetter = tmGetMock
		s.NoError(s.d.EnsureIndex(
			ctx,
			domain.WithFields("a"),
			domain.WithTTL(time.Nanosecond),
		))
		cur, err := s.d.Insert(ctx, M{"a": time.UnixMilli(10000)})
		s.NoError(err)
		s.NotNil(cur)
		insertCount := 0
		for cur.Next() {
			insertCount++
		}
		s.Equal(1, insertCount)

		tmGetMock.On("GetTime").Return(time.UnixMilli(10001)).Once()

		errDocFac := fmt.Errorf("doc fac error")
		var c int
		s.d.documentFactory = func(a any) (domain.Document, error) {
			if c > 0 {
				return nil, errDocFac
			}
			c++
			return data.NewDocument(a)
		}

		cur, err = s.d.Find(ctx, nil)
		s.ErrorIs(err, errDocFac)
		s.Nil(cur)
	})

	s.Run("FailedRemove", func() {
		tmGetMock := new(timeGetterMock)
		s.d.timeGetter = tmGetMock

		s.NoError(s.d.EnsureIndex(
			ctx,
			domain.WithFields("a"),
			domain.WithTTL(time.Nanosecond),
		))
		cur, err := s.d.Insert(ctx, M{"a": time.UnixMilli(10000)})
		s.NoError(err)
		s.NotNil(cur)

		tmGetMock.On("GetTime").
			Return(time.UnixMilli(10001)).
			Once()

		count := 0
		for cur.Next() {
			count++
		}
		s.Equal(1, count)

		errDocFac := fmt.Errorf("doc fac error")
		var c int
		s.d.documentFactory = func(in any) (domain.Document, error) {
			if c < 2 {
				c++
				return data.NewDocument(in)
			}
			return nil, errDocFac
		}

		cur, err = s.d.Find(ctx, nil)
		s.ErrorIs(err, errDocFac)
		s.Nil(cur)
	})

	s.Run("NotAllIndexedFields", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf", "tg")))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "tg": 0, "foo": 1}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "tg": 0, "foo": 2}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "th": 1, "foo": 3}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "th": 1, "foo": 4}))
		dt, err := listCandidates(s.d.getCandidates(
			ctx, data.M{"tf": 4, "th": 1}, false,
		))
		s.NoError(err)
		s.Len(dt, 4) // using no index
	})

	s.Run("CompCandidateIgnoreFieldOfValueDoc", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf", "tg")))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "tg": 0, "j": 1}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "tg": 0, "j": 2}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "th": 1, "j": 3}))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 6, "th": 1, "j": 4}))
		dt, err := listCandidates(s.d.getCandidates(
			ctx, data.M{"tf": 4, "tg": data.M{}}, false,
		))
		s.NoError(err)
		s.Len(dt, 4) // using no index
	})

	s.Run("CompCandidateFailedBounds", func() {
		im := new(indexMock)
		s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
			return im, nil
		}

		errGBB := fmt.Errorf("get between bounds error")
		im.On("Insert", mock.Anything, mock.Anything).Return(nil)
		im.On("GetBetweenBounds", mock.Anything, mock.Anything).Return(nil, errGBB)

		if !s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a"))) {
			return
		}

		_, err := s.d.Insert(ctx, M{"a": 1})
		if !s.NoError(err) {
			return
		}

		cur, err := s.d.Find(ctx, M{"a": M{"$gt": 1}})
		s.ErrorIs(err, errGBB)
		s.Nil(cur)
	})

	s.Run("ComposeFailedGetMatching", func() {
		comp := new(comparerMock)
		s.d.comparer = comp
		errComp := fmt.Errorf("compare error")
		comp.On("Compare", mock.Anything, mock.Anything).
			Return(0, errComp).Once()

		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("tf", "tg")))
		_ = s.insert(s.d.Insert(ctx, M{"tf": 4, "tg": 0, "j": 1}))
		dt, err := listCandidates(s.d.getCandidates(
			ctx, data.M{"tf": 4, "tg": 0}, false,
		))
		s.ErrorIs(err, errComp)
		s.Nil(dt)
	})

	s.Run("EnumCandidates", func() {
		comp := s.d.comparer

		errCmp := fmt.Errorf("compare error")
		c := new(comparerMock)
		s.d.comparer = c
		c.On("Compare", 1, 1).Return(0, errCmp).Twice()

		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
		s.insert(s.d.Insert(ctx, M{"a": 1}))

		s.d.comparer = comp

		cur, err := s.d.Find(ctx, M{"a": M{"$in": A{1}}})
		s.ErrorIs(err, errCmp)
		s.Nil(cur)

		cur, err = s.d.Find(ctx, M{"a": M{"$in": 1}})
		s.ErrorIs(err, errCmp)
		s.Nil(cur)
	})

} // ==== End of 'GetCandidates' ==== //

func (s *DatastoreTestSuite) TestFind() {

	// Can find all documents if an empty query is used
	s.Run("FindAllDocumentsWithEmptyQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "another", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 3)
		somedataValues := make([]any, len(docs))
		for i, doc := range docs {
			somedataValues[i] = doc["somedata"]
		}
		s.Contains(somedataValues, "ok")
		s.Contains(somedataValues, "another")
		s.Contains(somedataValues, "again")

		var docWithPlus M
		for _, doc := range docs {
			if doc["somedata"] == "another" {
				docWithPlus = doc
				break
			}
		}
		s.Equal("additional data", docWithPlus["plus"])
	})

	// Can find all documents matching a basic query
	s.Run("FindDocumentsMatchingBasicQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))

		cur, err := s.d.Find(ctx, M{"somedata": "again"})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		somedataValues := make([]any, len(docs))
		for i, doc := range docs {
			somedataValues[i] = doc["somedata"]
		}
		s.NotContains(somedataValues, "ok")

		// Test with query that doesn't match anything
		cur, err = s.d.Find(ctx, M{"somedata": "nope"})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Can find one document matching a basic query and return null if none is found
	s.Run("FindOneDocumentOrReturnNil", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))

		doc := make(M)
		err := s.d.FindOne(ctx, M{"somedata": "ok"}, &doc)
		s.NoError(err)
		s.Len(doc, 2)
		s.Equal("ok", doc["somedata"])
		s.Contains(doc, "_id")

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"somedata": "nope"}, &doc2)
		// Go implementation returns error instead of nil
		s.ErrorIs(err, domain.ErrNotFound)
	})

	// Can find dates and objects (non JS-native types)
	s.Run("FindDatesAndObjects", func() {
		date1 := time.UnixMilli(1234543)
		date2 := time.UnixMilli(9999)

		_ = s.insert(s.d.Insert(ctx, M{"now": date1, "sth": M{"name": "gedb"}}))

		doc := make(M)
		err := s.d.FindOne(ctx, M{"now": date1}, &doc)
		s.NoError(err)
		s.Equal("gedb", doc["sth"].(M)["name"])

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"now": date2}, &doc2)
		s.ErrorIs(err, domain.ErrNotFound)

		doc3 := make(M)
		err = s.d.FindOne(ctx, M{"sth": M{"name": "gedb"}}, &doc3)
		s.NoError(err)
		s.Equal("gedb", doc3["sth"].(M)["name"])

		doc4 := make(M)
		err = s.d.FindOne(ctx, M{"sth": M{"name": "other"}}, &doc4)
		s.ErrorIs(err, domain.ErrNotFound)
	})

	// Can use dot-notation to query subfields
	s.Run("DotNotationSubfields", func() {
		_ = s.insert(s.d.Insert(ctx, M{"greeting": M{"english": "hello"}}))

		doc := make(M)
		err := s.d.FindOne(ctx, M{"greeting.english": "hello"}, &doc)
		s.NoError(err)
		s.Equal("hello", doc["greeting"].(M)["english"])

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"greeting.english": "hellooo"}, &doc2)
		s.ErrorIs(err, domain.ErrNotFound)

		doc3 := make(M)
		err = s.d.FindOne(ctx, M{"greeting.englis": "hello"}, &doc3)
		s.ErrorIs(err, domain.ErrNotFound)
	})

	// Array fields match if any element matches
	s.Run("ArrayFieldsMatchAnyElement", func() {
		doc1 := s.insert(s.d.Insert(ctx, M{"fruits": []any{"pear", "apple", "banana"}}))
		doc2 := s.insert(s.d.Insert(ctx, M{"fruits": []any{"coconut", "orange", "pear"}}))
		doc3 := s.insert(s.d.Insert(ctx, M{"fruits": []any{"banana"}}))

		cur, err := s.d.Find(ctx, M{"fruits": "pear"})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		ids := make([]any, len(docs))
		for i, doc := range docs {
			ids[i] = doc["_id"]
		}
		s.Contains(ids, doc1[0]["_id"])
		s.Contains(ids, doc2[0]["_id"])

		cur, err = s.d.Find(ctx, M{"fruits": "banana"})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		ids = make([]any, len(docs))
		for i, doc := range docs {
			ids[i] = doc["_id"]
		}
		s.Contains(ids, doc1[0]["_id"])
		s.Contains(ids, doc3[0]["_id"])

		cur, err = s.d.Find(ctx, M{"fruits": "doesntexist"})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Returns an error if the query is not well formed
	s.Run("ErrorOnMalformedQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"hello": "world"}))

		cur, err := s.d.Find(ctx, M{"$or": M{"hello": "world"}})
		s.ErrorAs(err, &matcher.ErrCompArgType{})
		s.Nil(cur)

		doc := make(M)
		err = s.d.FindOne(ctx, M{"$or": M{"hello": "world"}}, &doc)
		s.ErrorAs(err, &matcher.ErrCompArgType{})
	})

	// Changing the documents returned by find or findOne do not change the database state
	s.Run("ReturnedDocsDoNotChangeDatabase", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))

		doc := make(M)
		err := s.d.FindOne(ctx, M{"a": 2}, &doc)
		s.NoError(err)
		doc["hello"] = "changed"

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"a": 2}, &doc2)
		s.NoError(err)
		s.Equal("world", doc2["hello"])

		cur, err := s.d.Find(ctx, M{"a": 2})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		docs[0]["hello"] = "changed"

		doc3 := make(M)
		err = s.d.FindOne(ctx, M{"a": 2}, &doc3)
		s.NoError(err)
		s.Equal("world", doc3["hello"])
	})

	// Can use sort, skip and limit with FindOptions
	s.Run("SortSkipLimitWithFindOptions", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 24, "hello": "earth"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 13, "hello": "blueplanet"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 15, "hello": "home"}))

		cur, err := s.d.Find(ctx, nil,
			domain.WithSort(S{{Key: "a", Order: 1}}),
			domain.WithLimit(2),
		)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)
		s.Equal("world", docs[0]["hello"])
		s.Equal("blueplanet", docs[1]["hello"])
	})

	// Can use sort and skip with FindOne
	s.Run("SortSkipWithFindOne", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 24, "hello": "earth"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 13, "hello": "blueplanet"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 15, "hello": "home"}))

		doc := make(M)
		err := s.d.FindOne(ctx, nil, &doc, domain.WithSort(S{{Key: "a", Order: 1}}))
		s.NoError(err)
		s.Equal("world", doc["hello"])

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"a": M{"$gt": 14}}, &doc2, domain.WithSort(S{{Key: "a", Order: 1}}))
		s.NoError(err)
		s.Equal("home", doc2["hello"])

		doc3 := make(M)
		err = s.d.FindOne(ctx, M{"a": M{"$gt": 14}}, &doc3,
			domain.WithSort(S{{Key: "a", Order: 1}}),
			domain.WithSkip(1),
		)
		s.NoError(err)
		s.Equal("earth", doc3["hello"])

		doc4 := make(M)
		err = s.d.FindOne(ctx, M{"a": M{"$gt": 14}}, &doc4,
			domain.WithSort(S{{Key: "a", Order: 1}}),
			domain.WithSkip(2),
		)
		s.ErrorIs(err, domain.ErrNotFound)
	})

	// Can use projections in find
	s.Run("ProjectionsInFind", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 24, "hello": "earth"}))

		cur, err := s.d.Find(ctx, M{"a": 2},
			domain.WithProjection(M{"a": 0, "_id": 0}),
		)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Equal(M{"hello": "world"}, docs[0])

		cur, err = s.d.Find(ctx, M{"a": 2},
			domain.WithProjection(M{"a": 0, "hello": 1}),
		)
		s.ErrorIs(err, projector.ErrMixOmitType)
		s.Nil(cur)
	})

	// Can use projections in findOne
	s.Run("ProjectionsInFindOne", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "world"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 24, "hello": "earth"}))

		doc := make(M)
		err := s.d.FindOne(ctx, M{"a": 2}, &doc,
			domain.WithProjection(M{"a": 0, "_id": 0}),
		)
		s.NoError(err)
		s.Equal(M{"hello": "world"}, doc)

		doc2 := make(M)
		err = s.d.FindOne(ctx, M{"a": 2}, &doc2,
			domain.WithProjection(M{"a": 0, "hello": 1}),
		)
		s.ErrorIs(err, projector.ErrMixOmitType)
	})

	s.Run("InvalidProjection", func() {
		cur, err := s.d.Find(ctx, nil, domain.WithProjection(1))
		s.ErrorAs(err, &domain.ErrDecode{})
		s.Nil(cur)
	})

	s.Run("FailedGetRawCandidates", func() {
		err := s.d.EnsureIndex(
			ctx,
			domain.WithFields("a", "b"),
		)
		s.NoError(err)

		fn := new(fieldNavigatorMock)
		s.d.fieldNavigator = fn

		errFieldNav := fmt.Errorf("field navigator error")

		fn.On("SplitFields", "_id").
			Return([]string{"_id"}, nil).
			Once().Maybe()
		fn.On("SplitFields", "a,b").
			Return([]string{}, errFieldNav).
			Once()

		cur, err := s.d.Find(ctx, M{"c": 1})

		s.ErrorIs(err, errFieldNav)
		s.Nil(cur)
	})

	s.Run("FailedCloneDocs", func() {
		cur, err := s.d.Insert(ctx, M{"a": []any{M{"b": 1}}})
		s.NoError(err)
		s.NotNil(cur)

		errDocFac := fmt.Errorf("doc fac error")
		var c int
		s.d.documentFactory = func(v any) (domain.Document, error) {
			if v == nil {
				if c > 0 {
					return nil, errDocFac
				}
				c++
			}
			return data.NewDocument(v)
		}

		cur, err = s.d.Find(ctx, M{})
		s.ErrorIs(err, errDocFac)
		s.Nil(cur)
	})

	s.Run("FailedMatchingResult", func() {
		errComp := fmt.Errorf("compare error")

		c := new(comparerMock)
		c.On("Compare", mock.Anything, mock.Anything).
			Return(0, errComp).
			Twice()
		s.d.comparer = c

		err := s.d.EnsureIndex(ctx, domain.WithFields("a"))
		s.NoError(err)

		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.NoError(err)
		s.NotNil(cur)

		cur, err = s.d.Find(ctx, M{"a": 1})
		s.ErrorIs(err, errComp)
		s.Nil(cur)

	})

} // ==== End of 'Find' ==== //

func (s *DatastoreTestSuite) TestGetAllData() {
	docs := []any{
		M{"ch": "oice"},
		M{"ch": "annel"},
		M{"ch": "ase"},
	}

	type Doc struct {
		Ch string
	}

	expected := []Doc{
		{Ch: "oice"},
		{Ch: "annel"},
		{Ch: "ase"},
	}

	curInsert, err := s.d.Insert(ctx, docs...)
	s.NoError(err)
	s.NotNil(curInsert)
	docsInsert := make([]Doc, 3)
	for i := 0; curInsert.Next(); i++ {
		var doc Doc
		s.NoError(curInsert.Scan(ctx, &doc))
		docsInsert[i] = doc
	}

	curFindNil, err := s.d.Find(ctx, nil)
	s.NoError(err)
	s.NotNil(curFindNil)
	docsFindNil := make([]Doc, 3)
	for i := 0; curFindNil.Next(); i++ {
		var doc Doc
		s.NoError(curFindNil.Scan(ctx, &doc))
		docsFindNil[i] = doc
	}

	curFindEmpty, err := s.d.Find(ctx, M{})
	s.NoError(err)
	s.NotNil(curFindEmpty)
	docsFindEmpty := make([]Doc, 3)
	for i := 0; curFindEmpty.Next(); i++ {
		var doc Doc
		s.NoError(curFindEmpty.Scan(ctx, &doc))
		docsFindEmpty[i] = doc
	}

	curGetAllData, err := s.d.GetAllData(ctx)
	s.NoError(err)
	s.NotNil(curGetAllData)
	docsGetAllData := make([]Doc, 3)
	for i := 0; curGetAllData.Next(); i++ {
		var doc Doc
		s.NoError(curGetAllData.Scan(ctx, &doc))
		docsGetAllData[i] = doc
	}
	s.Subset(docsInsert, expected)
	s.Subset(docsFindNil, expected)
	s.Subset(docsFindEmpty, expected)
	s.Subset(docsGetAllData, expected)
}

func (s *DatastoreTestSuite) TestGetAllDataFailedClone() {
	cur, err := s.d.Insert(ctx, M{"a": 1})
	s.NoError(err)
	s.NotNil(cur)

	errDocFac := fmt.Errorf("doc fac error")
	s.d.documentFactory = func(a any) (domain.Document, error) {
		if a != nil {
			return data.NewDocument(a)
		}
		return nil, errDocFac
	}

	cur, err = s.d.GetAllData(ctx)
	s.ErrorIs(err, errDocFac)
	s.Nil(cur)

}

func (s *DatastoreTestSuite) TestCount() {
	// Count all documents if an empty query is used
	s.Run("NoQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "another", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))
		docs, err := s.d.Count(ctx, nil)
		s.NoError(err)
		s.Equal(int64(3), docs)
	})

	// Count all documents matching a basic query
	s.Run("BasicQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))
		docs, err := s.d.Count(ctx, M{"somedata": "again"})
		s.NoError(err)
		s.Equal(int64(2), docs)
		docs, err = s.d.Count(ctx, M{"somedata": "nope"})
		s.NoError(err)
		s.Equal(int64(0), docs)
	})

	// Array fields match if any element matches
	s.Run("ArrayFields", func() {
		_ = s.insert(s.d.Insert(ctx, M{"fruits": []any{"pear", "apple", "banana"}}))
		_ = s.insert(s.d.Insert(ctx, M{"fruits": []any{"coconut", "orange", "pear"}}))
		_ = s.insert(s.d.Insert(ctx, M{"fruits": []any{"banana"}}))

		docs, err := s.d.Count(ctx, M{"fruits": "pear"})
		s.NoError(err)
		s.Equal(int64(2), docs)

		docs, err = s.d.Count(ctx, M{"fruits": "banana"})
		s.NoError(err)
		s.Equal(int64(2), docs)

		docs, err = s.d.Count(ctx, M{"fruits": "doesntexist"})
		s.NoError(err)
		s.Equal(int64(0), docs)
	})

	// Returns an error if the query is not well formed
	s.Run("BadQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"hello": "world"}))
		_, err := s.d.Count(ctx, M{"$or": M{"hello": "world"}})
		s.ErrorAs(err, &matcher.ErrCompArgType{})
	})

	s.Run("InvalidQuery", func() {
		c, err := s.d.Count(ctx, 1)
		s.Error(err)
		s.Zero(c)
	})

	s.Run("FailedGetCandidates", func() {
		comp := new(comparerMock)
		s.d.comparer = comp
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("test")))

		cur, err := s.d.Insert(ctx, M{"test": 123})
		s.NoError(err)
		s.NotNil(cur)

		errComp := fmt.Errorf("compare error")
		comp.On("Compare", 123, 123).Return(0, errComp).Once()

		c, err := s.d.Count(ctx, M{"test": 123})
		s.ErrorIs(err, errComp)
		s.Zero(c)
	})

	s.Run("FailedIteration", func() {
		comp := new(comparerMock)
		s.d.comparer = comp
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))

		comp.On("Compare", 1, 2).Return(-1, nil).Once()
		comp.On("Compare", 3, 2).Return(1, nil).Once()

		cur, err := s.d.Insert(ctx, M{"a": 2}, M{"a": 1}, M{"a": 3})
		s.NoError(err)
		s.NotNil(cur)

		errComp := fmt.Errorf("compare error")
		comp.On("Compare", 2, 1).Return(0, errComp).Once()

		c, err := s.d.Count(ctx, M{"a": M{"$gt": 1}})
		s.ErrorIs(err, errComp)
		s.Zero(c)
	})

	s.Run("FailedMatch", func() {
		m := new(matcherMock)
		s.d.matcher = m

		cur, err := s.d.Insert(ctx, M{"a": 2}, M{"a": 1}, M{"a": 3})
		s.NoError(err)
		s.NotNil(cur)

		errMatch := fmt.Errorf("match error")

		m.On("SetQuery", nil).Return(nil).Once()
		m.On("Match", mock.Anything).Return(false, errMatch).Once()

		c, err := s.d.Count(ctx, nil)
		s.ErrorIs(err, errMatch)
		s.Zero(c)
	})

} // ==== End of 'Count' ==== //

func (s *DatastoreTestSuite) TestUpdate() {

	// If the query doesn't match anything, database is not modified
	s.Run("NoChangeIfNoMatch", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "another"}))

		n := s.update(s.d.Update(ctx, M{"somedata": "nope"}, M{"newDoc": "yes"}, domain.WithUpdateMulti(true)))
		s.Len(n, 0)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		doc1 := docs[slices.IndexFunc(docs, func(d M) bool { return d["somedata"] == "ok" })]
		doc2 := docs[slices.IndexFunc(docs, func(d M) bool { return d["somedata"] == "again" })]
		doc3 := docs[slices.IndexFunc(docs, func(d M) bool { return d["somedata"] == "another" })]

		s.Len(docs, 3)
		for _, doc := range docs {
			s.NotContains(doc, "newDoc")
		}

		s.Equal(M{"_id": doc1["_id"], "somedata": "ok"}, doc1)
		s.Equal(M{"_id": doc2["_id"], "somedata": "again", "plus": "additional data"}, doc2)
		s.Equal(M{"_id": doc3["_id"], "somedata": "another"}, doc3)
	})

	// If timestampData option is set, update the updatedAt field
	s.Run("PatchUpdatedAtField", func() {

		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		call := timeGetter.On("GetTime").Return(beginning)

		d, err := NewDatastore(
			WithFilename(s.testDb),
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)
		insertedDocs := s.insert(d.Insert(ctx, M{"hello": "world"}))

		call.Unset()

		s.Equal(beginning, insertedDocs[0]["updatedAt"])
		s.Equal(beginning, insertedDocs[0]["createdAt"])
		s.Len(insertedDocs[0], 4)

		timeGetter.On("GetTime").Return(beginning.Add(time.Millisecond))
		n := s.update(d.Update(ctx, M{"_id": insertedDocs[0]["_id"]}, M{"$set": M{"hello": "mars"}}))
		s.Len(n, 1)

		cur, err := d.Find(ctx, M{"_id": insertedDocs[0]["_id"]})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 4)
		s.Equal(insertedDocs[0]["_id"], docs[0]["_id"])
		s.Equal(insertedDocs[0]["createdAt"], docs[0]["createdAt"])
		s.Equal(beginning.Add(time.Millisecond), docs[0]["updatedAt"])
		s.Equal("mars", docs[0]["hello"])

	})

	// Can update multiple documents matching the query
	s.Run("MultipleMatches", func() {

		_doc1 := s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		id1 := _doc1[0]["_id"]

		_doc2 := s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		id2 := _doc2[0]["_id"]

		_doc3 := s.insert(s.d.Insert(ctx, M{"somedata": "again"}))
		id3 := _doc3[0]["_id"]

		n := s.update(s.d.Update(ctx, M{"somedata": "again"}, M{"newDoc": "yes"}, domain.WithUpdateMulti(true)))
		s.Len(n, 2)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		doc1 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id1 })]
		doc2 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id2 })]
		doc3 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id3 })]

		s.Len(docs, 3)

		s.Len(doc1, 2)
		s.Equal("ok", doc1["somedata"])
		// removed redundant _id assertion

		s.Len(doc2, 2)
		s.Equal("yes", doc2["newDoc"])
		// removed redundant _id assertion

		s.Len(doc3, 2)
		s.Equal("yes", doc3["newDoc"])
		// removed redundant _id assertion

	})

	// Can update only one document matching the query
	s.Run("MultiDisabled", func() {
		_doc1 := s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		id1 := _doc1[0]["_id"]
		_doc2 := s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		id2 := _doc2[0]["_id"]
		_doc3 := s.insert(s.d.Insert(ctx, M{"somedata": "again"}))
		id3 := _doc3[0]["_id"]

		n := s.update(s.d.Update(ctx, M{"somedata": "again"}, M{"newDoc": "yes"}, domain.WithUpdateMulti(false)))
		s.Len(n, 1)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		doc1 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id1 })]
		doc2 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id2 })]
		doc3 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id3 })]

		s.Equal(M{"_id": doc1["_id"], "somedata": "ok"}, doc1)
		if len(doc2) == 2 {
			s.Equal(M{"_id": doc2["_id"], "newDoc": "yes"}, doc2)
			s.Equal(M{"_id": doc3["_id"], "somedata": "again"}, doc3)
		} else {
			s.Equal(M{"_id": doc2["_id"], "somedata": "again", "plus": "additional data"}, doc2)
			s.Equal(M{"_id": doc3["_id"], "newDoc": "yes"}, doc3)
		}

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)

		doc1 = docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id1 })]
		doc2 = docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id2 })]
		doc3 = docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == id3 })]

		s.Equal(M{"_id": doc1["_id"], "somedata": "ok"}, doc1)
		if len(doc2) == 2 {
			s.Equal(M{"_id": doc2["_id"], "newDoc": "yes"}, doc2)
			s.Equal(M{"_id": doc3["_id"], "somedata": "again"}, doc3)
		} else {
			s.Equal(M{"_id": doc2["_id"], "somedata": "again", "plus": "additional data"}, doc2)
			s.Equal(M{"_id": doc3["_id"], "newDoc": "yes"}, doc3)
		}
	})

	s.Run("Upsert", func() {

		// Can perform upserts if needed
		s.Run("Simple", func() {
			n := s.update(s.d.Update(ctx, M{"impossible": "db is empty anyway"}, M{"newDoc": true}))
			s.Len(n, 0)

			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 0)

			n = s.update(s.d.Update(ctx, M{"impossible": "db is empty anyway"}, M{"something": "created ok"}, domain.WithUpsert(true)))
			s.Len(n, 1)

			cur, err = s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err = s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 1)
			s.Equal("created ok", docs[0]["something"])

			// original test would check if returned updated
			// documents could modify the actual values in index,
			// but update here does not return documents.
		})

		// If the update query is a normal object with no modifiers, it is the doc that will be upserted
		s.Run("UseQueryIfNoDollarFields", func() {
			qry := M{"$or": []any{M{"a": 4}, M{"a": 5}}}
			update := M{"hello": "world", "bloup": "blap"}
			n := s.update(s.d.Update(ctx, qry, update, domain.WithUpsert(true)))
			s.Len(n, 1)
			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 3)
			s.Equal("world", doc["hello"])
			s.Equal("blap", doc["bloup"])
		})

		// If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 1
		s.Run("UseNonOperatorsFromFindQuery", func() {
			qry := M{"$or": []any{M{"a": 4}, M{"a": 5}}}
			update := M{
				"$set": M{"hello": "world"},
				"$inc": M{"bloup": 3},
			}
			n := s.update(s.d.Update(ctx, qry, update, domain.WithUpsert(true)))
			s.Len(n, 1)

			cur, err := s.d.Find(ctx, M{"hello": "world"})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 3)
			s.Equal("world", doc["hello"])
			s.Equal(float64(3), doc["bloup"])
		})

		// If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 2
		s.Run("UseNonOperatorsFromFindQuery", func() {
			qry := M{
				"$or": []any{M{"a": 4}, M{"a": 5}},
				"cac": "rrr",
			}
			update := M{
				"$set": M{"hello": "world"},
				"$inc": M{"bloup": 3},
			}
			n := s.update(s.d.Update(ctx, qry, update, domain.WithUpsert(true)))
			s.Len(n, 1)

			cur, err := s.d.Find(ctx, M{"hello": "world"})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 4)
			s.Equal("rrr", doc["cac"])
			s.Equal("world", doc["hello"])
			s.Equal(float64(3), doc["bloup"])
		})

		// Performing upsert with badly formatted fields yields a standard error not an exception
		s.Run("BadField", func() {
			_, err := s.d.Update(ctx, M{"_id": "1234"}, M{"$set": M{"$$badfield": 5}}, domain.WithUpsert(true))
			s.ErrorIs(err, domain.ErrFieldName{
				Field:  "$$badfield",
				Reason: "cannot start with '$'",
			})
		})

		s.Run("InvalidQuery", func() {
			cur, err := s.d.Update(
				ctx, 1, nil,
				domain.WithUpsert(true),
			)
			s.ErrorAs(err, &domain.ErrDocumentType{})
			s.Nil(cur)
		})

		s.Run("MultiResult", func() {
			cur, err := s.d.Insert(ctx,
				M{"a": 1},
				M{"a": 2},
			)
			s.NoError(err)
			s.NotNil(cur)

			cur, err = s.d.Update(
				ctx,
				M{"a": M{"$gt": 0}},
				M{"b": true},
				domain.WithUpsert(true),
				domain.WithUpdateMulti(true),
			)
			s.NoError(err)
			s.NotNil(cur)

			cur, err = s.d.Find(
				ctx, nil,
				domain.WithProjection(M{"_id": 0}),
				domain.WithSort(
					domain.Sort{{Key: "a", Order: 1}},
				),
			)
			s.NoError(err)
			res := []M{}
			for cur.Next() {
				m := M{}
				s.NoError(cur.Scan(ctx, &m))
				res = append(res, m)
			}
			s.Equal([]M{{"b": true}, {"b": true}}, res)
		})

		s.Run("FailedDocumentFactory", func() {
			errDocFac := fmt.Errorf("document factory error")
			var c int
			s.d.documentFactory = func(v any) (domain.Document, error) {
				if c < 2 {
					c++
					return data.NewDocument(v)
				}
				return nil, errDocFac
			}

			cur, err := s.d.Update(ctx, nil, nil, domain.WithUpsert(true))
			s.ErrorIs(err, errDocFac)
			s.Nil(cur)
		})

		s.Run("FailedModification", func() {
			cur, err := s.d.Update(ctx, nil, M{"a": 2, "$test": 3}, domain.WithUpsert(true))
			s.ErrorIs(err, modifier.ErrMixedOperators)
			s.Nil(cur)
		})

	}) // ==== End of 'Upserts' ==== //

	// Cannot perform update if the update query is not either registered-modifiers-only or copy-only, or contain badly formatted fields
	s.Run("ErrorBadField", func() {
		_ = s.insert(s.d.Insert(ctx, M{"somethnig": "yup"}))

		_, err := s.d.Update(ctx, nil, M{"$badField": 5})
		s.ErrorIs(err, modifier.ErrUnknownModifier{Name: "$badField"})

		_, err = s.d.Update(ctx, nil, M{"bad.field": 5})
		s.ErrorIs(err, domain.ErrFieldName{
			Field:  "bad.field",
			Reason: "cannot contain '.'",
		})

		_, err = s.d.Update(ctx, nil, M{
			"$inc":  M{"test": 5},
			"mixed": "rrr",
		})
		s.ErrorIs(err, modifier.ErrMixedOperators)

		_, err = s.d.Update(ctx, nil, M{
			"$inexistent": M{"test": 5},
		})

		s.ErrorIs(err, modifier.ErrUnknownModifier{Name: "$inexistent"})
	})

	// Can update documents using multiple modifiers
	s.Run("MultipleModifiers", func() {
		newDoc := s.insert(s.d.Insert(ctx, M{"something": "yup", "other": 40}))
		id := newDoc[0]["_id"]

		n := s.update(s.d.Update(ctx, nil, M{"$set": M{"something": "changed"}, "$inc": M{"other": 10}}))
		s.Len(n, 1)

		var doc M
		s.NoError(s.d.FindOne(ctx, M{"_id": id}, &doc))
		s.Len(doc, 3)
		s.Equal(id, doc["_id"])
		s.Equal("changed", doc["something"])
		s.Equal(float64(50), doc["other"])
	})

	// Can upsert a document even with modifiers
	s.Run("UpsertWithModifiers", func() {
		n := s.update(s.d.Update(ctx, M{"bloup": "blap"}, M{"$set": M{"hello": "world"}}, domain.WithUpsert(true)))
		s.Len(n, 1)
		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 3)
		s.Equal("world", docs[0]["hello"])
		s.Equal("blap", docs[0]["bloup"])
		s.Contains(docs[0], "_id")
	})

	// When using modifiers, the only way to update subdocs is with the dot-notation
	s.Run("UpdateSubdocWithModifier", func() {
		_ = s.insert(s.d.Insert(ctx, M{"bloup": M{"blip": "blap", "other": true}}))
		n := s.update(s.d.Update(ctx, nil, M{"$set": M{"bloup.blip": "hello"}}))
		s.Len(n, 1)

		var doc M
		s.NoError(s.d.FindOne(ctx, nil, &doc))
		s.Equal("hello", doc["bloup"].(M)["blip"])
		s.Equal(true, doc["bloup"].(M)["other"])

		// Wrong
		n = s.update(s.d.Update(ctx, nil, M{"$set": M{"bloup": M{"blip": "ola"}}}))
		s.Len(n, 1)

		s.NoError(s.d.FindOne(ctx, nil, &doc))
		s.Equal("ola", doc["bloup"].(M)["blip"])
		s.NotContains(doc["bloup"], "other")

	})

	// Returns an error if the query is not well formed
	s.Run("BadQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"hello": "world"}))
		_, err := s.d.Update(ctx, M{"$or": M{"hello": "world"}}, M{"a": 1})
		s.ErrorAs(err, &matcher.ErrCompArgType{})
	})

	// If an error is thrown by a modifier, the database state is not changed
	s.Run("NoChangeIfModificationError", func() {
		newDocs := s.insert(s.d.Insert(ctx, M{"hello": "world"}))
		n, err := s.d.Update(ctx, nil, M{"$inc": M{"hello": 4}})
		s.ErrorAs(err, &modifier.ErrModFieldType{})
		s.Nil(n, 0)

		var doc M
		s.NoError(s.d.FindOne(ctx, nil, &doc))
		s.Equal(newDocs[0], doc)
	})

	// Can't change the _id of a document
	s.Run("CannotChangeID", func() {
		newDocs := s.insert(s.d.Insert(ctx, M{"a": 2}))

		_, err := s.d.Update(ctx, M{"a": 2}, M{"a": 2, "_id": "nope"})
		s.ErrorIs(err, domain.ErrCannotModifyID)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(2, docs[0]["a"])
		s.Equal(newDocs[0]["_id"], docs[0]["_id"])

		_, err = s.d.Update(ctx, M{"a": 2}, M{"$set": M{"_id": "nope"}})
		s.ErrorIs(err, domain.ErrCannotModifyID)

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(2, docs[0]["a"])
		s.Equal(newDocs[0]["_id"], docs[0]["_id"])
	})

	// Non-multi updates are persistent
	s.Run("PersistSingleUpdate", func() {
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "hello": "world"}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "earth"}))
		n := s.update(s.d.Update(ctx, M{"a": 2}, M{"$set": M{"hello": "changed"}}))
		s.Len(n, 1)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b M) int { return a["a"].(int) - b["a"].(int) })
		s.Len(docs, 2)
		s.Equal(M{"_id": doc1[0]["_id"], "a": 1, "hello": "world"}, docs[0])
		s.Equal(M{"_id": doc2[0]["_id"], "a": 2, "hello": "changed"}, docs[1])
	})

	// Multi updates are persistent
	s.Run("PersistMultipleUpdates", func() {
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "hello": "world"}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "earth"}))
		doc3 := s.insert(s.d.Insert(ctx, M{"a": 5, "hello": "pluton"}))
		n := s.update(s.d.Update(ctx, M{"a": 2}, M{"$set": M{"hello": "changed"}}))
		s.Len(n, 1)

		n = s.update(s.d.Update(ctx, M{"a": M{"$in": []any{1, 2}}}, M{"$set": M{"hello": "changed"}}, domain.WithUpdateMulti(true)))
		s.Len(n, 2)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b M) int { return a["a"].(int) - b["a"].(int) })
		s.Len(docs, 3)
		s.Equal(M{"_id": doc1[0]["_id"], "a": 1, "hello": "changed"}, docs[0])
		s.Equal(M{"_id": doc2[0]["_id"], "a": 2, "hello": "changed"}, docs[1])
		s.Equal(M{"_id": doc3[0]["_id"], "a": 5, "hello": "pluton"}, docs[2])

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b M) int { return int(a["a"].(float64)) - int(b["a"].(float64)) })
		s.Len(docs, 3)

		// now numbers are float because they rave been serialized and then deserialized as json numbers
		s.Equal(M{"_id": doc1[0]["_id"], "a": float64(1), "hello": "changed"}, docs[0])
		s.Equal(M{"_id": doc2[0]["_id"], "a": float64(2), "hello": "changed"}, docs[1])
		s.Equal(M{"_id": doc3[0]["_id"], "a": float64(5), "hello": "pluton"}, docs[2])

	})

	// NOTE: did not add idiomatic js test 'Can update without the options arg (will use defaults then)'

	// If a multi update fails on one document, previous updates should be rolled back
	s.Run("RollbackAllOnError", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 4}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 5}))
		doc3 := s.insert(s.d.Insert(ctx, M{"a": "abc"}))

		qry := M{"a": M{"$in": []any{4, 5, "abc"}}}
		update := M{"$inc": M{"a": 10}}
		n, err := s.d.Update(ctx, qry, update, domain.WithUpdateMulti(true))
		s.ErrorAs(err, &modifier.ErrModFieldType{})
		s.Nil(n, 0)

		for _, idx := range s.d.indexes {
			docs := slices.Collect(idx.GetAll())
			d1 := docs[slices.IndexFunc(docs, func(d domain.Document) bool { return d.ID().(string) == doc1[0]["_id"].(string) })]
			d2 := docs[slices.IndexFunc(docs, func(d domain.Document) bool { return d.ID().(string) == doc2[0]["_id"].(string) })]
			d3 := docs[slices.IndexFunc(docs, func(d domain.Document) bool { return d.ID().(string) == doc3[0]["_id"].(string) })]

			s.Equal(4, d1.Get("a"))
			s.Equal(5, d2.Get("a"))
			s.Equal("abc", d3.Get("a"))
		}

	})

	// If an index constraint is violated by an update, all changes should be rolled back
	s.Run("RespectIndexConstraints", func() {
		s.NoError(s.d.EnsureIndex(ctx,
			domain.WithFields("a"),
			domain.WithUnique(true),
		))
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 4}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 5}))

		qry := M{"a": M{"$in": []any{4, 5, "abc"}}}
		update := M{"$set": M{"a": 10}}
		n, err := s.d.Update(ctx, qry, update, domain.WithUpdateMulti(true))
		s.ErrorIs(err, domain.ErrConstraintViolated)
		s.Nil(n, 0)

		for _, idx := range s.d.indexes {
			docs := slices.Collect(idx.GetAll())
			d1 := docs[slices.IndexFunc(docs, func(d domain.Document) bool { return d.ID().(string) == doc1[0]["_id"].(string) })]
			d2 := docs[slices.IndexFunc(docs, func(d domain.Document) bool { return d.ID().(string) == doc2[0]["_id"].(string) })]

			s.Equal(4, d1.Get("a"))
			s.Equal(5, d2.Get("a"))
		}
	})

	// NOTE: did not add test 'If options.returnUpdatedDocs is true, return all matched docs'
	// because there is no option returnUpdatedDocs in this package

	// createdAt property is unchanged and updatedAt correct after an update, even a complete document replacement
	s.Run("KeepCreatedAtOnUpdate", func() {
		beginning := time.Now().Truncate(time.Millisecond)
		timeGetter := new(timeGetterMock)
		d2, err := NewDatastore(
			WithTimestamps(true),
			WithTimeGetter(timeGetter),
		)
		s.NoError(err)

		call := timeGetter.On("GetTime").Return(beginning)

		_, err = d2.Insert(ctx, M{"a": 1})
		s.NoError(err)

		var doc M
		s.NoError(d2.FindOne(ctx, M{"a": 1}, &doc))
		s.NoError(err)
		createdAt := doc["createdAt"]

		// unset after find because it gets time to remove expired docs
		call.Unset()

		timeGetter.On("GetTime").Return(beginning.Add(time.Second))

		n := s.update(d2.Update(ctx, M{"a": 1}, M{"$set": M{"b": 2}}))
		s.Len(n, 1)

		doc = nil
		s.NoError(d2.FindOne(ctx, M{"a": 1}, &doc))
		s.NoError(err)
		s.Equal(createdAt, doc["createdAt"])

		timeGetter.On("GetTime").Return(beginning.Add(time.Minute))

		n = s.update(d2.Update(ctx, M{"a": 1}, M{"c": 3}))
		s.Len(n, 1)

		doc = nil
		s.NoError(d2.FindOne(ctx, M{"c": 3}, &doc))
		s.NoError(err)
		s.Equal(createdAt, doc["createdAt"])
	})

	// NOTE: 'Callback signature' tests not added because we don't use
	// callbacks

	s.Run("FailedDocumentFactory", func() {
		errDocFac := fmt.Errorf("document factory error")
		s.d.documentFactory = func(any) (domain.Document, error) {
			return nil, errDocFac
		}
		cur, err := s.d.Update(ctx, M{}, M{})
		s.ErrorIs(err, errDocFac)
		s.Nil(cur)
	})

} // ==== End of 'Update' ==== //

func (s *DatastoreTestSuite) TestRemove() {

	// Can remove multiple documents
	s.Run("MultipleDocs", func() {
		_doc1 := s.insert(s.d.Insert(ctx, M{"somedata": "ok"}))
		id1 := _doc1[0]["_id"]
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again", "plus": "additional data"}))
		_ = s.insert(s.d.Insert(ctx, M{"somedata": "again"}))

		n, err := s.d.Remove(ctx, M{"somedata": "again"}, domain.WithRemoveMulti(true))
		s.NoError(err)
		s.Equal(int64(2), n)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(id1, docs[0]["_id"])

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(id1, docs[0]["_id"])
	})

	// Remove can be called multiple times in parallel and everything that needs to be removed will be
	s.Run("ParallelCalls", func() {
		// context mutex should protect everything
		_ = s.insert(s.d.Insert(ctx, M{"planet": "Earth"}))
		_ = s.insert(s.d.Insert(ctx, M{"planet": "Mars"}))
		_ = s.insert(s.d.Insert(ctx, M{"planet": "Saturn"}))

		mu := &sync.Mutex{}
		removeStartWG := &sync.WaitGroup{}
		removeStartWG.Add(2)

		wg := &sync.WaitGroup{}
		c := sync.NewCond(mu)

		for _, planet := range [...]string{"Mars", "Saturn"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mu.Lock()
				removeStartWG.Done()
				c.Wait()
				mu.Unlock()
				_, err := s.d.Remove(ctx, M{"planet": planet})
				s.NoError(err)
			}()
		}

		removeStartWG.Wait() // wait until all removal goroutines are locked
		mu.Lock()
		c.Broadcast()
		mu.Unlock()

		wg.Wait() // wait until all goroutines finished removing

		count, err := s.d.Count(ctx, nil)
		s.NoError(err)
		s.Equal(int64(1), count)
	})

	// Returns an error if the query is not well formed
	s.Run("BadQuery", func() {
		_ = s.insert(s.d.Insert(ctx, M{"hello": "world"}))
		badQuery := M{"$or": M{"hello": "world"}}
		n, err := s.d.Remove(ctx, badQuery)
		s.ErrorAs(err, &matcher.ErrCompArgType{})
		s.Zero(n)
	})

	// Non-multi removes are persistent
	s.Run("PersistSingleRemove", func() {
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "hello": "world"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "earth"}))
		doc3 := s.insert(s.d.Insert(ctx, M{"a": 3, "hello": "moto"}))

		n, err := s.d.Remove(ctx, M{"a": 2})
		s.NoError(err)
		s.Equal(int64(1), n)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b M) int { return a["a"].(int) - b["a"].(int) })
		s.Len(docs, 2)

		s.Equal(M{"_id": doc1[0]["_id"], "a": 1, "hello": "world"}, doc1[0])
		s.Equal(M{"_id": doc3[0]["_id"], "a": 3, "hello": "moto"}, doc3[0])

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		// default deserializer unmarshals any number as float64
		slices.SortFunc(docs, func(a, b M) int { return int(a["a"].(float64)) - int(b["a"].(float64)) })
		s.Len(docs, 2)

		s.Equal(M{"_id": doc1[0]["_id"], "a": float64(1), "hello": "world"}, docs[0])
		s.Equal(M{"_id": doc3[0]["_id"], "a": float64(3), "hello": "moto"}, docs[1])
	})

	// Multi removes are persistent
	s.Run("PersistMultipleRemoves", func() {
		_ = s.insert(s.d.Insert(ctx, M{"a": 1, "hello": "world"}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "earth"}))
		_ = s.insert(s.d.Insert(ctx, M{"a": 3, "hello": "moto"}))

		n, err := s.d.Remove(ctx, M{"a": M{"$in": []any{1, 3}}})
		s.NoError(err)
		s.Equal(int64(2), n)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(M{"_id": doc2[0]["_id"], "a": 2, "hello": "earth"}, docs[0])

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(M{"_id": doc2[0]["_id"], "a": float64(2), "hello": "earth"}, docs[0])
	})

	// Can remove without the options arg (will use defaults then)
	s.Run("NoArgs", func() {
		doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "hello": "world"}))
		doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "hello": "earth"}))
		doc3 := s.insert(s.d.Insert(ctx, M{"a": 5, "hello": "moto"}))

		n, err := s.d.Remove(ctx, M{"a": 2})
		s.NoError(err)
		s.Equal(int64(1), n)

		cur, err := s.d.Find(ctx, nil)
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		d1Index := slices.IndexFunc(docs, func(d M) bool { return d["_id"] == doc1[0]["_id"] })
		d2Index := slices.IndexFunc(docs, func(d M) bool { return d["_id"] == doc2[0]["_id"] })
		d3Index := slices.IndexFunc(docs, func(d M) bool { return d["_id"] == doc3[0]["_id"] })

		s.Equal(1, docs[d1Index]["a"])
		s.Negative(d2Index)
		s.Equal(5, docs[d3Index]["a"])
	})

	s.Run("FailedRemoveFromIndexes", func() {
		errIdxRemove := fmt.Errorf("index remove error")

		cur, err := s.d.Insert(ctx, M{"a": 1})
		s.NoError(err)
		s.NotNil(cur)

		idxMock := new(indexMock)
		s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
			return idxMock, nil
		}
		// when creating a new index, existent docs are inserted.
		idxMock.On("Insert", mock.Anything, mock.Anything).
			Return(nil).
			Once()
		s.NoError(s.d.EnsureIndex(
			ctx,
			domain.WithFields("a"),
		))

		idxMock.On("Remove", mock.Anything, mock.Anything).
			Return(errIdxRemove).
			Once()

		removed, err := s.d.Remove(ctx, M{})
		s.ErrorIs(err, errIdxRemove)
		s.Zero(removed)
	})

	s.Run("FailedDocumentFactory", func() {
		errDocFac := fmt.Errorf("doc fac error")
		s.d.documentFactory = func(any) (domain.Document, error) {
			return nil, errDocFac
		}
		count, err := s.d.Remove(ctx, M{"a": 1})
		s.ErrorIs(err, errDocFac)
		s.Zero(count)
	})

	s.Run("FailedPersistence", func() {
		cur, err := s.d.Insert(ctx, M{})
		s.NoError(err)
		s.NotNil(cur)

		srMock := new(serializerMock)
		per, err := persistence.NewPersistence(
			persistence.WithFilename(s.testDb),
			persistence.WithSerializer(srMock),
		)
		s.NoError(err)
		s.NotNil(per)
		s.d.persistence = per

		errSerialize := fmt.Errorf("serialization error")
		srMock.On("Serialize", mock.Anything, mock.Anything).
			Return("", errSerialize).
			Once()

		count, err := s.d.Remove(ctx, nil)
		s.ErrorIs(err, errSerialize)
		s.Zero(count)
	})

} // ==== End of 'Remove' ==== //

func (s *DatastoreTestSuite) TestIndexes() {

	// ensureIndex and index initialization in database loading
	s.Run("EnsureOnLoad", func() {

		// ensureIndex can be called right after a loadDatabase and be initialized and filled correctly
		s.Run("EnsureIndexOnLoad", func() {
			now := time.Now()
			s.Len(slices.Collect(s.d.getAllData()), 0)

			buf := make([]byte, 0, 1024)
			docs := [...]M{
				{"_id": "aaa", "z": "1", "a": 2, "ages": []any{1, 5, 12}},
				{"_id": "bbb", "z": "2", "hello": "world"},
				{"_id": "ccc", "z": "3", "nested": M{"today": now}},
			}
			ser := serializer.NewSerializer(comparer.NewComparer(), data.NewDocument)
			for _, doc := range docs {
				b, err := ser.Serialize(ctx, doc)
				s.NoError(err)
				buf = append(buf, append(b, '\n')...)
			}

			s.NoError(os.WriteFile(s.testDb, buf, DefaultFileMode))

			s.NoError(s.d.LoadDatabase(ctx))
			s.Len(slices.Collect(s.d.getAllData()), 3)

			s.Equal([]string{"_id"}, slices.Collect(maps.Keys(s.d.indexes)))

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.Equal("z", s.d.indexes["z"].FieldName())
			s.False(s.d.indexes["z"].Unique())
			s.False(s.d.indexes["z"].Sparse())
			s.Equal(3, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal(3, s.d.indexes["z"].GetNumberOfKeys())

			s1, err := s.d.indexes["z"].(*index.Index).Tree.Search("1")
			s.NoError(err)
			s2, err := s.d.indexes["z"].(*index.Index).Tree.Search("2")
			s.NoError(err)
			s3, err := s.d.indexes["z"].(*index.Index).Tree.Search("3")
			s.NoError(err)

			allData := slices.Collect(s.d.getAllData())
			s.Equal(allData[0], s1.Values()[0])
			s.Equal(allData[1], s2.Values()[0])
			s.Equal(allData[2], s3.Values()[0])
		})

		s.Run("InvalidIndex", func() {
			f := s.d.indexFactory
			defer func() { s.d.indexFactory = f }()
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))

			cur, err := s.d.Insert(ctx, M{"a": true})
			s.NoError(err)
			s.NotNil(cur)

			errIdx := errors.New("index error")
			s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
				return nil, errIdx
			}

			s.ErrorIs(s.d.LoadDatabase(ctx), errIdx)
		})

		// ensureIndex can be called twice on the same field, the second call will have no effect
		s.Run("EnsureIndexTwice", func() {
			s.Len(s.d.indexes, 1)
			s.Equal("_id", slices.Collect(maps.Keys(s.d.indexes))[0])

			_ = s.insert(s.d.Insert(ctx, M{"planet": "Earth"}))
			_ = s.insert(s.d.Insert(ctx, M{"planet": "Mars"}))

			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 2)

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("planet")))
			s.Len(s.d.indexes, 2)

			indexNames := slices.Collect(maps.Keys(s.d.indexes))
			slices.Sort(indexNames)

			s.Equal("_id", indexNames[0])
			s.Equal("planet", indexNames[1])
			s.Len(slices.Collect(s.d.getAllData()), 2)

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("planet")))
			s.Len(s.d.indexes, 2)

			indexNames = slices.Collect(maps.Keys(s.d.indexes))
			slices.Sort(indexNames)

			s.Equal("_id", indexNames[0])
			s.Equal("planet", indexNames[1])
			s.Len(slices.Collect(s.d.getAllData()), 2)
		})

		// ensureIndex can be called twice on the same compound fields, the second call will have no effect
		s.Run("EnsureCompoundIndexTwice", func() {
			s.Len(s.d.indexes, 1)
			s.Equal("_id", slices.Collect(maps.Keys(s.d.indexes))[0])

			_ = s.insert(s.d.Insert(ctx, M{"star": "sun", "planet": "Earth"}))
			_ = s.insert(s.d.Insert(ctx, M{"star": "sun", "planet": "Mars"}))

			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 2)

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("star", "planet")))
			s.Len(s.d.indexes, 2)

			indexNames := slices.Collect(maps.Keys(s.d.indexes))
			slices.Sort(indexNames)

			s.Equal("_id", indexNames[0])
			s.Equal("planet,star", indexNames[1])
			s.Len(slices.Collect(s.d.getAllData()), 2)

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("star", "planet")))
			s.Len(s.d.indexes, 2)

			indexNames = slices.Collect(maps.Keys(s.d.indexes))
			slices.Sort(indexNames)

			s.Equal("_id", indexNames[0])
			s.Equal("planet,star", indexNames[1])
			s.Len(slices.Collect(s.d.getAllData()), 2)
		})

		// ensureIndex cannot be called with an illegal field name
		s.Run("IllegalFieldName", func() {
			err := s.d.EnsureIndex(ctx, domain.WithFields("star,planet"))
			s.ErrorIs(err, domain.ErrFieldName{
				Field:  "star,planet",
				Reason: "cannot contain ','",
			})
			err = s.d.EnsureIndex(ctx, domain.WithFields("star,planet", "other"))
			s.ErrorIs(err, domain.ErrFieldName{
				Field:  "star,planet",
				Reason: "cannot contain ','",
			})
		})

		// ensureIndex can be called after the data set was modified and the index still be correct
		s.Run("AfterModifyingData", func() {
			buf := make([]byte, 0, 1024)
			_docs := [...]M{
				{"_id": "aaa", "z": "1", "a": 2, "ages": []any{1, 5, 12}},
				{"_id": "bbb", "z": "2", "hello": "world"},
			}
			ser := serializer.NewSerializer(comparer.NewComparer(), data.NewDocument)
			for _, doc := range _docs {
				b, err := ser.Serialize(ctx, doc)
				s.NoError(err)
				buf = append(buf, append(b, '\n')...)
			}

			s.Len(slices.Collect(s.d.getAllData()), 0)

			s.NoError(os.WriteFile(s.testDb, buf, DefaultFileMode))
			s.NoError(s.d.LoadDatabase(ctx))

			s.Len(slices.Collect(s.d.getAllData()), 2)

			s.Equal([]string{"_id"}, slices.Collect(maps.Keys(s.d.indexes)))

			newDoc1 := s.insert(s.d.Insert(ctx, M{"z": "12", "yes": "yes"}))
			newDoc2 := s.insert(s.d.Insert(ctx, M{"z": "14", "nope": "nope"}))
			_, err := s.d.Remove(ctx, M{"z": "2"})
			s.NoError(err)
			_ = s.update(s.d.Update(ctx, M{"z": "1"}, M{"$set": M{"yes": "yep"}}))

			s.Equal([]string{"_id"}, slices.Collect(maps.Keys(s.d.indexes)))

			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))

			s.Equal("z", s.d.indexes["z"].FieldName())
			s.False(s.d.indexes["z"].Unique())
			s.False(s.d.indexes["z"].Sparse())
			s.Equal(3, s.d.indexes["z"].GetNumberOfKeys())

			matching, err := listMatching(s.d.indexes["_id"].GetMatching("aaa"))
			s.NoError(err)

			s1, err := s.d.indexes["z"].(*index.Index).Tree.Search("1")
			s.NoError(err)
			s12, err := s.d.indexes["z"].(*index.Index).Tree.Search("12")
			s.NoError(err)
			s14, err := s.d.indexes["z"].(*index.Index).Tree.Search("14")
			s.NoError(err)

			s.Equal(matching[0], s1.Values()[0])
			matching, err = listMatching(s.d.indexes["_id"].GetMatching(newDoc1[0]["_id"]))
			s.NoError(err)
			s.Equal(matching[0], s12.Values()[0])

			matching, err = listMatching(s.d.indexes["_id"].GetMatching(newDoc2[0]["_id"]))
			s.NoError(err)
			s.Equal(matching[0], s14.Values()[0])

			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			doc0 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == "aaa" })]
			doc1 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == newDoc1[0]["_id"] })]
			doc2 := docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == newDoc2[0]["_id"] })]

			s.Len(docs, 3)

			s.Equal(M{"_id": "aaa", "z": "1", "a": float64(2), "ages": []any{float64(1), float64(5), float64(12)}, "yes": "yep"}, doc0)
			s.Equal(M{"_id": newDoc1[0]["_id"], "z": "12", "yes": "yes"}, doc1)
			s.Equal(M{"_id": newDoc2[0]["_id"], "z": "14", "nope": "nope"}, doc2)
		})

		// ensureIndex can be called before a loadDatabase and still be initialized and filled correctly
		s.Run("BeforeLoadDatabase", func() {
			now := time.Now()
			buf := make([]byte, 0, 1024)
			_docs := [...]M{
				{"_id": "aaa", "z": "1", "a": 2, "ages": []any{1, 5, 12}},
				{"_id": "bbb", "z": "2", "hello": "world"},
				{"_id": "ccc", "z": "3", "nested": M{"today": now}},
			}
			ser := serializer.NewSerializer(comparer.NewComparer(), data.NewDocument)
			for _, doc := range _docs {
				b, err := ser.Serialize(ctx, doc)
				s.NoError(err)
				buf = append(buf, append(b, '\n')...)
			}

			s.Len(slices.Collect(s.d.getAllData()), 0)
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.Equal("z", s.d.indexes["z"].FieldName())
			s.False(s.d.indexes["z"].Unique())
			s.False(s.d.indexes["z"].Sparse())
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())

			s.NoError(os.WriteFile(s.testDb, buf, DefaultFileMode))
			s.NoError(s.d.LoadDatabase(ctx))

			dt := slices.Collect(s.d.getAllData())
			doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "1" })]
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "2" })]
			doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "3" })]

			s.Len(dt, 3)

			s.Equal(3, s.d.indexes["z"].GetNumberOfKeys())

			s1, err := s.d.indexes["z"].(*index.Index).Tree.Search("1")
			s.NoError(err)
			s2, err := s.d.indexes["z"].(*index.Index).Tree.Search("2")
			s.NoError(err)
			s3, err := s.d.indexes["z"].(*index.Index).Tree.Search("3")
			s.NoError(err)

			s.Equal(doc1, s1.Values()[0])
			s.Equal(doc2, s2.Values()[0])
			s.Equal(doc3, s3.Values()[0])
		})

		// Can initialize multiple indexes on a database load
		s.Run("InitializeMultipleIndexOnLoad", func() {
			// this date has to be truncated because it will be
			// persisted and loaded again
			now := time.Now().Truncate(time.Millisecond)
			buf := make([]byte, 0, 1024)
			_docs := [...]data.M{
				{"_id": "aaa", "z": "1", "a": 2, "ages": []any{1, 5, 12}},
				{"_id": "bbb", "z": "2", "a": "world"},
				{"_id": "ccc", "z": "3", "a": data.M{"today": now}},
			}
			ser := serializer.NewSerializer(comparer.NewComparer(), data.NewDocument)
			for _, doc := range _docs {
				b, err := ser.Serialize(ctx, doc)
				s.NoError(err)
				buf = append(buf, append(b, '\n')...)
			}

			s.Len(slices.Collect(s.d.getAllData()), 0)
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))

			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal(0, s.d.indexes["a"].GetNumberOfKeys())

			s.NoError(os.WriteFile(s.testDb, buf, DefaultFileMode))
			s.NoError(s.d.LoadDatabase(ctx))

			dt := slices.Collect(s.d.getAllData())
			doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "1" })]
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "2" })]
			doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.Get("z") == "3" })]

			s.Len(dt, 3)

			s.Equal(3, s.d.indexes["z"].GetNumberOfKeys())

			s1, err := s.d.indexes["z"].(*index.Index).Tree.Search("1")
			s.NoError(err)
			s.Equal(doc1, s1.Values()[0])
			s2, err := s.d.indexes["z"].(*index.Index).Tree.Search("2")
			s.NoError(err)
			s.Equal(doc2, s2.Values()[0])
			s3, err := s.d.indexes["z"].(*index.Index).Tree.Search("3")
			s.NoError(err)
			s.Equal(doc3, s3.Values()[0])

			s.Equal(3, s.d.indexes["a"].GetNumberOfKeys())

			s2n, err := s.d.indexes["a"].(*index.Index).Tree.Search(2)
			s.NoError(err)
			world, err := s.d.indexes["a"].(*index.Index).Tree.Search("world")
			s.NoError(err)
			today, err := s.d.indexes["a"].(*index.Index).Tree.Search(data.M{"today": now})
			s.NoError(err)

			s.Equal(doc1, s2n.Values()[0])
			s.Equal(doc2, world.Values()[0])
			s.Equal(doc3, today.Values()[0])
		})

		// If a unique constraint is not respected, database loading will not work and no data will be inserted
		s.Run("LoadPersistedConstraintViolation", func() {
			now := time.Now()
			buf := make([]byte, 0, 1024)
			_docs := [...]M{
				{"_id": "aaa", "z": "1", "a": 2, "ages": []any{1, 5, 12}},
				{"_id": "bbb", "z": "2", "a": "world"},
				{"_id": "ccc", "z": "1", "a": M{"today": now}},
			}
			ser := serializer.NewSerializer(comparer.NewComparer(), data.NewDocument)
			for _, doc := range _docs {
				b, err := ser.Serialize(ctx, doc)
				s.NoError(err)
				buf = append(buf, append(b, '\n')...)
			}

			s.Len(slices.Collect(s.d.getAllData()), 0)
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("z"),
				domain.WithUnique(true),
			))

			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())

			s.NoError(os.WriteFile(s.testDb, buf, DefaultFileMode))
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(s.d.LoadDatabase(ctx), &e)
			s.Len(slices.Collect(s.d.getAllData()), 0)
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())
		})

		// If a unique constraint is not respected, ensureIndex will return an error and not create an index
		s.Run("NotCreateIndexWithViolatedConstraint", func() {
			_ = s.insert(s.d.Insert(ctx, M{"a": 1, "b": 4}))
			_ = s.insert(s.d.Insert(ctx, M{"a": 2, "b": 45}))
			_ = s.insert(s.d.Insert(ctx, M{"a": 1, "b": 3}))

			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("b"),
				domain.WithUnique(true),
			))

			err := s.d.EnsureIndex(ctx,
				domain.WithFields("a"),
				domain.WithUnique(true),
			)
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)
		})

		// Can remove an index
		s.Run("RemoveIndex", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("e")))
			s.Len(s.d.indexes, 2)
			s.Contains(s.d.indexes, "e")
			s.NoError(s.d.RemoveIndex(ctx, "e"))
			s.Len(s.d.indexes, 1)
			s.NotContains(s.d.indexes, "e")
		})

		s.Run("RemoveIndexComma", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a", "b")))
			s.Len(s.d.indexes, 2)
			s.Contains(s.d.indexes, "a,b")
			err := s.d.RemoveIndex(ctx, "a,b")
			s.ErrorIs(err, domain.ErrFieldName{
				Field:  "a,b",
				Reason: "cannot contain ','",
			})
			s.Len(s.d.indexes, 2)
			s.Contains(s.d.indexes, "a,b")
		})

		s.Run("RemoveFailedDocumentFactory", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("e")))
			s.Len(s.d.indexes, 2)
			s.Contains(s.d.indexes, "e")
			errDocFac := fmt.Errorf("document factory error")
			s.d.documentFactory = func(any) (domain.Document, error) {
				return nil, errDocFac
			}
			s.ErrorIs(s.d.RemoveIndex(ctx, "e"), errDocFac)

			// removed from cached, but cannot persist deletion
			s.Len(s.d.indexes, 1)
			s.NotContains(s.d.indexes, "e")

			s.d.documentFactory = data.NewDocument
			s.NoError(s.d.LoadDatabase(ctx))

			// index is recreated if db is reloaded
			s.Len(s.d.indexes, 2)
			s.Contains(s.d.indexes, "e")
		})

	}) // ==== End of 'ensureIndex and index initialization in database loading' ==== //

	// Indexing newly inserted documents
	s.Run("IndexNew", func() {

		// Newly inserted documents are indexed
		s.Run("IndexNewlyInsertedDocs", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())

			newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "z": "yes"}))
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			newDoc = s.insert(s.d.Insert(ctx, M{"a": 5, "z": "nope"}))
			s.Equal(2, s.d.indexes["z"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["z"].GetMatching("nope"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)
		})

		// If multiple indexes are defined, the document is inserted in all of them
		s.Run("InsertMultipleIndexes", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("ya")))
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal(0, s.d.indexes["ya"].GetNumberOfKeys())

			newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "z": "yes", "ya": "indeed"}))
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["ya"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)
			matching, err = listMatching(s.d.indexes["ya"].GetMatching("indeed"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			newDoc2 := s.insert(s.d.Insert(ctx, M{"a": 5, "z": "nope", "ya": "sure"}))
			s.Equal(2, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal(2, s.d.indexes["ya"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["z"].GetMatching("nope"))
			s.NoError(err)
			s.EqualDocs(newDoc2, matching)
			matching, err = listMatching(s.d.indexes["ya"].GetMatching("sure"))
			s.NoError(err)
			s.EqualDocs(newDoc2, matching)

		})

		// Can insert two docs at the same key for a non unique index
		s.Run("AllowRepeatedNonUniqueIndexKey", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("z")))
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())

			newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "z": "yes"}))
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			newDoc2 := s.insert(s.d.Insert(ctx, M{"a": 5, "z": "yes"}))
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(append(newDoc, newDoc2...), matching)

		})

		// If the index has a unique constraint, an error is thrown if it is violated and the data is not modified
		s.Run("NotModifyIfViolates", func() {
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("z"),
				domain.WithUnique(true),
			))
			s.Equal(0, s.d.indexes["z"].GetNumberOfKeys())

			newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "z": "yes"}))
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			newDoc2, err := s.d.Insert(ctx, M{"a": 5, "z": "yes"})
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)
			s.Nil(newDoc2)
			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			s.Equal("yes", e.Key)

			s.Equal(1, s.d.indexes["z"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["z"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			allData := slices.Collect(s.d.getAllData())
			s.EqualDocs(newDoc, allData)
			s.NoError(s.d.LoadDatabase(ctx))
			allData = slices.Collect(s.d.getAllData())
			s.Equal(data.M{"_id": newDoc[0]["_id"], "a": 2.0, "z": "yes"}, allData[0])
		})

		// If an index has a unique constraint, other indexes cannot be modified when it raises an error
		s.Run("NotModifyOthersIfViolates", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("nonu1")))
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("uni"),
				domain.WithUnique(true),
			))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("nonu2")))

			newDoc := s.insert(s.d.Insert(ctx, M{"nonu1": "yes", "nonu2": "yes2", "uni": "willfail"}))
			s.Equal(1, s.d.indexes["nonu1"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["uni"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["nonu2"].GetNumberOfKeys())

			_, err := s.d.Insert(ctx, M{"nonu1": "no", "nonu2": "no2", "uni": "willfail"})
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)

			s.Equal(1, s.d.indexes["nonu1"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["uni"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["nonu2"].GetNumberOfKeys())

			matching, err := listMatching(s.d.indexes["nonu1"].GetMatching("yes"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)
			matching, err = listMatching(s.d.indexes["uni"].GetMatching("willfail"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)
			matching, err = listMatching(s.d.indexes["nonu2"].GetMatching("yes2"))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

		})

		// Unique indexes prevent you from inserting two docs where the
		// field is undefined except if they're sparse
		s.Run("SparseAcceptUnset", func() {
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("zzz"),
				domain.WithUnique(true),
			))
			s.Equal(0, s.d.indexes["zzz"].GetNumberOfKeys())

			newDoc := s.insert(s.d.Insert(ctx, M{"a": 2, "z": "yes"}))
			s.Equal(1, s.d.indexes["zzz"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["zzz"].GetMatching(nil))
			s.NoError(err)
			s.EqualDocs(newDoc, matching)

			_, err = s.d.Insert(ctx, M{"a": 5, "z": "other"})
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)

			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("yyy"),
				domain.WithUnique(true),
				domain.WithSparse(true),
			))

			_ = s.insert(s.d.Insert(ctx, M{"a": 5, "z": "other", "zzz": "set"}))
			s.Equal(0, s.d.indexes["yyy"].GetNumberOfKeys())
			s.Equal(2, s.d.indexes["zzz"].GetNumberOfKeys())
		})

		// Insertion still works as before with indexing
		s.Run("InsertWithIndexing", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("b")))

			doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))
			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Equal(doc1[0], docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == doc1[0]["_id"] })])
			s.Equal(doc2[0], docs[slices.IndexFunc(docs, func(d M) bool { return d["_id"] == doc2[0]["_id"] })])
		})

		// All indexes point to the same data as the main index on _id
		s.Run("AllIndexesHaveSameData", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))

			doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))
			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 2)
			s.Len(slices.Collect(s.d.getAllData()), 2)

			matching, err := listMatching(s.d.indexes["_id"].GetMatching(doc1[0]["_id"]))
			s.NoError(err)
			s.Len(matching, 1)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			s.Len(matching, 1)
			matching, err = listMatching(s.d.indexes["_id"].GetMatching(doc1[0]["_id"]))
			s.NoError(err)
			expected, err := listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			s.Equal(expected[0], matching[0])

			matching, err = listMatching(s.d.indexes["_id"].GetMatching(doc2[0]["_id"]))
			s.NoError(err)
			s.Len(matching, 1)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Len(matching, 1)
			matching, err = listMatching(s.d.indexes["_id"].GetMatching(doc2[0]["_id"]))
			s.NoError(err)
			expected, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(expected[0], matching[0])
		})

		// If a unique constraint is violated, no index is changed, including the main one
		s.Run("NoChangeOnUniqueViolation", func() {
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("a"),
				domain.WithUnique(true),
			))

			doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))

			_, err := s.d.Insert(ctx, M{"a": 1, "b": "si"})
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)

			cur, err := s.d.Find(ctx, nil)
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Len(docs, 1)
			s.Len(slices.Collect(s.d.getAllData()), 1)

			matching, err := listMatching(s.d.indexes["_id"].GetMatching(doc1[0]["_id"]))
			s.NoError(err)
			s.Len(matching, 1)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			s.Len(matching, 1)
			expected, err := listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["_id"].GetMatching(docs[0]["_id"]))
			s.NoError(err)
			s.Equal(expected[0], matching[0])

			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Len(matching, 0)
		})
	}) // ==== End of 'Indexing newly inserted documents' ==== //

	// Updating indexes upon document update
	s.Run("Update", func() {

		s.Run("UpdateWithIndexing", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))

			_doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			_doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))

			n := s.update(s.d.Update(ctx, M{"a": 1}, M{"$set": M{"a": 456, "b": "no"}}))
			s.Len(n, 1)

			dt := slices.Collect(s.d.getAllData())
			doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]

			s.Len(dt, 2)
			s.Equal(data.M{"a": 456, "b": "no", "_id": _doc1[0]["_id"]}, doc1)
			s.Equal(data.M{"a": 2, "b": "si", "_id": _doc2[0]["_id"]}, doc2)

			n = s.update(s.d.Update(ctx, nil, M{"$inc": M{"a": 10}, "$set": M{"b": "same"}}, domain.WithUpdateMulti(true)))
			s.Len(n, 2)

			dt = slices.Collect(s.d.getAllData())
			doc1 = dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
			doc2 = dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]

			s.Len(dt, 2)
			s.Equal(data.M{"a": 466.0, "b": "same", "_id": _doc1[0]["_id"]}, doc1)
			s.Equal(data.M{"a": 12.0, "b": "same", "_id": _doc2[0]["_id"]}, doc2)
		})

		// Indexes get updated when a document (or multiple documents) is updated
		s.Run("", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("b")))

			doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))

			n := s.update(s.d.Update(ctx, M{"a": 1}, M{"$set": M{"a": 456, "b": "no"}}))
			s.Len(n, 1)

			s.Equal(2, s.d.indexes["a"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["a"].GetMatching(456))
			s.NoError(err)
			s.Equal(doc1[0]["_id"], matching[0].ID())
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(doc2[0]["_id"], matching[0].ID())

			s.Equal(2, s.d.indexes["b"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("no"))
			s.NoError(err)
			s.Equal(doc1[0]["_id"], matching[0].ID())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("si"))
			s.NoError(err)
			s.Equal(doc2[0]["_id"], matching[0].ID())

			s.Equal(2, s.d.indexes["a"].GetNumberOfKeys())
			s.Equal(2, s.d.indexes["b"].GetNumberOfKeys())
			s.Equal(2, s.d.indexes["_id"].GetNumberOfKeys())

			expected, err := listMatching(s.d.indexes["_id"].GetMatching(doc1[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(456))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("no"))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())

			expected, err = listMatching(s.d.indexes["_id"].GetMatching(doc2[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("si"))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())

			n = s.update(s.d.Update(ctx, nil, M{"$inc": M{"a": 10}, "$set": M{"b": "same"}}, domain.WithUpdateMulti(true)))
			s.Len(n, 2)

			s.Equal(2, s.d.indexes["a"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["a"].GetMatching(466))
			s.NoError(err)
			s.Equal(doc1[0]["_id"], matching[0].ID())
			matching, err = listMatching(s.d.indexes["a"].GetMatching(12))
			s.NoError(err)
			s.Equal(doc2[0]["_id"], matching[0].ID())

			s.Equal(1, s.d.indexes["b"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("same"))
			s.NoError(err)
			s.Len(matching, 2)
			matching, err = listMatching(s.d.indexes["b"].GetMatching("same"))
			s.NoError(err)
			ids := make([]any, len(matching))
			for n, m := range matching {
				ids[n] = m.ID()
			}
			s.Contains(ids, doc1[0]["_id"])
			s.Contains(ids, doc2[0]["_id"])

			s.Equal(2, s.d.indexes["a"].GetNumberOfKeys())
			s.Equal(1, s.d.indexes["b"].GetNumberOfKeys())
			s.Len(slices.Collect(s.d.indexes["b"].GetAll()), 2)
			s.Equal(2, s.d.indexes["_id"].GetNumberOfKeys())

			expected, err = listMatching(s.d.indexes["_id"].GetMatching(doc1[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(466))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
			expected, err = listMatching(s.d.indexes["_id"].GetMatching(doc2[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(12))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
		})

		// If a simple update violates a constraint, all changes are
		// rolled back and an error is thrown
		s.Run("RollbackAllOnViolationSimple", func() {
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("a"),
				domain.WithUnique(true),
			))
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("b"),
				domain.WithUnique(true),
			))
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("c"),
				domain.WithUnique(true),
			))

			_doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": 10, "c": 100}))
			_doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": 20, "c": 200}))
			_doc3 := s.insert(s.d.Insert(ctx, M{"a": 3, "b": 30, "c": 300}))

			n, err := s.d.Update(ctx, M{"a": 2}, M{"$inc": M{"a": 10, "c": 1000}, "$set": M{"b": 30}})
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)
			s.Nil(n, 0)

			dt := slices.Collect(s.d.getAllData())
			doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]
			doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc3[0]["_id"] })]

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["a"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["a"].GetMatching(3))
			s.NoError(err)
			s.Equal(doc3, matching[0])

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["b"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["b"].GetMatching(10))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["b"].GetMatching(20))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["b"].GetMatching(30))
			s.NoError(err)
			s.Equal(doc3, matching[0])

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["c"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["c"].GetMatching(100))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["c"].GetMatching(200))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["c"].GetMatching(300))
			s.NoError(err)
			s.Equal(doc3, matching[0])
		})

		// If a multi update violates a constraint, all changes are
		// rolled back and an error is thrown
		s.Run("RollbackAllOnViolationMulti", func() {
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("a"),
				domain.WithUnique(true),
			))
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("b"),
				domain.WithUnique(true),
			))
			s.NoError(s.d.EnsureIndex(ctx,
				domain.WithFields("c"),
				domain.WithUnique(true),
			))

			_doc1 := s.insert(s.d.Insert(ctx, M{"a": 1, "b": 10, "c": 100}))
			_doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": 20, "c": 200}))
			_doc3 := s.insert(s.d.Insert(ctx, M{"a": 3, "b": 30, "c": 300}))

			n, err := s.d.Update(ctx, M{"a": M{"$in": []any{1, 2}}}, M{"$inc": M{"a": 10, "c": 1000}, "$set": M{"b": 30}}, domain.WithUpdateMulti(true))
			e := bst.ErrUniqueViolated{}
			s.ErrorAs(err, &e)
			s.Nil(n, 0)

			dt := slices.Collect(s.d.getAllData())
			doc1 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc1[0]["_id"] })]
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]
			doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc3[0]["_id"] })]

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["a"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["a"].GetMatching(1))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["a"].GetMatching(3))
			s.NoError(err)
			s.Equal(doc3, matching[0])

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["b"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["b"].GetMatching(10))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["b"].GetMatching(20))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["b"].GetMatching(30))
			s.NoError(err)
			s.Equal(doc3, matching[0])

			s.Len(dt, 3)
			s.Equal(3, s.d.indexes["c"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["c"].GetMatching(100))
			s.NoError(err)
			s.Equal(doc1, matching[0])
			matching, err = listMatching(s.d.indexes["c"].GetMatching(200))
			s.NoError(err)
			s.Equal(doc2, matching[0])
			matching, err = listMatching(s.d.indexes["c"].GetMatching(300))
			s.NoError(err)
			s.Equal(doc3, matching[0])
		})

	}) // ==== End of 'Updating indexes upon document update' ==== //

	// Updating indexes upon document remove
	s.Run("UpdateIndexOnRemoveDocs", func() {

		// Removing docs still works as before with indexing
		s.Run("UpdateIndexOnRemoveDocs", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
			_ = s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			_doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))
			_doc3 := s.insert(s.d.Insert(ctx, M{"a": 3, "b": "coin"}))
			n, err := s.d.Remove(ctx, M{"a": 1})
			s.NoError(err)
			s.Equal(int64(1), n)

			dt := slices.Collect(s.d.getAllData())
			doc2 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc2[0]["_id"] })]
			doc3 := dt[slices.IndexFunc(dt, func(d domain.Document) bool { return d.ID() == _doc3[0]["_id"] })]

			s.Len(dt, 2)
			s.Equal(data.M{"a": 2, "b": "si", "_id": _doc2[0]["_id"]}, doc2)
			s.Equal(data.M{"a": 3, "b": "coin", "_id": _doc3[0]["_id"]}, doc3)

			n, err = s.d.Remove(ctx, M{"a": M{"$in": []any{2, 3}}}, domain.WithRemoveMulti(true))
			s.NoError(err)
			s.Equal(int64(2), n)

			dt = slices.Collect(s.d.getAllData())
			s.Len(dt, 0)
		})

		// Indexes get updated when a document (or multiple documents) is removed
		s.Run("UpdateIndexesOnRemoveMulti", func() {
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("a")))
			s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("b")))
			_ = s.insert(s.d.Insert(ctx, M{"a": 1, "b": "hello"}))
			doc2 := s.insert(s.d.Insert(ctx, M{"a": 2, "b": "si"}))
			doc3 := s.insert(s.d.Insert(ctx, M{"a": 3, "b": "coin"}))
			n, err := s.d.Remove(ctx, M{"a": 1})
			s.NoError(err)
			s.Equal(int64(1), n)

			s.Equal(2, s.d.indexes["a"].GetNumberOfKeys())
			matching, err := listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(doc2[0]["_id"], matching[0].ID())
			matching, err = listMatching(s.d.indexes["a"].GetMatching(3))
			s.NoError(err)
			s.Equal(doc3[0]["_id"], matching[0].ID())

			s.Equal(2, s.d.indexes["b"].GetNumberOfKeys())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("si"))
			s.NoError(err)
			s.Equal(doc2[0]["_id"], matching[0].ID())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("coin"))
			s.NoError(err)
			s.Equal(doc3[0]["_id"], matching[0].ID())

			s.Equal(2, s.d.indexes["_id"].GetNumberOfKeys())

			expected, err := listMatching(s.d.indexes["_id"].GetMatching(doc2[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(2))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("si"))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())

			expected, err = listMatching(s.d.indexes["_id"].GetMatching(doc3[0]["_id"]))
			s.NoError(err)
			matching, err = listMatching(s.d.indexes["a"].GetMatching(3))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
			matching, err = listMatching(s.d.indexes["b"].GetMatching("coin"))
			s.NoError(err)
			s.Equal(reflect.ValueOf(expected[0]).Pointer(), reflect.ValueOf(matching[0]).Pointer())
		})

		s.Run("FailedRevert", func() {
			idxMock := new(indexMock)
			s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
				return idxMock, nil
			}
			idxMock.On("Insert", mock.Anything, mock.Anything).
				Return(nil).
				Times(4)
			s.NoError(s.d.EnsureIndex(
				ctx, domain.WithFields("a")),
			)
			s.NoError(s.d.EnsureIndex(
				ctx, domain.WithFields("b")),
			)
			_ = s.insert(s.d.Insert(
				ctx, M{"a": 1}, M{"a": 2},
			))

			errUpdMultiDoc := fmt.Errorf("update multi doc error")
			errRevMultiDoc := fmt.Errorf("revert multi doc error")
			idxMock.On("UpdateMultipleDocs", mock.Anything, mock.Anything).
				Return(nil).
				Once()
			idxMock.On("UpdateMultipleDocs", mock.Anything, mock.Anything).
				Return(errUpdMultiDoc).
				Once()
			idxMock.On("RevertMultipleUpdates", mock.Anything, mock.Anything).
				Return(errRevMultiDoc).
				Once()

			cur, err := s.d.Update(ctx, M{}, M{})
			s.ErrorIs(err, errUpdMultiDoc)
			s.ErrorIs(err, errRevMultiDoc)
			s.Nil(cur)
		})

	}) // ==== End of 'Updating indexes upon document remove' ==== //

	// Persisting indexes
	s.Run("Persistence", func() {

		// Indexes are persisted to a separate file and recreated upon reload
		s.Run("PersistAndReload", func() {
			persDB := filepath.Join(s.testDbDir, "persistIndexes.db")

			if _, err := os.Stat(persDB); !os.IsNotExist(err) {
				s.NoError(os.WriteFile(persDB, nil, DefaultFileMode))
			}

			db, err := NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d := db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 1)
			s.Contains(d.indexes, "_id")

			_, err = d.Insert(ctx, M{"planet": "Earth"})
			s.NoError(err)
			_, err = d.Insert(ctx, M{"planet": "Mars"})
			s.NoError(err)

			s.NoError(d.EnsureIndex(ctx, domain.WithFields("planet")))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())
		})

		// Indexes are persisted with their options and recreated even if some db operation happen between loads
		s.Run("PersistOperationBetweenLoads", func() {
			persDB := filepath.Join(s.testDbDir, "persistIndexes.db")

			if _, err := os.Stat(persDB); !os.IsNotExist(err) {
				s.NoError(os.WriteFile(persDB, nil, DefaultFileMode))
			}

			db, err := NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d := db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 1)
			s.Contains(d.indexes, "_id")

			_, err = d.Insert(ctx, M{"planet": "Earth"})
			s.NoError(err)
			_, err = d.Insert(ctx, M{"planet": "Mars"})
			s.NoError(err)

			s.NoError(d.EnsureIndex(ctx,
				domain.WithFields("planet"),
				domain.WithUnique(true),
			))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())
			s.True(d.indexes["planet"].Unique())
			s.False(d.indexes["planet"].Sparse())

			_, err = d.Insert(ctx, M{"planet": "Jupiter"})
			s.NoError(err)

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(3, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(3, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Equal(3, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(3, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())
			s.True(d.indexes["planet"].Unique())
			s.False(d.indexes["planet"].Sparse())

			s.NoError(d.EnsureIndex(ctx,
				domain.WithFields("bloup"),
				domain.WithSparse(true),
			))

			s.Len(d.indexes, 3)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Contains(d.indexes, "bloup")
			s.Equal(3, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(3, d.indexes["planet"].GetNumberOfKeys())
			s.Equal(0, d.indexes["bloup"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())
			s.Equal("bloup", d.indexes["bloup"].FieldName())
			s.True(d.indexes["planet"].Unique())
			s.False(d.indexes["planet"].Sparse())
			s.False(d.indexes["bloup"].Unique())
			s.True(d.indexes["bloup"].Sparse())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 3)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Contains(d.indexes, "bloup")
			s.Equal(3, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(3, d.indexes["planet"].GetNumberOfKeys())
			s.Equal(0, d.indexes["bloup"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())
			s.Equal("bloup", d.indexes["bloup"].FieldName())
			s.True(d.indexes["planet"].Unique())
			s.False(d.indexes["planet"].Sparse())
			s.False(d.indexes["bloup"].Unique())
			s.True(d.indexes["bloup"].Sparse())
		})

		// Indexes can also be removed and the remove persisted
		s.Run("PersistRemove", func() {
			persDB := filepath.Join(s.testDbDir, "persistIndexes.db")

			if _, err := os.Stat(persDB); !os.IsNotExist(err) {
				s.NoError(os.WriteFile(persDB, nil, DefaultFileMode))
			}

			db, err := NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d := db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 1)
			s.Contains(d.indexes, "_id")

			_, err = d.Insert(ctx, M{"planet": "Earth"})
			s.NoError(err)
			_, err = d.Insert(ctx, M{"planet": "Mars"})
			s.NoError(err)

			s.NoError(d.EnsureIndex(ctx, domain.WithFields("planet")))
			s.NoError(d.EnsureIndex(ctx, domain.WithFields("another")))

			s.Len(d.indexes, 3)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Contains(d.indexes, "another")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 3)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "planet")
			s.Contains(d.indexes, "another")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
			s.Equal(2, d.indexes["planet"].GetNumberOfKeys())
			s.Equal("planet", d.indexes["planet"].FieldName())

			s.NoError(d.RemoveIndex(ctx, "planet"))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "another")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "another")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())

			db, err = NewDatastore(WithFilename(persDB))
			s.NoError(err)
			d = db.(*Datastore)
			s.NoError(db.LoadDatabase(ctx))

			s.Len(d.indexes, 2)
			s.Contains(d.indexes, "_id")
			s.Contains(d.indexes, "another")
			s.Equal(2, d.indexes["_id"].GetNumberOfKeys())
		})

	}) // ==== End of 'Persisting indexes' ====

	// Results of getMatching should never contain duplicates
	s.Run("NoDuplicatesInGetMatching", func() {
		s.NoError(s.d.EnsureIndex(ctx, domain.WithFields("bad")))
		_ = s.insert(s.d.Insert(ctx, M{"bad": []any{"a", "b"}}))
		candidates, err := listCandidates(s.d.getCandidates(ctx, data.M{"$in": []any{"a", "b"}}, false))
		s.NoError(err)
		s.Len(candidates, 1)
	})

	s.Run("NoFieldname", func() {
		s.ErrorIs(s.d.EnsureIndex(ctx), domain.ErrNoFieldName)
	})

	s.Run("FailedIndexFactory", func() {
		errIdxFac := fmt.Errorf("index factory error")
		s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
			return nil, errIdxFac
		}

		err := s.d.EnsureIndex(
			ctx, domain.WithFields("a"),
		)
		s.ErrorIs(err, errIdxFac)
	})

	s.Run("FailedDocumentFactory", func() {
		errDocFac := fmt.Errorf("document factory error")
		s.d.documentFactory = func(any) (domain.Document, error) {
			return nil, errDocFac
		}

		err := s.d.EnsureIndex(
			ctx, domain.WithFields("a"),
		)
		s.ErrorIs(err, errDocFac)
	})

}

func (s *DatastoreTestSuite) TestDropDatabase() {
	s.Run("RemovesFile", func() {
		s.FileExists(s.testDb)
		ctx := context.Background()
		s.NoError(s.d.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
	})

	s.Run("ReloadAfterwards", func() {
		s.FileExists(s.testDb)
		ctx := context.Background()
		cur, err := s.d.Insert(ctx, M{"_id": uuid.New().String(), "hello": "world"})
		s.NoError(err)
		s.NotNil(cur)
		s.FileExists(s.testDb)
		b, err := os.ReadFile(s.testDb)
		s.NoError(err)
		var lines [][]byte
		for line := range bytes.SplitSeq(b, []byte("\n")) {
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		s.Len(lines, 1)
		s.NoError(s.d.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
		cur, err = s.d.Insert(ctx, M{"_id": uuid.New().String(), "hello": "world"})
		s.NoError(err)
		s.NotNil(cur)
		s.NoError(err)
		s.NotNil(cur)
		s.FileExists(s.testDb)
		b, err = os.ReadFile(s.testDb)
		s.NoError(err)
		lines = lines[:0]
		for line := range bytes.SplitSeq(b, []byte("\n")) {
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		s.Len(lines, 1)
	})

	s.Run("CanDropMultipleTimes", func() {
		s.FileExists(s.testDb)
		ctx := context.Background()
		s.NoError(s.d.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
		s.NoError(s.d.DropDatabase(ctx))
		s.NoFileExists(s.testDb)
	})

	s.Run("ResetBuffer", func() {
		ctx := context.Background()
		s.NoError(s.d.DropDatabase(ctx))
		docs := []any{
			M{"_id": uuid.New().String(), "hello": "world"},
			M{"_id": uuid.New().String(), "hello": "world"},
			M{"_id": uuid.New().String(), "hello": "world"},
		}
		cur, err := s.d.Insert(ctx, docs...)
		s.NoError(err)
		s.NotNil(cur)
		s.NoError(s.d.DropDatabase(ctx))
		cur, err = s.d.Insert(ctx, M{"_id": uuid.New().String(), "hi": "world"})
		s.NoError(err)
		s.NotNil(cur)
		err = s.d.LoadDatabase(ctx)
		s.NoError(err)
		c, err := s.d.Count(ctx, nil)
		s.NoError(err)
		s.Equal(int64(1), c)
	})

	s.Run("FailedIndexFactory", func() {
		errIdxFac := fmt.Errorf("index factory error")
		s.d.indexFactory = func(...domain.IndexOption) (domain.Index, error) {
			return nil, errIdxFac
		}

		err := s.d.DropDatabase(ctx)
		s.ErrorIs(err, errIdxFac)
	})
}

func (s *DatastoreTestSuite) TestLoadDatabaseCancelledContext() {
	db, err := NewDatastore(WithFilename(s.testDb))
	s.NoError(err)
	s.NotNil(db)

	open := (<-chan struct{})(make(chan struct{}))
	c := make(chan struct{})
	close(c)
	closed := (<-chan struct{})(c)

	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err3 := errors.New("error 3")
	err4 := errors.New("error 4")

	ctxMock := new(contextMock)

	// cannot lock mutex
	ctxMock.On("Done").Return(closed).Once()
	ctxMock.On("Err").Return(err1).Once()
	s.ErrorIs(db.LoadDatabase(ctxMock), err1)
	ctxMock.AssertExpectations(s.T())

	// cannot reset indexes
	ctxMock.On("Done").Return(open).Twice()
	ctxMock.On("Done").Return(closed).Once()
	ctxMock.On("Err").Return(err2).Once()
	s.ErrorIs(db.LoadDatabase(ctxMock), err2)
	ctxMock.AssertExpectations(s.T())

	// cannot load persisted data
	ctxMock.On("Done").Return(open).Times(4)
	ctxMock.On("Done").Return(closed).Once()
	ctxMock.On("Err").Return(err3).Once()
	s.ErrorIs(db.LoadDatabase(ctxMock), err3)
	ctxMock.AssertExpectations(s.T())

	// cannot reset index with loaded data
	ctxMock.On("Done").Return(open).Times(8)
	ctxMock.On("Done").Return(closed).Twice()
	ctxMock.On("Err").Return(err4).Twice()
	s.ErrorIs(db.LoadDatabase(ctxMock), err4)
	ctxMock.AssertExpectations(s.T())
}

func (s *DatastoreTestSuite) TestCancelContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	<-ctx.Done()

	err := s.d.CompactDatafile(ctx)
	s.ErrorIs(err, context.Canceled)

	count, err := s.d.Count(ctx, nil)
	s.ErrorIs(err, context.Canceled)
	s.Zero(count)

	err = s.d.DropDatabase(ctx)
	s.ErrorIs(err, context.Canceled)

	err = s.d.EnsureIndex(ctx)
	s.ErrorIs(err, context.Canceled)

	cur, err := s.d.Find(ctx, nil)
	s.ErrorIs(err, context.Canceled)
	s.Zero(cur)

	err = s.d.FindOne(ctx, nil, nil)
	s.ErrorIs(err, context.Canceled)

	cur, err = s.d.GetAllData(ctx)
	s.ErrorIs(err, context.Canceled)
	s.Zero(cur)

	cur, err = s.d.Insert(ctx)
	s.ErrorIs(err, context.Canceled)
	s.Zero(cur)

	count, err = s.d.Remove(ctx, nil)
	s.ErrorIs(err, context.Canceled)
	s.Zero(count)

	err = s.d.RemoveIndex(ctx)
	s.ErrorIs(err, context.Canceled)

	cur, err = s.d.Update(ctx, nil, nil)
	s.ErrorIs(err, context.Canceled)
	s.Zero(cur)
}

func (s *DatastoreTestSuite) EqualDocs(expected []M, actual []domain.Document) bool {
	if !s.Len(actual, len(expected)) {
		return false
	}
	success := true
	for i, m := range actual {
		if !s.EqualDoc(expected[i], m) {
			success = false
		}
	}
	return success
}

func (s *DatastoreTestSuite) EqualDoc(expected M, actual domain.Document) bool {
	doc, _ := data.NewDocument(expected)
	return s.Equal(doc, actual)
}

func (s *DatastoreTestSuite) insert(in domain.Cursor, err error) []M {
	if !s.NoError(err) {
		return nil
	}
	var res []M
	for in.Next() {
		var m M
		if !s.NoError(in.Scan(ctx, &m)) {
			return nil
		}
		res = append(res, m)
	}
	return res
}

func (s *DatastoreTestSuite) update(in domain.Cursor, err error) []domain.Document {
	if !s.NoError(err) {
		return nil
	}
	var res []domain.Document
	for in.Next() {
		var d any
		if !s.NoError(in.Scan(ctx, &d)) {
			return nil
		}
		m, err := data.NewDocument(d)
		s.NoError(err)
		res = append(res, m)
	}
	return res
}

func listMatching[T any](seq iter.Seq2[T, error], err error) ([]T, error) {
	res := make([]T, 0, 126)
	if err != nil {
		return nil, err
	}
	for i, err := range seq {
		if err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func listCandidates[T any](seq iter.Seq2[T, error], err error) ([]T, error) {
	res := make([]T, 0, 126)
	if err != nil {
		return nil, err
	}
	for i, err := range seq {
		if err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}
