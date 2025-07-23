package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vinicius-lino-figueiredo/bst"
	"github.com/vinicius-lino-figueiredo/gedb"
)

var ctx = context.Background()

type timeGetterMock struct{ mock.Mock }

func (t *timeGetterMock) GetTime() time.Time { return t.Called().Get(0).(time.Time) }

type DatastoreTestSuite struct {
	suite.Suite
	d *Datastore
}

func (s *DatastoreTestSuite) SetupTest() {
	d, err := NewDatastore(gedb.DatastoreOptions{Filename: testDb})
	s.NoError(err)
	s.d = d.(*Datastore)
	s.NoError(s.d.persistence.(*Persistence).ensureParentDirectoryExistsAsync(ctx, testDb, DefaultDirMode))
	if _, err = os.Stat(testDb); err != nil {
		if !os.IsNotExist(err) {
			s.FailNow(err.Error())
		}
	} else {
		s.NoError(os.Remove(testDb))
	}

	s.NoError(s.d.LoadDatabase(ctx))
	s.Len(s.d.getAllData(), 0)
}

func (s *DatastoreTestSuite) SetupSubTest() {
	s.SetupTest()
}

func TestDatastoreTestSuite(t *testing.T) {
	suite.Run(t, new(DatastoreTestSuite))
}

func (s *DatastoreTestSuite) readCursor(cur gedb.Cursor) ([]Document, error) {
	var res []Document
	ctx := context.Background()
	for cur.Next() {
		n := make(Document)
		if err := cur.Exec(ctx, &n); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *DatastoreTestSuite) TestAutoloading() {

	// Can autoload a database and query it right away
	s.Run("AutoloadAndQueryDatabase", func() {
		var fileBytes []byte
		_docs := []map[string]any{
			{"_id": "1", "a": 5, "planet": "Earth"},
			{"_id": "2", "a": 5, "planet": "Mars"},
		}
		for _, _doc := range _docs {
			b, err := json.Marshal(_doc)
			s.NoError(err)
			fileBytes = append(fileBytes, b...)
			fileBytes = append(fileBytes, byte('\n'))
		}

		const autoDb = "../workspace/auto.db"

		s.NoError(os.WriteFile(autoDb, fileBytes, 0666))
		db, err := LoadDatastore(ctx, gedb.DatastoreOptions{Filename: autoDb})
		s.NoError(err)

		cur, err := db.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.Len(docs, 2)
	})

	// Throws if autoload fails
	s.Run("Throws if autoload fails", func() {
		var fileBytes []byte
		_docs := []map[string]any{
			{"_id": "1", "a": 5, "planet": "Earth"},
			{"_id": "2", "a": 5, "planet": "Mars"},
			{"$$indexCreated": map[string]any{"fieldName": "a", "unique": true}},
		}
		for _, _doc := range _docs {
			b, err := json.Marshal(_doc)
			s.NoError(err)
			fileBytes = append(fileBytes, b...)
			fileBytes = append(fileBytes, byte('\n'))
		}

		const autoDb = "../workspace/auto.db"

		s.NoError(os.WriteFile(autoDb, fileBytes, 0666))

		db, err := LoadDatastore(ctx, gedb.DatastoreOptions{Filename: autoDb})
		e := &bst.ErrViolated{}
		s.ErrorAs(err, &e)
		s.Nil(db)
	})

}

func (s *DatastoreTestSuite) TestInsert() {

	// Able to insert a document in the database, setting an _id if none provided, and retrieve it even after a reload
	s.Run("InsertDocAndSetIDIfNotProvidedAndRetrieveAfterReload", func() {
		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		_, err = s.d.Insert(ctx, map[string]any{"somedata": "ok"})
		s.NoError(err)

		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal("ok", docs[0].Get("somedata"))
		s.Contains(docs[0], "_id")

		s.NoError(s.d.LoadDatabase(ctx))
		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal("ok", docs[0].Get("somedata"))
		s.Contains(docs[0], "_id")
	})

	// Can insert multiple documents in the database
	s.Run("InsertMultipleDocs", func() {
		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		_, err = s.d.Insert(ctx, map[string]any{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, map[string]any{"somedata": "another"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, map[string]any{"somedata": "again"})
		s.NoError(err)
		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
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

		_, err := s.d.Insert(ctx, obj)
		s.NoError(err)

		var res Document
		s.NoError(s.d.FindOne(ctx, nil, &res, gedb.FindOptions{}))

		s.Len(res["a"], 3)
		s.Equal("ee", res["a"].([]any)[0])
		s.Equal("ff", res["a"].([]any)[1])
		s.Equal(42, res["a"].([]any)[2])
		s.Equal(da, res["date"])
		s.Equal("b", res["subobj"].(Document)["a"])
		s.Equal("c", res["subobj"].(Document)["b"])

	})

	// If an object returned from the DB is modified and refetched, the original value should be found
	s.Run("CannotModifyFetched", func() {
		s.d.insert(ctx, Document{"a": "something"})

		doc := make(Document)
		s.NoError(s.d.FindOne(ctx, nil, &doc, gedb.FindOptions{}))
		s.Equal("something", doc.Get("a"))
		doc.Set("a", "another thing")
		s.Equal("another thing", doc.Get("a"))

		doc2 := make(Document)
		s.NoError(s.d.FindOne(ctx, nil, &doc2, gedb.FindOptions{}))
		s.Equal("something", doc2.Get("a"))
		doc2.Set("a", "another thing")
		s.Equal("another thing", doc2.Get("a"))

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal("something", docs[0].Get("a"))
	})

	// Cannot insert a doc that has a field beginning with a $ sign
	s.Run("CannotInsertFieldWithDollarPrefix", func() {
		_, err := s.d.Insert(ctx, Document{"$something": "atest"})
		s.Error(err)
	})

	// If an _id is already given when we insert a document, use that instead of generating a random one
	s.Run("UseCustomIDIfProvided", func() {
		newDoc, err := s.d.Insert(ctx, Document{"_id": "test", "stuff": true})
		s.NoError(err)
		s.Equal(true, newDoc[0].Get("stuff"))

		s.Equal("test", newDoc[0].ID())

		_, err = s.d.Insert(ctx, Document{"_id": "test", "otherstuff": 42})
		e := &bst.ErrViolated{}
		s.ErrorAs(err, &e)
	})

	// Modifying the insertedDoc after an insert doesnt change the copy saved in the database
	s.Run("InsertReturnsUnmodifiableDocs", func() {
		newDoc, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)
		newDoc[0].Set("hello", "changed")

		doc := make(Document)
		s.NoError(s.d.FindOne(ctx, Document{"a": 2}, &doc, gedb.FindOptions{}))
		s.Equal("world", doc["hello"])
	})

	// Can insert an array of documents at once
	s.Run("InsertMultipleDocsAtOnce", func() {
		_docs := []any{Document{"a": 5, "b": "hello"}, Document{"a": 42, "b": "world"}}

		_, err := s.d.Insert(ctx, _docs...)
		s.NoError(err)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 2)
		docMaps := make(map[int]Document)
		for _, value := range docs {
			docMaps[value["a"].(int)] = value
		}
		s.Equal("hello", docMaps[5]["b"])
		s.Equal("world", docMaps[42]["b"])

		b, err := os.ReadFile(testDb)
		s.NoError(err)
		lines := bytes.Split(b, []byte("\n"))
		data := make([]map[string]any, 0, len(lines))
		for _, line := range lines {
			if len(line) > 0 {
				var d map[string]any
				s.NoError(json.Unmarshal(line, &d))
				data = append(data, d)
			}
		}

		s.Len(data, 2)
		s.Equal(5.0, data[0]["a"])
		s.Equal("hello", data[0]["b"])
		s.Equal(42.0, data[1]["a"])
		s.Equal("world", data[1]["b"])
	})

	// If a bulk insert violates a constraint, all changes are rolled back
	s.Run("RollbackAllIfAnyViolatesConstraint", func() {
		_docs := []any{
			Document{"a": 5, "b": "hello"},
			Document{"a": 42, "b": "world"},
			Document{"a": 5, "b": "bloup"},
			Document{"a": 7},
		}

		s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{
			FieldNames: []string{"a"},
			Unique:     true,
		})

		_, err := s.d.Insert(ctx, _docs...)
		e := &bst.ErrViolated{}
		s.ErrorAs(err, &e)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		b, err := os.ReadFile(testDb)
		s.NoError(err)
		lines := bytes.Split(b, []byte("\n"))
		data := make([]any, 0, len(lines))
		for _, line := range lines {
			if len(line) > 0 {
				var d map[string]any
				s.NoError(json.Unmarshal(line, &d))
				data = append(data, d)
			}
		}

		s.Equal([]any{map[string]any{"$$indexCreated": map[string]any{"fieldName": "a", "unique": true}}}, data)
		length := 0
		for cur.Next() {
			length++
		}
		s.NoError(cur.Err())
	})

	// If timestampData option is set, a createdAt field is added and persisted
	s.Run("TimestampDataAddsCreatedAtField", func() {
		newDoc := Document{"hello": "world"}

		// precision bellow milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)

		insertedDocs, err := d.Insert(ctx, newDoc)
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]

		s.Equal(Document{"hello": "world"}, newDoc)
		s.Equal("world", insertedDoc.Get("hello"))
		s.Contains(insertedDoc, "createdAt")
		s.Contains(insertedDoc, "updatedAt")
		s.Equal(insertedDoc.Get("createdAt"), insertedDoc.Get("updatedAt"))
		s.Contains(insertedDoc, "_id")
		s.Len(insertedDoc, 4)
		s.Equal(beginning, insertedDoc.Get("createdAt"))

		insertedDoc.Set("bloup", "another")
		s.Len(insertedDoc, 5)

		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(Document{
			"hello":     "world",
			"_id":       insertedDoc.Get("_id"),
			"createdAt": insertedDoc.Get("createdAt"),
			"updatedAt": insertedDoc.Get("updatedAt"),
		}, docs[0])

		s.NoError(d.LoadDatabase(ctx))

		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		s.Equal(Document{
			"hello":     "world",
			"_id":       insertedDoc.Get("_id"),
			"createdAt": insertedDoc.Get("createdAt"),
			"updatedAt": insertedDoc.Get("updatedAt"),
		}, docs[0])
	})

	// If timestampData option not set, don't create a createdAt and a updatedAt field
	s.Run("IfNotTimestampDataCreatedAtNotAdded", func() {
		insertedDocs, err := s.d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]

		s.Len(insertedDoc, 2)
		s.NotContains(insertedDoc, "createdAt")
		s.NotContains(insertedDoc, "updatedAt")

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)

		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Equal(docs[0], insertedDoc)

	})

	// If timestampData is set but createdAt is specified by user, don't change it
	s.Run("ShouldNotChangeProvidedCreatedAtField", func() {
		newDoc := Document{"hello": "world", "createdAt": time.UnixMilli(234)}

		// precision bellow milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		insertedDocs, err := d.Insert(ctx, newDoc)
		s.NoError(err)
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]
		s.Len(insertedDoc, 4)
		s.Equal(time.UnixMilli(234), insertedDoc.Get("createdAt"))
		s.Equal(beginning, insertedDoc.Get("updatedAt"))

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])

		s.NoError(d.LoadDatabase(ctx))
		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])
	})

	// If timestampData is set but updatedAt is specified by user, don't change it
	s.Run("ShouldNotChangeProvidedCreatedAtField", func() {
		newDoc := Document{"hello": "world", "updatedAt": time.UnixMilli(234)}

		// precision bellow milliseconds and comparison would fail
		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		timeGetter.On("GetTime").Return(beginning)

		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		insertedDocs, err := d.Insert(ctx, newDoc)
		s.NoError(err)
		s.Len(insertedDocs, 1)
		insertedDoc := insertedDocs[0]
		s.Len(insertedDoc, 4)
		s.Equal(time.UnixMilli(234), insertedDoc.Get("updatedAt"))
		s.Equal(beginning, insertedDoc.Get("createdAt"))

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])

		s.NoError(d.LoadDatabase(ctx))
		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Equal(insertedDoc, docs[0])
	})

	// Can insert a doc with id 0
	s.Run("InsertNumberZeroAsID", func() {
		// NOTE: This test passes, but persisted data would now work
		// properly because the package expects _id to always be string.
		s.T().Skip()
		doc, err := s.d.Insert(ctx, Document{"_id": 0, "hello": "world"})
		s.NoError(err)
		s.Equal(0, doc[0].Get("_id"))
		s.Equal("world", doc[0].Get("hello"))
	})

} // ==== End of 'Insert' ==== //

func (s *DatastoreTestSuite) TestGetCandidates() {
	// Can use an index to get docs with a basic match
	s.Run("BasicMatch", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"tf"}}))
		_doc1, err := s.d.Insert(ctx, Document{"tf": 4})
		s.NoError(err)
		s.Len(_doc1, 1)
		_, err = s.d.Insert(ctx, Document{"tf": 6})
		s.NoError(err)
		_doc2, err := s.d.Insert(ctx, Document{"tf": 4, "an": "other"})
		s.Len(_doc2, 1)
		_, err = s.d.Insert(ctx, Document{"tf": 9})
		s.NoError(err)
		data, err := s.d.getCandidates(ctx, Document{"r": 6, "tf": 4}, false)
		s.NoError(err)
		s.Len(data, 2)
		doc1ID := slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc1[0].ID() })
		s.GreaterOrEqual(doc1ID, 0)
		doc2ID := slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc2[0].ID() })
		s.GreaterOrEqual(doc2ID, 0)

		doc1 := data[doc1ID]
		doc2 := data[doc2ID]

		s.Equal(Document{"_id": doc1.ID(), "tf": 4}, doc1)
		s.Equal(Document{"_id": doc2.ID(), "tf": 4, "an": "other"}, doc2)
	})

	// Can use a compound index to get docs with a basic match
	s.Run("BasicMatchCompoundIndex", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"tf", "tg"}}))

		_, err := s.d.Insert(ctx, Document{"tf": 4, "tg": 0, "foo": 1})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"tf": 6, "tg": 0, "foo": 2})
		s.NoError(err)
		_doc1, err := s.d.Insert(ctx, Document{"tf": 4, "tg": 1, "foo": 3})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"tf": 6, "tg": 1, "foo": 4})
		data, err := s.d.getCandidates(ctx, Document{"tf": 4, "tg": 1}, false)
		s.NoError(err)
		s.Len(data, 1)
		doc1 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc1[0].ID() })]
		s.Equal(Document{"_id": doc1.ID(), "tf": 4, "tg": 1, "foo": 3}, doc1)
	})

	// Can use an index to get docs with a $in match
	s.Run("Match$inOperator", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"tf"}}))

		_, err := s.d.Insert(ctx, Document{"tf": 4})
		s.NoError(err)
		_doc1, err := s.d.Insert(ctx, Document{"tf": 6})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"tf": 4, "an": "other"})
		s.NoError(err)
		_doc2, err := s.d.Insert(ctx, Document{"tf": 9})
		s.NoError(err)

		data, err := s.d.getCandidates(ctx, Document{"r": 6, "tf": Document{"$in": []any{6, 9, 5}}}, false)
		s.NoError(err)

		doc1 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc1[0].ID() })]
		doc2 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc2[0].ID() })]

		s.Len(data, 2)

		s.Equal(Document{"_id": doc1.ID(), "tf": 6}, doc1)
		s.Equal(Document{"_id": doc2.ID(), "tf": 9}, doc2)
	})

	// If no index can be used, return the whole database
	s.Run("ReturnDatabaseIfNoUsabeIndex", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"tf"}}))

		_doc1, err := s.d.Insert(ctx, Document{"tf": 4})
		s.NoError(err)
		_doc2, err := s.d.Insert(ctx, Document{"tf": 6})
		s.NoError(err)
		_doc3, err := s.d.Insert(ctx, Document{"tf": 4, "an": "other"})
		s.NoError(err)
		_doc4, err := s.d.Insert(ctx, Document{"tf": 9})
		s.NoError(err)

		data, err := s.d.getCandidates(ctx, Document{"r": 6, "notf": Document{"$in": []any{6, 9, 5}}}, false)
		s.NoError(err)

		doc1 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc1[0].ID() })]
		doc2 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc2[0].ID() })]
		doc3 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc3[0].ID() })]
		doc4 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc4[0].ID() })]

		s.Equal(Document{"_id": doc1.ID(), "tf": 4}, doc1)
		s.Equal(Document{"_id": doc2.ID(), "tf": 6}, doc2)
		s.Equal(Document{"_id": doc3.ID(), "tf": 4, "an": "other"}, doc3)
		s.Equal(Document{"_id": doc4.ID(), "tf": 9}, doc4)
	})

	// Can use indexes for comparison matches
	s.Run("ComparisonMatch", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"tf"}}))

		_, err := s.d.Insert(ctx, Document{"tf": 4})
		s.NoError(err)
		_doc2, err := s.d.Insert(ctx, Document{"tf": 6})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"tf": 4, "an": "other"})
		s.NoError(err)
		_doc4, err := s.d.Insert(ctx, Document{"tf": 9})
		s.NoError(err)

		data, err := s.d.getCandidates(ctx, Document{"r": 6, "tf": Document{"$lte": 9, "$gte": 6}}, false)
		s.NoError(err)

		doc2 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc2[0].ID() })]
		doc4 := data[slices.IndexFunc(data, func(d gedb.Document) bool { return d.ID() == _doc4[0].ID() })]

		s.Len(data, 2)

		s.Equal(Document{"_id": doc2.ID(), "tf": 6}, doc2)
		s.Equal(Document{"_id": doc4.ID(), "tf": 9}, doc4)
	})

	// Can set a TTL index that expires documents
	s.Run("TLLIndex", func() {
		now := time.Now()
		timeGetter := new(timeGetterMock)
		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		s.NoError(d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"exp"}, ExpireAfter: 200 * time.Millisecond}))

		// will be called on insert and on find
		timeGetter.On("GetTime").Return(now.Add(300 * time.Millisecond))

		_, err = d.Insert(ctx, Document{"hello": "world", "exp": now})
		s.NoError(err)

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 0)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.NoError(d.WaitCompaction(ctx))
			b, err := os.ReadFile(testDb)
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
		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		s.NoError(d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"exp"}, ExpireAfter: 200 * time.Millisecond}))

		// will be called on insert and on find
		firstTimestamp := timeGetter.On("GetTime").Return(now).Times(4)

		_, err = d.Insert(ctx, Document{"hello": "world1", "exp": now})
		s.NoError(err)
		_, err = d.Insert(ctx, Document{"hello": "world2", "exp": now.Add(50 * time.Millisecond)})
		s.NoError(err)
		_, err = d.Insert(ctx, Document{"hello": "world3", "exp": now.Add(100 * time.Millisecond)})
		s.NoError(err)

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 3)

		firstTimestamp.Unset()

		// after first doc (200ms) and second doc (250ms) + 1ms
		secondTimestamp := timeGetter.On("GetTime").Return(now.Add(251 * time.Millisecond))

		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)

		secondTimestamp.Unset()

		// after third doc (300ms) + 1ms
		timeGetter.On("GetTime").Return(now.Add(301 * time.Millisecond))

		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Document where indexed field is absent or not a date are ignored
	s.Run("IgnoreIfFieldIsNotAValidDate", func() {
		now := time.Now()
		timeGetter := new(timeGetterMock)
		options := gedb.DatastoreOptions{
			Filename:      testDb,
			TimestampData: true,
			Autoload:      true,
			TimeGetter:    timeGetter,
		}

		d, err := LoadDatastore(ctx, options)
		s.NoError(err)

		s.NoError(d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"exp"}, ExpireAfter: 200 * time.Millisecond}))

		// will be called on insert and on find
		firstTimestamp := timeGetter.On("GetTime").Return(now).Times(4)

		_, err = d.Insert(ctx, Document{"hello": "world1", "exp": now})
		s.NoError(err)
		_, err = d.Insert(ctx, Document{"hello": "world2", "exp": "not a date"})
		s.NoError(err)
		_, err = d.Insert(ctx, Document{"hello": "world3"})
		s.NoError(err)

		cur, err := d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 3)

		firstTimestamp.Unset()

		timeGetter.On("GetTime").Return(now.Add(301 * time.Millisecond))

		cur, err = d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

	})
} // ==== End of 'GetCandidates' ==== //
