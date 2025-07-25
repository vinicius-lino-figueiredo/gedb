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
	s.NoError(s.d.persistence.(*Persistence).ensureParentDirectoryExists(ctx, testDb, DefaultDirMode))
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

func (s *DatastoreTestSuite) TestFind() {

	// Can find all documents if an empty query is used
	s.Run("FindAllDocumentsWithEmptyQuery", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "another", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 3)
		somedataValues := make([]any, len(docs))
		for i, doc := range docs {
			somedataValues[i] = doc.Get("somedata")
		}
		s.Contains(somedataValues, "ok")
		s.Contains(somedataValues, "another")
		s.Contains(somedataValues, "again")

		var docWithPlus Document
		for _, doc := range docs {
			if doc.Get("somedata") == "another" {
				docWithPlus = doc
				break
			}
		}
		s.Equal("additional data", docWithPlus.Get("plus"))
	})

	// Can find all documents matching a basic query
	s.Run("FindDocumentsMatchingBasicQuery", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)

		cur, err := s.d.Find(ctx, Document{"somedata": "again"}, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		somedataValues := make([]any, len(docs))
		for i, doc := range docs {
			somedataValues[i] = doc.Get("somedata")
		}
		s.NotContains(somedataValues, "ok")

		// Test with query that doesn't match anything
		cur, err = s.d.Find(ctx, Document{"somedata": "nope"}, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Can find one document matching a basic query and return null if none is found
	s.Run("FindOneDocumentOrReturnNil", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"somedata": "ok"}, &doc, gedb.FindOptions{})
		s.NoError(err)
		s.Len(doc, 2)
		s.Equal("ok", doc.Get("somedata"))
		s.Contains(doc, "_id")

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"somedata": "nope"}, &doc2, gedb.FindOptions{})
		s.Error(err) // Go implementation returns error instead of nil
	})

	// Can find dates and objects (non JS-native types)
	s.Run("FindDatesAndObjects", func() {
		date1 := time.UnixMilli(1234543)
		date2 := time.UnixMilli(9999)

		_, err := s.d.Insert(ctx, Document{"now": date1, "sth": Document{"name": "gedb"}})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"now": date1}, &doc, gedb.FindOptions{})
		s.NoError(err)
		s.Equal("gedb", doc.Get("sth").(Document).Get("name"))

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"now": date2}, &doc2, gedb.FindOptions{})
		s.Error(err) // No match

		doc3 := make(Document)
		err = s.d.FindOne(ctx, Document{"sth": Document{"name": "gedb"}}, &doc3, gedb.FindOptions{})
		s.NoError(err)
		s.Equal("gedb", doc3.Get("sth").(Document).Get("name"))

		doc4 := make(Document)
		err = s.d.FindOne(ctx, Document{"sth": Document{"name": "other"}}, &doc4, gedb.FindOptions{})
		s.Error(err) // No match
	})

	// Can use dot-notation to query subfields
	s.Run("DotNotationSubfields", func() {
		_, err := s.d.Insert(ctx, Document{"greeting": Document{"english": "hello"}})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"greeting.english": "hello"}, &doc, gedb.FindOptions{})
		s.NoError(err)
		s.Equal("hello", doc.Get("greeting").(Document).Get("english"))

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"greeting.english": "hellooo"}, &doc2, gedb.FindOptions{})
		s.Error(err) // No match

		doc3 := make(Document)
		err = s.d.FindOne(ctx, Document{"greeting.englis": "hello"}, &doc3, gedb.FindOptions{})
		s.Error(err) // No match
	})

	// Array fields match if any element matches
	s.Run("ArrayFieldsMatchAnyElement", func() {
		doc1, err := s.d.Insert(ctx, Document{"fruits": []any{"pear", "apple", "banana"}})
		s.NoError(err)
		doc2, err := s.d.Insert(ctx, Document{"fruits": []any{"coconut", "orange", "pear"}})
		s.NoError(err)
		doc3, err := s.d.Insert(ctx, Document{"fruits": []any{"banana"}})
		s.NoError(err)

		cur, err := s.d.Find(ctx, Document{"fruits": "pear"}, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		ids := make([]any, len(docs))
		for i, doc := range docs {
			ids[i] = doc.ID()
		}
		s.Contains(ids, doc1[0].ID())
		s.Contains(ids, doc2[0].ID())

		cur, err = s.d.Find(ctx, Document{"fruits": "banana"}, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)

		ids = make([]any, len(docs))
		for i, doc := range docs {
			ids[i] = doc.ID()
		}
		s.Contains(ids, doc1[0].ID())
		s.Contains(ids, doc3[0].ID())

		cur, err = s.d.Find(ctx, Document{"fruits": "doesntexist"}, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 0)
	})

	// Returns an error if the query is not well formed
	s.Run("ErrorOnMalformedQuery", func() {
		_, err := s.d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)

		cur, err := s.d.Find(ctx, Document{"$or": Document{"hello": "world"}}, gedb.FindOptions{})
		s.Error(err)
		s.Nil(cur)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"$or": Document{"hello": "world"}}, &doc, gedb.FindOptions{})
		s.Error(err)
	})

	// Changing the documents returned by find or findOne do not change the database state
	s.Run("ReturnedDocsDoNotChangeDatabase", func() {
		_, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"a": 2}, &doc, gedb.FindOptions{})
		s.NoError(err)
		doc.Set("hello", "changed")

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": 2}, &doc2, gedb.FindOptions{})
		s.NoError(err)
		s.Equal("world", doc2.Get("hello"))

		cur, err := s.d.Find(ctx, Document{"a": 2}, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		docs[0].Set("hello", "changed")

		doc3 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": 2}, &doc3, gedb.FindOptions{})
		s.NoError(err)
		s.Equal("world", doc3.Get("hello"))
	})

	// Can use sort, skip and limit with FindOptions
	s.Run("SortSkipLimitWithFindOptions", func() {
		_, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 24, "hello": "earth"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 13, "hello": "blueplanet"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 15, "hello": "home"})
		s.NoError(err)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{
			Sort:  Document{"a": 1},
			Limit: 2,
		})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 2)
		s.Equal("world", docs[0].Get("hello"))
		s.Equal("blueplanet", docs[1].Get("hello"))
	})

	// Can use sort and skip with FindOne
	s.Run("SortSkipWithFindOne", func() {
		_, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 24, "hello": "earth"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 13, "hello": "blueplanet"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 15, "hello": "home"})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, nil, &doc, gedb.FindOptions{Sort: Document{"a": 1}})
		s.NoError(err)
		s.Equal("world", doc.Get("hello"))

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": Document{"$gt": 14}}, &doc2, gedb.FindOptions{Sort: Document{"a": 1}})
		s.NoError(err)
		s.Equal("home", doc2.Get("hello"))

		doc3 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": Document{"$gt": 14}}, &doc3, gedb.FindOptions{
			Sort: Document{"a": 1},
			Skip: 1,
		})
		s.NoError(err)
		s.Equal("earth", doc3.Get("hello"))

		doc4 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": Document{"$gt": 14}}, &doc4, gedb.FindOptions{
			Sort: Document{"a": 1},
			Skip: 2,
		})
		s.Error(err) // No documents found
	})

	// Can use projections in find
	s.Run("ProjectionsInFind", func() {
		_, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 24, "hello": "earth"})
		s.NoError(err)

		cur, err := s.d.Find(ctx, Document{"a": 2}, gedb.FindOptions{
			Projection: Document{"a": 0, "_id": 0},
		})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Equal(Document{"hello": "world"}, docs[0])

		cur, err = s.d.Find(ctx, Document{"a": 2}, gedb.FindOptions{
			Projection: Document{"a": 0, "hello": 1},
		})
		s.Error(err)
		s.Nil(cur)
	})

	// Can use projections in findOne
	s.Run("ProjectionsInFindOne", func() {
		_, err := s.d.Insert(ctx, Document{"a": 2, "hello": "world"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"a": 24, "hello": "earth"})
		s.NoError(err)

		doc := make(Document)
		err = s.d.FindOne(ctx, Document{"a": 2}, &doc, gedb.FindOptions{
			Projection: Document{"a": 0, "_id": 0},
		})
		s.NoError(err)
		s.Equal(Document{"hello": "world"}, doc)

		doc2 := make(Document)
		err = s.d.FindOne(ctx, Document{"a": 2}, &doc2, gedb.FindOptions{
			Projection: Document{"a": 0, "hello": 1},
		})
		s.Error(err)
	})

} // ==== End of 'Find' ==== //

func (s *DatastoreTestSuite) TestCount() {
	// Count all documents if an empty query is used
	s.Run("NoQuery", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "another", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)
		docs, err := s.d.Count(ctx, nil)
		s.NoError(err)
		s.Equal(int64(3), docs)
	})

	// Count all documents matching a basic query
	s.Run("BasicQuery", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)
		docs, err := s.d.Count(ctx, Document{"somedata": "again"})
		s.NoError(err)
		s.Equal(int64(2), docs)
		docs, err = s.d.Count(ctx, Document{"somedata": "nope"})
		s.NoError(err)
		s.Equal(int64(0), docs)
	})

	// Array fields match if any element matches
	s.Run("ArrayFields", func() {
		_, err := s.d.Insert(ctx, Document{"fruits": []any{"pear", "apple", "banana"}})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"fruits": []any{"coconut", "orange", "pear"}})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"fruits": []any{"banana"}})
		s.NoError(err)

		docs, err := s.d.Count(ctx, Document{"fruits": "pear"})
		s.NoError(err)
		s.Equal(int64(2), docs)

		docs, err = s.d.Count(ctx, Document{"fruits": "banana"})
		s.NoError(err)
		s.Equal(int64(2), docs)

		docs, err = s.d.Count(ctx, Document{"fruits": "doesntexist"})
		s.NoError(err)
		s.Equal(int64(0), docs)
	})

	// Returns an error if the query is not well formed
	s.Run("BadQuery", func() {
		_, err := s.d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)
		_, err = s.d.Count(ctx, Document{"$or": Document{"hello": "world"}})
		s.Error(err)
	})

} // ==== End of 'Count' ==== //

func (s *DatastoreTestSuite) TestUpdate() {

	// If the query doesn't match anything, database is not modified
	s.Run("NoChangeIfNoMatch", func() {
		_, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		_, err = s.d.Insert(ctx, Document{"somedata": "another"})
		s.NoError(err)

		n, err := s.d.Update(ctx, Document{"somedata": "nope"}, Document{"newDoc": "yes"}, gedb.UpdateOptions{Multi: true})
		s.NoError(err)
		s.Len(n, 0)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		doc1 := docs[slices.IndexFunc(docs, func(d Document) bool { return d["somedata"] == "ok" })]
		doc2 := docs[slices.IndexFunc(docs, func(d Document) bool { return d["somedata"] == "again" })]
		doc3 := docs[slices.IndexFunc(docs, func(d Document) bool { return d["somedata"] == "another" })]

		s.Len(docs, 3)
		for _, doc := range docs {
			s.NotContains(doc, "newDoc")
		}

		s.Equal(Document{"_id": doc1["_id"], "somedata": "ok"}, doc1)
		s.Equal(Document{"_id": doc2["_id"], "somedata": "again", "plus": "additional data"}, doc2)
		s.Equal(Document{"_id": doc3["_id"], "somedata": "another"}, doc3)
	})

	// If timestampData option is set, update the updatedAt field
	s.Run("PatchUpdatedAtField", func() {

		beginning := time.Now().Truncate(time.Millisecond)

		timeGetter := new(timeGetterMock)
		call := timeGetter.On("GetTime").Return(beginning)

		d, err := NewDatastore(gedb.DatastoreOptions{Filename: testDb, TimestampData: true, TimeGetter: timeGetter})
		s.NoError(err)
		insertedDocs, err := d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)

		call.Unset()

		s.Equal(beginning, insertedDocs[0].Get("updatedAt"))
		s.Equal(beginning, insertedDocs[0].Get("createdAt"))
		s.Len(insertedDocs[0], 4)

		call = timeGetter.On("GetTime").Return(beginning.Add(time.Millisecond))
		n, err := d.Update(ctx, Document{"_id": insertedDocs[0].ID()}, Document{"$set": Document{"hello": "mars"}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		cur, err := d.Find(ctx, Document{"_id": insertedDocs[0].ID()}, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 4)
		s.Equal(insertedDocs[0].ID(), docs[0].ID())
		s.Equal(insertedDocs[0].Get("createdAt"), docs[0].Get("createdAt"))
		s.Equal(beginning.Add(time.Millisecond), docs[0].Get("updatedAt"))
		s.Equal("mars", docs[0].Get("hello"))

	})

	// Can update multiple documents matching the query
	s.Run("MultipleMatches", func() {

		_doc1, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		id1 := _doc1[0].ID()

		_doc2, err := s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		id2 := _doc2[0].ID()

		_doc3, err := s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)
		id3 := _doc3[0].ID()

		n, err := s.d.Update(ctx, Document{"somedata": "again"}, Document{"newDoc": "yes"}, gedb.UpdateOptions{Multi: true})
		s.NoError(err)
		s.Len(n, 2)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		doc1 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id1 })]
		doc2 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id2 })]
		doc3 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id3 })]

		s.Len(docs, 3)

		s.Len(doc1, 2)
		s.Equal("ok", doc1.Get("somedata"))
		// removed redundant _id assertion

		s.Len(doc2, 2)
		s.Equal("yes", doc2.Get("newDoc"))
		// removed redundant _id assertion

		s.Len(doc3, 2)
		s.Equal("yes", doc3.Get("newDoc"))
		// removed redundant _id assertion

	})

	// Can update only one document matching the query
	s.Run("MultiDisabled", func() {
		_doc1, err := s.d.Insert(ctx, Document{"somedata": "ok"})
		s.NoError(err)
		id1 := _doc1[0].ID()
		_doc2, err := s.d.Insert(ctx, Document{"somedata": "again", "plus": "additional data"})
		s.NoError(err)
		id2 := _doc2[0].ID()
		_doc3, err := s.d.Insert(ctx, Document{"somedata": "again"})
		s.NoError(err)
		id3 := _doc3[0].ID()

		n, err := s.d.Update(ctx, Document{"somedata": "again"}, Document{"newDoc": "yes"}, gedb.UpdateOptions{Multi: false})
		s.NoError(err)
		s.Len(n, 1)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		doc1 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id1 })]
		doc2 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id2 })]
		doc3 := docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id3 })]

		s.Equal(Document{"_id": doc1.ID(), "somedata": "ok"}, doc1)
		if len(doc2) == 2 {
			s.Equal(Document{"_id": doc2.ID(), "newDoc": "yes"}, doc2)
			s.Equal(Document{"_id": doc3.ID(), "somedata": "again"}, doc3)
		} else {
			s.Equal(Document{"_id": doc2.ID(), "somedata": "again", "plus": "additional data"}, doc2)
			s.Equal(Document{"_id": doc3.ID(), "newDoc": "yes"}, doc3)
		}

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)

		doc1 = docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id1 })]
		doc2 = docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id2 })]
		doc3 = docs[slices.IndexFunc(docs, func(d Document) bool { return d.ID() == id3 })]

		s.Equal(Document{"_id": doc1.ID(), "somedata": "ok"}, doc1)
		if len(doc2) == 2 {
			s.Equal(Document{"_id": doc2.ID(), "newDoc": "yes"}, doc2)
			s.Equal(Document{"_id": doc3.ID(), "somedata": "again"}, doc3)
		} else {
			s.Equal(Document{"_id": doc2.ID(), "somedata": "again", "plus": "additional data"}, doc2)
			s.Equal(Document{"_id": doc3.ID(), "newDoc": "yes"}, doc3)
		}
	})

	s.Run("Upsert", func() {

		// Can perform upserts if needed
		s.Run("Simple", func() {
			n, err := s.d.Update(ctx, Document{"impossible": "db is empty anyway"}, Document{"newDoc": true}, gedb.UpdateOptions{})
			s.NoError(err)
			s.Len(n, 0)

			cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 0)

			n, err = s.d.Update(ctx, Document{"impossible": "db is empty anyway"}, Document{"something": "created ok"}, gedb.UpdateOptions{Upsert: true})
			s.NoError(err)
			s.Len(n, 1)

			cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
			s.NoError(err)
			docs, err = s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 1)
			s.Equal("created ok", docs[0].Get("something"))

			// original test would check if returned updated
			// documents could modify the actual values in index,
			// but update here does not return documents.
		})

		// If the update query is a normal object with no modifiers, it is the doc that will be upserted
		s.Run("UseQueryIfNoDollarFields", func() {
			qry := Document{"$or": []any{Document{"a": 4}, Document{"a": 5}}}
			update := Document{"hello": "world", "bloup": "blap"}
			n, err := s.d.Update(ctx, qry, update, gedb.UpdateOptions{Upsert: true})
			s.NoError(err)
			s.Len(n, 1)
			cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)
			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 3)
			s.Equal("world", doc.Get("hello"))
			s.Equal("blap", doc.Get("bloup"))
		})

		// If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 1
		s.Run("UseNonOperatorsFromFindQuery", func() {
			qry := Document{"$or": []any{Document{"a": 4}, Document{"a": 5}}}
			update := Document{
				"$set": Document{"hello": "world"},
				"$inc": Document{"bloup": 3},
			}
			n, err := s.d.Update(ctx, qry, update, gedb.UpdateOptions{Upsert: true})
			s.NoError(err)
			s.Len(n, 1)

			cur, err := s.d.Find(ctx, Document{"hello": "world"}, gedb.FindOptions{})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 3)
			s.Equal("world", doc.Get("hello"))
			s.Equal(float64(3), doc.Get("bloup"))
		})

		// If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 2
		s.Run("UseNonOperatorsFromFindQuery", func() {
			qry := Document{
				"$or": []any{Document{"a": 4}, Document{"a": 5}},
				"cac": "rrr",
			}
			update := Document{
				"$set": Document{"hello": "world"},
				"$inc": Document{"bloup": 3},
			}
			n, err := s.d.Update(ctx, qry, update, gedb.UpdateOptions{Upsert: true})
			s.NoError(err)
			s.Len(n, 1)

			cur, err := s.d.Find(ctx, Document{"hello": "world"}, gedb.FindOptions{})
			s.NoError(err)
			docs, err := s.readCursor(cur)
			s.NoError(err)

			s.Len(docs, 1)
			doc := docs[0]
			s.Len(doc, 4)
			s.Equal("rrr", doc.Get("cac"))
			s.Equal("world", doc.Get("hello"))
			s.Equal(float64(3), doc.Get("bloup"))
		})

		// Performing upsert with badly formatted fields yields a standard error not an exception
		s.Run("BadField", func() {
			_, err := s.d.Update(ctx, Document{"_id": "1234"}, Document{"$set": Document{"$$badfield": 5}}, gedb.UpdateOptions{Upsert: true})
			s.Error(err)
		})
	}) // ==== End of 'Upserts' ==== //

	// Cannot perform update if the update query is not either registered-modifiers-only or copy-only, or contain badly formatted fields
	s.Run("ErrorBadField", func() {
		_, err := s.d.Insert(ctx, Document{"somethnig": "yup"})
		s.NoError(err)
		_, err = s.d.Update(ctx, nil, Document{"$badField": 5}, gedb.UpdateOptions{})
		s.Error(err)
		_, err = s.d.Update(ctx, nil, Document{"bad.field": 5}, gedb.UpdateOptions{})
		s.Error(err)
		_, err = s.d.Update(ctx, nil, Document{"$inc": Document{"test": 5}, "mixed": "rrr"}, gedb.UpdateOptions{})
		s.Error(err)
		_, err = s.d.Update(ctx, nil, Document{"$inexistent": Document{"test": 5}}, gedb.UpdateOptions{})
		s.Error(err)
	})

	// Can update documents using multiple modifiers
	s.Run("MultipleModifiers", func() {
		newDoc, err := s.d.Insert(ctx, Document{"something": "yup", "other": 40})
		s.NoError(err)
		id := newDoc[0].ID()

		n, err := s.d.Update(ctx, nil, Document{"$set": Document{"something": "changed"}, "$inc": Document{"other": 10}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		var doc Document
		s.NoError(s.d.FindOne(ctx, Document{"_id": id}, &doc, gedb.FindOptions{}))
		s.Len(doc, 3)
		s.Equal(id, doc.ID())
		s.Equal("changed", doc.Get("something"))
		s.Equal(float64(50), doc.Get("other"))
	})

	// Can upsert a document even with modifiers
	s.Run("UpsertWithModifiers", func() {
		n, err := s.d.Update(ctx, Document{"bloup": "blap"}, Document{"$set": Document{"hello": "world"}}, gedb.UpdateOptions{Upsert: true})
		s.NoError(err)
		s.Len(n, 1)
		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		s.Len(docs, 1)
		s.Len(docs[0], 3)
		s.Equal("world", docs[0].Get("hello"))
		s.Equal("blap", docs[0].Get("bloup"))
		s.Contains(docs[0], "_id")
	})

	// When using modifiers, the only way to update subdocs is with the dot-notation
	s.Run("UpdateSubdocWithModifier", func() {
		_, err := s.d.Insert(ctx, Document{"bloup": Document{"blip": "blap", "other": true}})
		s.NoError(err)
		n, err := s.d.Update(ctx, nil, Document{"$set": Document{"bloup.blip": "hello"}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		var doc Document
		s.NoError(s.d.FindOne(ctx, nil, &doc, gedb.FindOptions{}))
		s.Equal("hello", doc.D("bloup").Get("blip"))
		s.Equal(true, doc.D("bloup").Get("other"))

		// Wrong
		n, err = s.d.Update(ctx, nil, Document{"$set": Document{"bloup": Document{"blip": "ola"}}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		s.NoError(s.d.FindOne(ctx, nil, &doc, gedb.FindOptions{}))
		s.Equal("ola", doc.D("bloup").Get("blip"))
		s.False(doc.D("bloup").Has("other"))

	})

	// Returns an error if the query is not well formed
	s.Run("BadQuery", func() {
		_, err := s.d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)
		_, err = s.d.Update(ctx, Document{"$or": Document{"hello": "world"}}, Document{"a": 1}, gedb.UpdateOptions{})
		s.Error(err)
	})

	// If an error is thrown by a modifier, the database state is not changed
	s.Run("NoChangeIfModificationError", func() {
		newDocs, err := s.d.Insert(ctx, Document{"hello": "world"})
		s.NoError(err)
		n, err := s.d.Update(ctx, nil, Document{"$inc": Document{"hello": 4}}, gedb.UpdateOptions{})
		s.Error(err)
		s.Len(n, 0)

		var doc Document
		s.NoError(s.d.FindOne(ctx, nil, &doc, gedb.FindOptions{}))
		s.Equal(newDocs[0], doc)
	})

	// Cant change the _id of a document
	s.Run("CannotChangeID", func() {
		newDocs, err := s.d.Insert(ctx, Document{"a": 2})
		s.NoError(err)

		_, err = s.d.Update(ctx, Document{"a": 2}, Document{"a": 2, "_id": "nope"}, gedb.UpdateOptions{})
		s.Error(err)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(2, docs[0].Get("a"))
		s.Equal(newDocs[0].ID(), docs[0].ID())

		_, err = s.d.Update(ctx, Document{"a": 2}, Document{"$set": Document{"_id": "nope"}}, gedb.UpdateOptions{})
		s.Error(err)

		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)

		s.Len(docs, 1)
		s.Len(docs[0], 2)
		s.Equal(2, docs[0].Get("a"))
		s.Equal(newDocs[0].ID(), docs[0].ID())
	})

	// Non-multi updates are persistent
	s.Run("PersistSingleUpdate", func() {
		doc1, err := s.d.Insert(ctx, Document{"a": 1, "hello": "world"})
		s.NoError(err)
		doc2, err := s.d.Insert(ctx, Document{"a": 2, "hello": "earth"})
		s.NoError(err)
		n, err := s.d.Update(ctx, Document{"a": 2}, Document{"$set": Document{"hello": "changed"}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b Document) int { return a.Get("a").(int) - b.Get("a").(int) })
		s.Len(docs, 2)
		s.Equal(Document{"_id": doc1[0].ID(), "a": 1, "hello": "world"}, docs[0])
		s.Equal(Document{"_id": doc2[0].ID(), "a": 2, "hello": "changed"}, docs[1])
	})

	// Multi updates are persistent
	s.Run("PersistMultipleUpdates", func() {
		doc1, err := s.d.Insert(ctx, Document{"a": 1, "hello": "world"})
		s.NoError(err)
		doc2, err := s.d.Insert(ctx, Document{"a": 2, "hello": "earth"})
		s.NoError(err)
		doc3, err := s.d.Insert(ctx, Document{"a": 5, "hello": "pluton"})
		s.NoError(err)
		n, err := s.d.Update(ctx, Document{"a": 2}, Document{"$set": Document{"hello": "changed"}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		n, err = s.d.Update(ctx, Document{"a": Document{"$in": []any{1, 2}}}, Document{"$set": Document{"hello": "changed"}}, gedb.UpdateOptions{Multi: true})
		s.NoError(err)
		s.Len(n, 2)

		cur, err := s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err := s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b Document) int { return a.Get("a").(int) - b.Get("a").(int) })
		s.Len(docs, 3)
		s.Equal(Document{"_id": doc1[0].ID(), "a": 1, "hello": "changed"}, docs[0])
		s.Equal(Document{"_id": doc2[0].ID(), "a": 2, "hello": "changed"}, docs[1])
		s.Equal(Document{"_id": doc3[0].ID(), "a": 5, "hello": "pluton"}, docs[2])

		s.NoError(s.d.LoadDatabase(ctx))

		cur, err = s.d.Find(ctx, nil, gedb.FindOptions{})
		s.NoError(err)
		docs, err = s.readCursor(cur)
		s.NoError(err)
		slices.SortFunc(docs, func(a, b Document) int { return int(a.Get("a").(float64)) - int(b.Get("a").(float64)) })
		s.Len(docs, 3)

		// now numbers are float because they rave been serialized and then deserialized as json numbers
		s.Equal(Document{"_id": doc1[0].ID(), "a": float64(1), "hello": "changed"}, docs[0])
		s.Equal(Document{"_id": doc2[0].ID(), "a": float64(2), "hello": "changed"}, docs[1])
		s.Equal(Document{"_id": doc3[0].ID(), "a": float64(5), "hello": "pluton"}, docs[2])

	})

	// NOTE: did not add idiomatic js test 'Can update without the options arg (will use defaults then)'

	// If a multi update fails on one document, previous updates should be rolled back
	s.Run("RollbackAllOnError", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"a"}}))
		doc1, err := s.d.Insert(ctx, Document{"a": 4})
		s.NoError(err)
		doc2, err := s.d.Insert(ctx, Document{"a": 5})
		s.NoError(err)
		doc3, err := s.d.Insert(ctx, Document{"a": "abc"})
		s.NoError(err)

		qry := Document{"a": Document{"$in": []any{4, 5, "abc"}}}
		update := Document{"$inc": Document{"a": 10}}
		n, err := s.d.Update(ctx, qry, update, gedb.UpdateOptions{Multi: true})
		s.Error(err)
		s.Len(n, 0)

		for _, idx := range s.d.indexes {
			docs := idx.GetAll()
			d1 := docs[slices.IndexFunc(docs, func(d gedb.Document) bool { return d.ID().(string) == doc1[0].ID().(string) })]
			d2 := docs[slices.IndexFunc(docs, func(d gedb.Document) bool { return d.ID().(string) == doc2[0].ID().(string) })]
			d3 := docs[slices.IndexFunc(docs, func(d gedb.Document) bool { return d.ID().(string) == doc3[0].ID().(string) })]

			s.Equal(4, d1.Get("a"))
			s.Equal(5, d2.Get("a"))
			s.Equal("abc", d3.Get("a"))
		}

	})

	// If an index constraint is violated by an update, all changes should be rolled back
	s.Run("RespectIndexConstraints", func() {
		s.NoError(s.d.EnsureIndex(ctx, gedb.EnsureIndexOptions{FieldNames: []string{"a"}, Unique: true}))
		doc1, err := s.d.Insert(ctx, Document{"a": 4})
		s.NoError(err)
		doc2, err := s.d.Insert(ctx, Document{"a": 5})
		s.NoError(err)

		qry := Document{"a": Document{"$in": []any{4, 5, "abc"}}}
		update := Document{"$set": Document{"a": 10}}
		n, err := s.d.Update(ctx, qry, update, gedb.UpdateOptions{Multi: true})
		s.Error(err)
		s.Len(n, 0)

		for _, idx := range s.d.indexes {
			docs := idx.GetAll()
			d1 := docs[slices.IndexFunc(docs, func(d gedb.Document) bool { return d.ID().(string) == doc1[0].ID().(string) })]
			d2 := docs[slices.IndexFunc(docs, func(d gedb.Document) bool { return d.ID().(string) == doc2[0].ID().(string) })]

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
		d2, err := NewDatastore(gedb.DatastoreOptions{TimestampData: true, TimeGetter: timeGetter})
		s.NoError(err)

		call := timeGetter.On("GetTime").Return(beginning)

		_, err = d2.Insert(ctx, Document{"a": 1})
		s.NoError(err)

		var doc Document
		s.NoError(d2.FindOne(ctx, Document{"a": 1}, &doc, gedb.FindOptions{}))
		s.NoError(err)
		createdAt := doc.Get("createdAt")

		// unset after find because it gets time to remove expired docs
		call.Unset()

		call = timeGetter.On("GetTime").Return(beginning.Add(time.Second))

		n, err := d2.Update(ctx, Document{"a": 1}, Document{"$set": Document{"b": 2}}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		doc = nil
		s.NoError(d2.FindOne(ctx, Document{"a": 1}, &doc, gedb.FindOptions{}))
		s.NoError(err)
		s.Equal(createdAt, doc.Get("createdAt"))

		call = timeGetter.On("GetTime").Return(beginning.Add(time.Minute))

		n, err = d2.Update(ctx, Document{"a": 1}, Document{"c": 3}, gedb.UpdateOptions{})
		s.NoError(err)
		s.Len(n, 1)

		doc = nil
		s.NoError(d2.FindOne(ctx, Document{"c": 3}, &doc, gedb.FindOptions{}))
		s.NoError(err)
		s.Equal(createdAt, doc.Get("createdAt"))
	})

	// NOTE: 'Callback signature' tests not added because we dont use callbacks

} // ==== End of 'Update' ==== //
